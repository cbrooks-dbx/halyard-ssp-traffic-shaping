# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Score traffic and publish throttle rules
# MAGIC
# MAGIC Loads the `@champion` bid-propensity model from Unity Catalog, scores the last 7 days of outbound calls,
# MAGIC and turns the scores into one rule per traffic segment:
# MAGIC
# MAGIC | action | send rate | when |
# MAGIC |---|---|---|
# MAGIC | `THROTTLE` | 5% | predicted bid rate below the guardrail threshold. 5% keeps flowing so the model keeps learning (exploration) |
# MAGIC | `REDUCE` | 50% | between 1x and 2x the threshold |
# MAGIC | `ALLOW` | 100% | everything else |
# MAGIC
# MAGIC The bidder edge looks these rules up per request (served from Lakebase in notebook 06).

# COMMAND ----------

dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
MODEL_NAME = f"{fq}.bid_propensity"

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
threshold = float(mv.tags["throttle_threshold"])
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
print(f"Using {MODEL_NAME} v{mv.version} (@champion), throttle threshold p_bid < {threshold:.4f}")

CAT = ["dsp_id", "publisher_id", "vertical", "inventory_type", "ads_txt_status", "ad_format",
       "device_type", "os", "geo_country", "supply_path"]
NUM = ["floor_cpm", "user_id_present", "event_hour"]
SEGMENT_KEY = ["dsp_id", "publisher_id", "ad_format", "device_type", "geo_country", "supply_path", "user_id_present"]

max_date = spark.sql(f"SELECT max(event_date) FROM {fq}.silver_dsp_calls").first()[0]
calls = spark.sql(f"""
  SELECT {", ".join(CAT + NUM)}, dsp_name, publisher_name, bid, won, net_revenue_usd, gross_spend_usd
  FROM {fq}.silver_dsp_calls WHERE event_date > date_sub('{max_date}', 7)
""").toPandas()
calls["user_id_present"] = calls["user_id_present"].astype(int)
calls["bid"] = calls["bid"].astype(int)
calls["p_bid"] = model.predict_proba(calls[CAT + NUM])[:, 1]
print(f"Scored {len(calls):,} outbound calls from the 7 days ending {max_date}")

# COMMAND ----------

seg = calls.groupby(SEGMENT_KEY + ["dsp_name", "publisher_name", "vertical"], observed=True).agg(
    calls_7d=("bid", "size"), bids_7d=("bid", "sum"), wins_7d=("won", "sum"),
    net_revenue_7d=("net_revenue_usd", "sum"), predicted_bid_rate=("p_bid", "mean"),
    avg_floor_cpm=("floor_cpm", "mean")).reset_index()
seg["observed_bid_rate"] = seg.bids_7d / seg.calls_7d
seg["action"] = np.where(seg.predicted_bid_rate < threshold, "THROTTLE",
                np.where(seg.predicted_bid_rate < 2 * threshold, "REDUCE", "ALLOW"))
seg["send_rate"] = seg.action.map({"THROTTLE": 0.05, "REDUCE": 0.5, "ALLOW": 1.0})
seg["est_daily_calls_saved"] = seg.calls_7d / 7 * (1 - seg.send_rate)
seg["est_daily_revenue_at_risk_usd"] = seg.net_revenue_7d / 7 * (1 - seg.send_rate)
seg["user_id_present"] = seg.user_id_present.astype(bool)
seg["rule_id"] = seg[SEGMENT_KEY].astype(str).agg("|".join, axis=1)
seg["model_version"] = int(mv.version)
seg["threshold"] = threshold

rules = spark.createDataFrame(seg).withColumn("generated_at", F.current_timestamp())
rules.write.mode("overwrite").option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(f"{fq}.throttle_rules")
spark.sql(f"ALTER TABLE {fq}.throttle_rules ALTER COLUMN rule_id SET NOT NULL")
try:
    spark.sql(f"ALTER TABLE {fq}.throttle_rules ADD CONSTRAINT throttle_rules_pk PRIMARY KEY (rule_id)")
except Exception as e:
    print(f"(primary key constraint already present: {str(e).splitlines()[0][:80]})")
spark.sql(f"COMMENT ON TABLE {fq}.throttle_rules IS 'One traffic-shaping rule per segment: action, send rate, predicted vs observed bid rate, estimated daily calls saved and revenue at risk'")
print(seg.action.value_counts().to_string())

# COMMAND ----------

# MAGIC %md ### Counterfactual auction replay
# MAGIC The conservative revenue-at-risk figure assumes every throttled win is lost. In a real auction the
# MAGIC runner-up DSP often still clears the impression. This replays each of the last 7 days' auctions with only the
# MAGIC calls the rules would have sent (deterministic hash sampling at each segment's send rate), re-runs the
# MAGIC second-price clearing against the floor, and measures the revenue Halyard would actually have kept.

# COMMAND ----------

replay = spark.sql(f"""
WITH calls AS (
  SELECT c.auction_id, c.floor_cpm, c.bid, c.bid_cpm, c.won, c.net_revenue_usd,
         coalesce(r.send_rate, 1.0) AS send_rate,
         (pmod(xxhash64(c.auction_id, c.dsp_id), 10000) / 10000.0) < coalesce(r.send_rate, 1.0) AS sent
  FROM {fq}.silver_dsp_calls c
  LEFT JOIN {fq}.throttle_rules r
    ON r.rule_id = concat_ws('|', c.dsp_id, c.publisher_id, c.ad_format, c.device_type, c.geo_country, c.supply_path,
                              CASE WHEN c.user_id_present THEN 'True' ELSE 'False' END)
  WHERE c.event_date > date_sub('{max_date}', 7)
),
auctions AS (
  SELECT auction_id, max(floor_cpm) AS floor_cpm,
         sum(net_revenue_usd) AS actual_net_usd,
         count(*) AS calls_before, sum(CAST(sent AS INT)) AS calls_after,
         sort_array(filter(collect_list(CASE WHEN sent AND bid THEN bid_cpm END), x -> x IS NOT NULL), false) AS sent_bids
  FROM calls GROUP BY auction_id
)
SELECT
  count(*) AS auctions,
  sum(calls_before) AS calls_before, sum(calls_after) AS calls_after,
  sum(actual_net_usd) AS actual_net_usd,
  sum(CASE WHEN size(sent_bids) > 0
           THEN (greatest(coalesce(try_element_at(sent_bids, 2), 0.0), floor_cpm) + 0.01) / 1000.0 * 0.15 ELSE 0.0 END) AS replay_net_usd,
  avg(CASE WHEN actual_net_usd > 0 THEN 1.0 ELSE 0.0 END) AS fill_before,
  avg(CASE WHEN size(sent_bids) > 0 THEN 1.0 ELSE 0.0 END) AS fill_after
FROM auctions
""").first()
replay_loss = 1 - replay.replay_net_usd / replay.actual_net_usd
replay_cut = 1 - replay.calls_after / replay.calls_before
print(f"Auctions replayed      {replay.auctions:,}")
print(f"Outbound calls         {replay.calls_before:,} -> {replay.calls_after:,}  ({replay_cut:.1%} cut)")
print(f"Fill rate              {replay.fill_before:.2%} -> {replay.fill_after:.2%}")
print(f"Net revenue            ${replay.actual_net_usd:,.2f} -> ${replay.replay_net_usd:,.2f}  ({replay_loss:.2%} lost)")

# COMMAND ----------

# MAGIC %md ### Impact: what the rules do to the last 7 days of traffic, and what that is worth at Halyard's scale

# COMMAND ----------

a = {r.parameter: r.value for r in spark.table(f"{fq}.value_assumptions").collect()}
tot_calls, tot_rev = seg.calls_7d.sum(), seg.net_revenue_7d.sum()
calls_cut = (seg.calls_7d * (1 - seg.send_rate)).sum() / tot_calls
rev_risk = (seg.net_revenue_7d * (1 - seg.send_rate)).sum() / tot_rev
calls_per_auction = spark.sql(f"SELECT avg(calls_per_auction) FROM {fq}.gold_exchange_daily").first()[0]
annual_outbound = a["inbound_auctions_per_day"] * calls_per_auction * 365
annual_outbound_cost = annual_outbound / 1e6 * a["cost_per_million_outbound_calls_usd"]
net_revenue = a["annual_gross_spend_usd"] * a["take_rate"]

summary = pd.DataFrame([
    ("outbound_calls_cut_pct", calls_cut, "pct", "Share of outbound bid requests no longer sent"),
    ("net_revenue_at_risk_pct", rev_risk, "pct", "Conservative: assumes every throttled win is lost, ignoring the runner-up bid"),
    ("revenue_retained_pct", 1 - rev_risk, "pct", "Conservative bound; guardrail is >= 99%"),
    ("replay_net_revenue_loss_pct", replay_loss, "pct", "Expected: auction replay where the runner-up bid can still win"),
    ("replay_fill_rate_before", replay.fill_before, "pct", "Share of auctions with a winner today"),
    ("replay_fill_rate_after", replay.fill_after, "pct", "Share of auctions with a winner under the rules"),
    ("segments_throttled", float((seg.action == "THROTTLE").sum()), "count", "Segments sent at 5%"),
    ("segments_reduced", float((seg.action == "REDUCE").sum()), "count", "Segments sent at 50%"),
    ("segments_total", float(len(seg)), "count", "Segments with traffic in the last 7 days"),
    ("annual_outbound_calls", annual_outbound, "calls/yr", "Inbound auctions x observed fan-out x 365"),
    ("annual_outbound_cost_usd", annual_outbound_cost, "USD/yr", "Cost of sending every call today"),
    ("annual_infra_savings_usd", annual_outbound_cost * calls_cut, "USD/yr", "Hard savings: calls not sent x cost per call"),
    ("annual_revenue_at_risk_usd", net_revenue * rev_risk, "USD/yr", "Upper bound on net revenue exposed"),
    ("annual_expected_revenue_loss_usd", net_revenue * replay_loss, "USD/yr", "Expected net revenue lost, from the auction replay"),
    ("annual_qps_reallocation_upside_usd", net_revenue * a["qps_reallocation_uplift_pct"], "USD/yr", "Scenario: freed DSP QPS spent on traffic they do buy"),
    ("annual_net_benefit_usd", annual_outbound_cost * calls_cut - net_revenue * replay_loss, "USD/yr", "Hard savings minus expected revenue loss (upside excluded)"),
    ("annual_net_benefit_worst_case_usd", annual_outbound_cost * calls_cut - net_revenue * rev_risk, "USD/yr", "Hard savings minus the conservative revenue-at-risk bound"),
], columns=["metric", "value", "unit", "definition"])
summary["value"] = summary["value"].astype(float)  # replay aggregates come back as Decimal
spark.createDataFrame(summary).withColumn("model_version", F.lit(int(mv.version))).withColumn("computed_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.throttle_impact_summary")
spark.sql(f"COMMENT ON TABLE {fq}.throttle_impact_summary IS 'Business case for the current rule set, scaled to Halyard volume using value_assumptions'")
fmt = lambda r: f"{r.value:.2%}" if r.unit == "pct" else (f"${r.value:,.0f}" if r.unit.startswith("USD") else f"{r.value:,.0f}")
for r in summary.itertuples():
    print(f"{r.metric:<38} {fmt(r):>18}   {r.definition}")

# COMMAND ----------

dsp = seg.groupby(["dsp_id", "dsp_name"]).apply(lambda g: pd.Series({
    "calls_7d": g.calls_7d.sum(),
    "observed_bid_rate": g.bids_7d.sum() / g.calls_7d.sum(),
    "calls_cut_pct": (g.calls_7d * (1 - g.send_rate)).sum() / g.calls_7d.sum(),
    "revenue_at_risk_pct": (g.net_revenue_7d * (1 - g.send_rate)).sum() / max(g.net_revenue_7d.sum(), 1e-9),
    "net_revenue_7d": g.net_revenue_7d.sum(),
    "segments_throttled": int((g.action == "THROTTLE").sum()),
    "top_throttled_formats": ", ".join(g[g.action == "THROTTLE"].groupby("ad_format").calls_7d.sum().sort_values(ascending=False).head(2).index),
    "top_throttled_devices": ", ".join(g[g.action == "THROTTLE"].groupby("device_type").calls_7d.sum().sort_values(ascending=False).head(2).index),
    "top_throttled_countries": ", ".join(g[g.action == "THROTTLE"].groupby("geo_country").calls_7d.sum().sort_values(ascending=False).head(3).index),
})).reset_index()
spark.createDataFrame(dsp).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.dsp_throttle_impact")
spark.sql(f"COMMENT ON TABLE {fq}.dsp_throttle_impact IS 'Per-DSP effect of the rule set: share of calls cut, revenue at risk, where the cuts land'")
show = dsp.copy()
for c in ["observed_bid_rate", "calls_cut_pct", "revenue_at_risk_pct"]:
    show[c] = (show[c] * 100).round(1)
print(show[["dsp_name", "calls_7d", "observed_bid_rate", "calls_cut_pct", "revenue_at_risk_pct", "segments_throttled", "top_throttled_formats"]].to_string(index=False))
