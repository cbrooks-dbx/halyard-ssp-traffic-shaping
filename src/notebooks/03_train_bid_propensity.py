# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bid-propensity model (MLflow + Unity Catalog model registry)
# MAGIC
# MAGIC **Question the model answers:** if Halyard sends this bid request to this DSP, how likely is the DSP to bid?
# MAGIC
# MAGIC **Why a model and not just historical bid rates:** traffic segments are long-tailed. Most
# MAGIC DSP x publisher x format x device x country x path x user-ID combinations see only a handful of calls a
# MAGIC week, so their observed bid rate is noise, and new publishers have no history at all. A gradient-boosted
# MAGIC model shares strength across segments (it learns "Tidewater only buys video and CTV" once, not per
# MAGIC publisher). We prove the lift below by comparing both approaches on the same out-of-time holdout.
# MAGIC
# MAGIC **How the threshold is chosen:** not by accuracy, by the business guardrail. We pick the largest score
# MAGIC cut-off that keeps at least 99% of net revenue, measured on the holdout.

# COMMAND ----------

dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
MODEL_NAME = f"{fq}.bid_propensity"

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

CAT = ["dsp_id", "publisher_id", "vertical", "inventory_type", "ads_txt_status", "ad_format",
       "device_type", "os", "geo_country", "supply_path"]
NUM = ["floor_cpm", "user_id_present", "event_hour"]
SEGMENT_KEY = ["dsp_id", "publisher_id", "ad_format", "device_type", "geo_country", "supply_path", "user_id_present"]

dates = [r.event_date for r in spark.sql(f"SELECT DISTINCT event_date FROM {fq}.silver_dsp_calls ORDER BY 1").collect()]
cutoff = dates[-4]
print(f"{len(dates)} days of calls: {dates[0]} .. {dates[-1]}. Train < {cutoff} <= holdout")

cols = ", ".join(CAT + NUM + ["bid", "won", "net_revenue_usd", "event_date"])
train_sdf = spark.sql(f"SELECT {cols} FROM {fq}.silver_dsp_calls WHERE event_date < '{cutoff}'")
test_sdf = spark.sql(f"SELECT {cols} FROM {fq}.silver_dsp_calls WHERE event_date >= '{cutoff}'")
n_train_all = train_sdf.count()
train = train_sdf.sample(fraction=min(1.0, 1_500_000 / n_train_all), seed=42).toPandas()
test = test_sdf.toPandas()
for df in (train, test):
    df["user_id_present"] = df["user_id_present"].astype(int)
    df["bid"] = df["bid"].astype(int)
print(f"train rows {len(train):,} (sampled from {n_train_all:,}), holdout rows {len(test):,}, train bid rate {train.bid.mean():.4f}")

# COMMAND ----------

def build_model():
    pre = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1), CAT),
        ("num", "passthrough", NUM),  # trees are invariant to monotone transforms, so floor_cpm needs no log
    ])
    clf = HistGradientBoostingClassifier(
        categorical_features=list(range(len(CAT))), max_iter=300, learning_rate=0.08,
        max_leaf_nodes=63, l2_regularization=1.0, early_stopping=True, validation_fraction=0.1, random_state=42)
    return Pipeline([("pre", pre), ("clf", clf)])


THROTTLE_SEND, REDUCE_SEND = 0.05, 0.5  # must match the policy in 04_score_throttle_rules


def guardrail_curve(df, score_col, label):
    """Segment-level policy curve for the deployed three-tier policy at each threshold t:
    score < t -> THROTTLE (send 5%), t <= score < 2t -> REDUCE (send 50%), else ALLOW.
    Revenue at risk is the conservative bound: every win in traffic not sent is assumed lost."""
    seg = df.groupby(SEGMENT_KEY, observed=True).agg(
        calls=("bid", "size"), bids=("bid", "sum"), rev=("net_revenue_usd", "sum"), score=(score_col, "mean")).reset_index()
    tot_calls, tot_rev, tot_bids = seg.calls.sum(), seg.rev.sum(), seg.bids.sum()
    out = []
    for t in np.unique(np.concatenate([np.linspace(0.0, 0.05, 201), np.linspace(0.05, 0.15, 41)])):
        cut = np.where(seg.score < t, 1 - THROTTLE_SEND, np.where(seg.score < 2 * t, 1 - REDUCE_SEND, 0.0))
        out.append((label, float(t), (seg.calls * cut).sum() / tot_calls, (seg.rev * cut).sum() / tot_rev,
                    (seg.bids * cut).sum() / tot_bids, int((cut > 0).sum())))
    return pd.DataFrame(out, columns=["method", "threshold", "calls_cut_pct", "revenue_at_risk_pct", "bids_lost_pct", "segments_shaped"])


def best_at_guardrail(curve, max_risk=0.01):
    ok = curve[curve.revenue_at_risk_pct <= max_risk]
    return ok.sort_values("calls_cut_pct").iloc[-1]

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/halyard-bid-propensity")

with mlflow.start_run(run_name="hgb_bid_propensity") as run:
    model = build_model()
    model.fit(train[CAT + NUM], train["bid"])
    test["p_bid"] = model.predict_proba(test[CAT + NUM])[:, 1]

    metrics = {
        "holdout_auc": roc_auc_score(test.bid, test.p_bid),
        "holdout_pr_auc": average_precision_score(test.bid, test.p_bid),
        "holdout_log_loss": log_loss(test.bid, test.p_bid),
        "holdout_brier": brier_score_loss(test.bid, test.p_bid),
        "holdout_bid_rate": test.bid.mean(),
        "holdout_mean_pred": test.p_bid.mean(),
    }

    # Baseline: each segment's observed bid rate in the training window. To be fair to the baseline, segments
    # with fewer than 30 training calls fall back to the DSP x format rate instead of a noisy (often 0) estimate.
    seg_hist = train.groupby(SEGMENT_KEY).bid.agg(hist_rate="mean", hist_n="size").reset_index()
    df_hist = train.groupby(["dsp_id", "ad_format"]).bid.mean().rename("dsp_fmt_rate").reset_index()
    test = test.merge(seg_hist, on=SEGMENT_KEY, how="left").merge(df_hist, on=["dsp_id", "ad_format"], how="left")
    test["hist_rate"] = np.where(test.hist_n.fillna(0) >= 30, test.hist_rate, test.dsp_fmt_rate)
    test["hist_rate"] = test.hist_rate.fillna(train.bid.mean())

    curve = pd.concat([guardrail_curve(test, "p_bid", "ml_model"), guardrail_curve(test, "hist_rate", "historical_rate")])
    # Guardrail comes from value_assumptions (Finance owns it). Tune with 10% headroom so the rule set still holds
    # the guardrail when applied to a different week of traffic than the holdout it was tuned on.
    min_retention = spark.sql(f"SELECT value FROM {fq}.value_assumptions WHERE parameter = 'min_revenue_retention'").first()[0]
    max_risk = (1 - min_retention) * 0.9
    ml_pt = best_at_guardrail(curve[curve.method == "ml_model"], max_risk)
    base_pt = best_at_guardrail(curve[curve.method == "historical_rate"], max_risk)
    metrics.update({
        "threshold": ml_pt.threshold,
        "ml_calls_cut_at_99pct_revenue": ml_pt.calls_cut_pct,
        "ml_revenue_at_risk": ml_pt.revenue_at_risk_pct,
        "baseline_calls_cut_at_99pct_revenue": base_pt.calls_cut_pct,
        "baseline_revenue_at_risk": base_pt.revenue_at_risk_pct,
    })
    mlflow.log_metrics(metrics)
    mlflow.log_params({"train_rows": len(train), "holdout_rows": len(test), "cutoff_date": str(cutoff),
                       "features": ",".join(CAT + NUM), "segment_key": ",".join(SEGMENT_KEY)})
    sample = train[CAT + NUM].head(50)
    mlflow.sklearn.log_model(model, "model", signature=infer_signature(sample, model.predict_proba(sample)[:, 1]),
                             input_example=sample.head(3), registered_model_name=MODEL_NAME)
    run_id = run.info.run_id

for k, v in metrics.items():
    print(f"{k:<40} {v:.4f}")

# COMMAND ----------

# MAGIC %md ### Calibration (predicted vs actual bid rate by decile)
# MAGIC Traffic shaping decisions use the probability itself, so calibration matters more than AUC.

# COMMAND ----------

test["decile"] = pd.qcut(test.p_bid.rank(method="first"), 10, labels=False) + 1
calib = test.groupby("decile").agg(calls=("bid", "size"), predicted=("p_bid", "mean"), actual=("bid", "mean")).round(4)
print(calib.to_string())

# COMMAND ----------

# MAGIC %md ### Policy curve at the revenue guardrail: ML vs historical bid rate

# COMMAND ----------

print(f"ML model        : cut {ml_pt.calls_cut_pct:.1%} of outbound calls, {ml_pt.revenue_at_risk_pct:.2%} revenue at risk, threshold p_bid < {ml_pt.threshold:.4f}")
print(f"Historical rate : cut {base_pt.calls_cut_pct:.1%} of outbound calls, {base_pt.revenue_at_risk_pct:.2%} revenue at risk")
print(f"Lift from ML    : {ml_pt.calls_cut_pct - base_pt.calls_cut_pct:+.1%} more calls removed at the same revenue guardrail")
show = curve[curve.threshold.isin([0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.1])].copy()
for c in ["calls_cut_pct", "revenue_at_risk_pct", "bids_lost_pct"]:
    show[c] = (show[c] * 100).round(2)
print(show.to_string(index=False))

spark.createDataFrame(curve).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.threshold_curve")
spark.sql(f"COMMENT ON TABLE {fq}.threshold_curve IS 'Holdout policy curve: share of outbound calls cut vs share of net revenue at risk, per scoring method and threshold'")

# COMMAND ----------

client = MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'") if v.run_id == run_id)
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.set_model_version_tag(MODEL_NAME, str(version), "throttle_threshold", f"{ml_pt.threshold:.5f}")
client.set_model_version_tag(MODEL_NAME, str(version), "calls_cut_at_guardrail", f"{ml_pt.calls_cut_pct:.4f}")
client.update_registered_model(MODEL_NAME, description="Probability a DSP bids on a given outbound bid request. Drives Halyard traffic-shaping rules.")
print(f"Registered {MODEL_NAME} v{version} as @champion (MLflow run {run_id})")
