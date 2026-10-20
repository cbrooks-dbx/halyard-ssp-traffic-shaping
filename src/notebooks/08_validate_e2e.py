# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - End-to-end validation
# MAGIC
# MAGIC One check per layer, each proving the layer is fed by the one before it (the journey is connected, not
# MAGIC a set of separate demos). Results are written to `e2e_validation` and the run fails if any check fails.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.1"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
dbutils.widgets.text("lakebase_project", "halyard-traffic-shaping")
dbutils.widgets.text("app_name", "halyard-traffic-shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
project = dbutils.widgets.get("lakebase_project")
app_name = dbutils.widgets.get("app_name")
fq = f"{catalog}.{schema}"

import os

import psycopg
from databricks.sdk import WorkspaceClient
from mlflow.tracking import MlflowClient
import mlflow

w = WorkspaceClient()
checks = []


def check(layer, name, ok, detail):
    checks.append((layer, name, bool(ok), str(detail)))
    print(f"[{'PASS' if ok else 'FAIL'}] {layer:<14} {name:<46} {detail}")


q = lambda s: spark.sql(s).first()

# COMMAND ----------

files = os.listdir(f"/Volumes/{catalog}/{schema}/raw_landing/auctions")
check("raw", "hourly auction log files landed", len(files) > 0, f"{len(files)} files")

pipe = next(iter(w.pipelines.list_pipelines(filter="name LIKE 'halyard-bid-log-pipeline'")))
upd = w.pipelines.get(pipe.pipeline_id).latest_updates[0]
check("lakeflow", "latest pipeline update completed", str(upd.state).endswith("COMPLETED"), f"{pipe.pipeline_id} {upd.state}")
b, s, c = (q(f"SELECT count(*) FROM {fq}.{t}")[0] for t in ["bronze_auctions", "silver_auctions", "silver_dsp_calls"])
check("lakeflow", "bronze -> silver drops bad rows", 0 < s < b, f"bronze {b:,} -> silver {s:,} auctions ({b - s:,} dropped or replayed)")
dupes = q(f"SELECT count(*) - count(DISTINCT auction_id, dsp_id) FROM {fq}.silver_dsp_calls")[0]
check("lakeflow", "silver_dsp_calls has no replayed duplicates", dupes == 0, f"{c:,} calls, {dupes} duplicates")
exp = spark.sql(f"""
  SELECT e.name AS expectation, sum(e.failed_records) AS failed
  FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
          'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
        FROM event_log('{pipe.pipeline_id}') WHERE event_type = 'flow_progress')
  GROUP BY 1 ORDER BY 1""").toPandas()
check("lakeflow", "expectations recorded in event log", len(exp) > 0, "; ".join(f"{r.expectation}={int(r.failed)}" for r in exp.itertuples()))
g = q(f"SELECT sum(outbound_calls) FROM {fq}.gold_segment_daily")[0]
check("lakeflow", "gold reconciles to silver", g == c, f"gold {g:,} == silver {c:,}")

# COMMAND ----------

tags = q(f"SELECT count(*) FROM {catalog}.information_schema.table_tags WHERE schema_name = '{schema}' AND tag_name = 'layer'")[0]
check("unity_catalog", "tables carry layer/domain tags", tags >= 6, f"{tags} tagged tables")
view_rows = q(f"SELECT count(*) FROM {fq}.v_partner_segment_performance")[0]
check("unity_catalog", "governed partner view resolves for yield_ops", view_rows > 0, f"{view_rows:,} rows visible to {w.current_user.me().user_name}")
app_sp = w.apps.get(app_name).service_principal_client_id
grants = spark.sql(f"SHOW GRANTS `{app_sp}` ON TABLE {fq}.throttle_rules").count()
check("unity_catalog", "app SP has SELECT on throttle_rules", grants > 0, f"{grants} grant rows")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mv = MlflowClient().get_model_version_by_alias(f"{fq}.bid_propensity", "champion")
run = MlflowClient().get_run(mv.run_id)
auc = run.data.metrics["holdout_auc"]
cut, base = run.data.metrics["ml_calls_cut_at_99pct_revenue"], run.data.metrics["baseline_calls_cut_at_99pct_revenue"]
check("ml", "champion model registered in UC", mv.version is not None, f"bid_propensity v{mv.version}, holdout AUC {auc:.3f}")
check("ml", "model beats historical-rate baseline", cut > base, f"{cut:.1%} vs {base:.1%} calls cut at 99% revenue retained")
rules_v = q(f"SELECT min(model_version), count(*) FROM {fq}.throttle_rules")
check("ml", "throttle rules come from the champion model", int(rules_v[0]) == int(mv.version), f"{rules_v[1]:,} rules from v{rules_v[0]}")
impact = {r.metric: r.value for r in spark.table(f"{fq}.throttle_impact_summary").collect()}
check("ml", "revenue guardrail held (>= 99% retained)", impact["revenue_retained_pct"] >= 0.99, f"{impact['revenue_retained_pct']:.2%} retained, {impact['outbound_calls_cut_pct']:.1%} calls cut")

# COMMAND ----------

briefs = q(f"SELECT count(*), min(length(partner_brief)) FROM {fq}.dsp_partner_briefs")
dsps = q(f"SELECT count(*) FROM {fq}.dsp_throttle_impact")[0]
check("genai", "one partner brief per DSP", briefs[0] == dsps and briefs[1] > 100, f"{briefs[0]} briefs, shortest {briefs[1]} chars")

# COMMAND ----------

ENDPOINT = f"projects/{project}/branches/production/endpoints/primary"
host = w.api_client.do("GET", f"/api/2.0/postgres/{ENDPOINT}")["status"]["hosts"]["host"]
token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]
with psycopg.connect(host=host, dbname="databricks_postgres", user=w.current_user.me().user_name,
                     password=token, sslmode="require") as conn:
    n_pg = conn.execute(f'SELECT count(*) FROM "{schema}"."throttle_rules_serving"').fetchone()[0]
    n_ovr = conn.execute("SELECT count(*) FROM halyard_ops.rule_overrides").fetchone()[0]
n_delta = q(f"SELECT count(*) FROM {fq}.throttle_rules")[0]
check("lakebase", "serving table matches Delta rules", n_pg == n_delta, f"Lakebase {n_pg:,} == Delta {n_delta:,}")
check("lakebase", "ops override table reachable", n_ovr >= 0, f"{n_ovr} overrides recorded")

# COMMAND ----------

space_id = q(f"SELECT value FROM {fq}.app_config WHERE key = 'genie_space_id'")[0]
sp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}", query={"include_serialized_space": "true"})
import json
n_tables = len(json.loads(sp.get("serialized_space", "{}")).get("data_sources", {}).get("tables", []))
check("genie", "Genie space over gold + rules tables", n_tables >= 8, f"space {space_id}, {n_tables} tables")

app = w.apps.get(app_name)
check("app", "Databricks App deployed and running", str(app.app_status.state).endswith("RUNNING") if app.app_status else False,
      f"{app.url} ({app.app_status.state if app.app_status else 'no status'})")

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(checks, columns=["layer", "check_name", "passed", "detail"])
spark.createDataFrame(df).selectExpr("*", "current_timestamp() AS checked_at").write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{fq}.e2e_validation")
print(f"\n{df.passed.sum()}/{len(df)} checks passed")
failed = df[~df.passed]
if len(failed):
    raise AssertionError(f"Failed checks: {failed.check_name.tolist()}")
