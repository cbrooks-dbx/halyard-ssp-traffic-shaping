# Task `validate_e2e`

- task run id: 979563940548190
- notebook: `src/notebooks/08_validate_e2e`
- started: 2026-10-20 18:20:59 UTC, duration 81s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 08 - End-to-end validation

One check per layer, each proving the layer is fed by the one before it (the journey is connected, not
a set of separate demos). Results are written to `e2e_validation` and the run fails if any check fails.

### Cell 2
```python
%pip install -q "psycopg[binary]>=3.1"
%restart_python
```
Output:
```text
[43mNote: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.[0m
```

### Cell 3
```python
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
```
Output:
```text
[]
```

### Cell 4
```python
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
```
Output:
```text
[PASS] raw            hourly auction log files landed                336 files
[PASS] lakeflow       latest pipeline update completed               d88fa974-f1d1-4680-81fd-b54e1e2b6239 UpdateStateInfoState.COMPLETED
{"ts": "2026-10-20 18:21:48.552", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:21:48.552", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:21:48.552", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
[PASS] lakeflow       bronze -> silver drops bad rows                bronze 562,800 -> silver 558,864 auctions (3,936 dropped or replayed)
[PASS] lakeflow       silver_dsp_calls has no replayed duplicates    4,785,525 calls, 0 duplicates
[PASS] lakeflow       expectations recorded in event log             has_dsp_calls=0; known_dsp=0; known_publisher=0; valid_auction_id=0; valid_floor=1124; valid_publisher=2258; valid_timestamp=562
[PASS] lakeflow       gold reconciles to silver                      gold 4,785,525 == silver 4,785,525
```

### Cell 5
```python
tags = q(f"SELECT count(*) FROM {catalog}.information_schema.table_tags WHERE schema_name = '{schema}' AND tag_name = 'layer'")[0]
check("unity_catalog", "tables carry layer/domain tags", tags >= 6, f"{tags} tagged tables")
view_rows = q(f"SELECT count(*) FROM {fq}.v_partner_segment_performance")[0]
check("unity_catalog", "governed partner view resolves for yield_ops", view_rows > 0, f"{view_rows:,} rows visible to {w.current_user.me().user_name}")
app_sp = w.apps.get(app_name).service_principal_client_id
grants = spark.sql(f"SHOW GRANTS `{app_sp}` ON TABLE {fq}.throttle_rules").count()
check("unity_catalog", "app SP has SELECT on throttle_rules", grants > 0, f"{grants} grant rows")
```
Output:
```text
[PASS] unity_catalog  tables carry layer/domain tags                 8 tagged tables
[PASS] unity_catalog  governed partner view resolves for yield_ops   511,889 rows visible to chandler.brooks@databricks.com
[PASS] unity_catalog  app SP has SELECT on throttle_rules            5 grant rows
```

### Cell 6
```python
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
```
Output:
```text
[PASS] ml             champion model registered in UC                bid_propensity v3, holdout AUC 0.907
[PASS] ml             model beats historical-rate baseline           35.5% vs 30.8% calls cut at 99% revenue retained
[PASS] ml             throttle rules come from the champion model    113,762 rules from v3
[PASS] ml             revenue guardrail held (>= 99% retained)       99.17% retained, 35.5% calls cut
```

### Cell 7
```python
briefs = q(f"SELECT count(*), min(length(partner_brief)) FROM {fq}.dsp_partner_briefs")
dsps = q(f"SELECT count(*) FROM {fq}.dsp_throttle_impact")[0]
check("genai", "one partner brief per DSP", briefs[0] == dsps and briefs[1] > 100, f"{briefs[0]} briefs, shortest {briefs[1]} chars")
```
Output:
```text
[PASS] genai          one partner brief per DSP                      10 briefs, shortest 675 chars
```

### Cell 8
```python
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
```
Output:
```text
[PASS] lakebase       serving table matches Delta rules              Lakebase 113,762 == Delta 113,762
[PASS] lakebase       ops override table reachable                   0 overrides recorded
```

### Cell 9
```python
space_id = q(f"SELECT value FROM {fq}.app_config WHERE key = 'genie_space_id'")[0]
sp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}", query={"include_serialized_space": "true"})
import json
n_tables = len(json.loads(sp.get("serialized_space", "{}")).get("data_sources", {}).get("tables", []))
check("genie", "Genie space over gold + rules tables", n_tables >= 8, f"space {space_id}, {n_tables} tables")

app = w.apps.get(app_name)
check("app", "Databricks App deployed and running", str(app.app_status.state).endswith("RUNNING") if app.app_status else False,
      f"{app.url} ({app.app_status.state if app.app_status else 'no status'})")
```
Output:
```text
[PASS] genie          Genie space over gold + rules tables           space 01f1ccb10c7f12b78b235fea709b4bc5, 10 tables
[PASS] app            Databricks App deployed and running            https://halyard-traffic-shaping-7474656585748611.aws.databricksapps.com (ApplicationState.RUNNING)
```

### Cell 10
```python
import pandas as pd
df = pd.DataFrame(checks, columns=["layer", "check_name", "passed", "detail"])
spark.createDataFrame(df).selectExpr("*", "current_timestamp() AS checked_at").write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{fq}.e2e_validation")
print(f"\n{df.passed.sum()}/{len(df)} checks passed")
failed = df[~df.passed]
if len(failed):
    raise AssertionError(f"Failed checks: {failed.check_name.tolist()}")
```
Output:
```text
18/18 checks passed
```
