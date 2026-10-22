# Task `lakebase_serving`

- task run id: 584769068839295
- notebook: `src/notebooks/06_lakebase_serving`
- started: 2026-10-20 18:19:31 UTC, duration 60s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 06 - Operational serving on Lakebase

The bidder edge makes a send / don't-send decision per bid request, so it needs single-digit-millisecond
key lookups, not an analytical query. The rules live in Delta (`throttle_rules`) and are **synced to
Lakebase Postgres** as a read-optimized serving table.

Lakebase also holds the one piece of state that originates in the business, not the lakehouse:
**ops overrides**. When a partner manager pins a segment to ALLOW (say, a DSP is launching a CTV campaign
next week), the app writes that row transactionally to `halyard_ops.rule_overrides`, and the edge applies
override-then-rule.

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
SYNCED = f"{fq}.throttle_rules_serving"
PG_DB = "databricks_postgres"
BRANCH = f"projects/{project}/branches/production"
ENDPOINT = f"{BRANCH}/endpoints/primary"
```
Output:
```text
[]
```

### Cell 4
```python
import time

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
app_sp = w.apps.get(app_name).service_principal_client_id

# Lakebase Autoscaling (project -> branch -> endpoint) through the Postgres REST API
ep = w.api_client.do("GET", f"/api/2.0/postgres/{ENDPOINT}")
while ep["status"]["current_state"] not in ("ACTIVE", "IDLE"):
    print(f"endpoint state {ep['status']['current_state']}, waiting...")
    time.sleep(15)
    ep = w.api_client.do("GET", f"/api/2.0/postgres/{ENDPOINT}")
PG_HOST = ep["status"]["hosts"]["host"]
st = ep["status"]
print(f"Lakebase endpoint {ENDPOINT}: {st['current_state']}, autoscaling {st.get('autoscaling_limit_min_cu')}-{st.get('autoscaling_limit_max_cu')} CU, host {PG_HOST}")


# Override-then-rule: the same query the app and the bidder edge use. Kept as a query rather than a Postgres
# view because snapshot syncs recreate the serving table, and a dependent view would block that.
EFFECTIVE_SQL = f"""
SELECT r.rule_id, r.dsp_name, r.publisher_name, r.ad_format, r.device_type, r.geo_country, r.supply_path,
       r.predicted_bid_rate, r.est_daily_calls_saved,
       COALESCE(o.action, r.action) AS effective_action, COALESCE(o.send_rate, r.send_rate) AS effective_send_rate
FROM "{schema}"."throttle_rules_serving" r
LEFT JOIN LATERAL (
  SELECT action, send_rate FROM halyard_ops.rule_overrides x
  WHERE x.rule_id = r.rule_id AND (x.expires_at IS NULL OR x.expires_at > now())
  ORDER BY x.created_at DESC LIMIT 1) o ON true
"""


def pg_connect():
    token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]
    return psycopg.connect(host=PG_HOST, dbname=PG_DB, user=me, password=token, sslmode="require", autocommit=True)
```
Output:
```text
Lakebase endpoint projects/halyard-traffic-shaping/branches/production/endpoints/primary: ACTIVE, autoscaling 1.0-1.0 CU, host ep-aged-fog-d21h94uk.database.us-east-1.cloud.databricks.com
```

## 1. Sync `throttle_rules` (Delta) -> Lakebase

### Cell 6
```python
def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r:
                return r
    return None


try:
    synced = w.api_client.do("GET", f"/api/2.0/postgres/synced_tables/{SYNCED}")
    pipeline_id = find_key(synced, "pipeline_id")
    w.pipelines.start_update(pipeline_id=pipeline_id)
    print(f"Synced table exists, triggered snapshot refresh (pipeline {pipeline_id})")
except Exception as e:
    if "not found" not in str(e).lower() and "does not exist" not in str(e).lower():
        raise
    op = w.api_client.do("POST", "/api/2.0/postgres/synced_tables", query={"synced_table_id": SYNCED}, body={"spec": {
        "branch": BRANCH,
        "postgres_database": PG_DB,
        "source_table_full_name": f"{fq}.throttle_rules",
        "primary_key_columns": ["rule_id"],
        "scheduling_policy": "SNAPSHOT",
        "create_database_objects_if_missing": True,
        "new_pipeline_spec": {"storage_catalog": catalog, "storage_schema": schema},
    }})
    print(f"Creating synced table {SYNCED} (operation {op.get('name')})")

delta_rows = spark.table(f"{fq}.throttle_rules").count()
delta_ts = spark.table(f"{fq}.throttle_rules").selectExpr("max(generated_at)").first()[0]
deadline = time.time() + 1800
while True:
    try:
        with pg_connect() as conn:
            n, ts = conn.execute(f'SELECT count(*), max(generated_at) FROM "{schema}"."throttle_rules_serving"').fetchone()
        if n == delta_rows and ts is not None and ts.replace(tzinfo=None) >= delta_ts.replace(tzinfo=None):
            break
        print(f"  Lakebase rows {n:,} / Delta rows {delta_rows:,}, waiting for sync...")
    except (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName):
        print("  serving table not created yet, waiting...")
    if time.time() > deadline:
        raise TimeoutError("Synced table did not converge within 30 minutes")
    time.sleep(30)
print(f"In sync: {n:,} rules in Lakebase == {delta_rows:,} in Delta (generated_at {ts})")
```
Output:
```text
Synced table exists, triggered snapshot refresh (pipeline 371257e2-40c9-41a3-8b4a-6c52d356e50a)
  Lakebase rows 113,762 / Delta rows 113,762, waiting for sync...
In sync: 113,762 rules in Lakebase == 113,762 in Delta (generated_at 2026-10-20 18:19:06.616211+00:00)
```

## 2. Ops writeback table and least-privilege Postgres grants for the app

### Cell 8
```python
with pg_connect() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS halyard_ops")
    conn.execute("""
      CREATE TABLE IF NOT EXISTS halyard_ops.rule_overrides (
        override_id  BIGSERIAL PRIMARY KEY,
        rule_id      TEXT NOT NULL,
        action       TEXT NOT NULL CHECK (action IN ('ALLOW', 'REDUCE', 'THROTTLE')),
        send_rate    NUMERIC(4,2) NOT NULL CHECK (send_rate BETWEEN 0 AND 1),
        reason       TEXT NOT NULL,
        created_by   TEXT NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        expires_at   TIMESTAMPTZ
      )""")
    conn.execute("CREATE INDEX IF NOT EXISTS rule_overrides_rule_idx ON halyard_ops.rule_overrides (rule_id, created_at DESC)")
    try:
        conn.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        conn.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (app_sp,))
        print(f"Created Postgres role for app SP {app_sp}")
    except Exception as e:
        print(f"App SP role already present ({str(e).splitlines()[0][:80]})")
    for g in [
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{app_sp}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{app_sp}"',
        f'GRANT USAGE ON SCHEMA halyard_ops TO "{app_sp}"',
        f'GRANT SELECT, INSERT ON halyard_ops.rule_overrides TO "{app_sp}"',
        f'GRANT USAGE ON SEQUENCE halyard_ops.rule_overrides_override_id_seq TO "{app_sp}"',
    ]:
        conn.execute(g)
        print(f"OK  {g}")
    print("App SP can read rules and insert overrides. It cannot UPDATE or DELETE history (audit trail by design).")
```
Output:
```text
App SP role already present (role "b18ba3d7-1529-48c0-ae90-dde5403373b0" already exists)
OK  GRANT USAGE ON SCHEMA "ssp_traffic_shaping" TO "b18ba3d7-1529-48c0-ae90-dde5403373b0"
OK  GRANT SELECT ON ALL TABLES IN SCHEMA "ssp_traffic_shaping" TO "b18ba3d7-1529-48c0-ae90-dde5403373b0"
OK  GRANT USAGE ON SCHEMA halyard_ops TO "b18ba3d7-1529-48c0-ae90-dde5403373b0"
OK  GRANT SELECT, INSERT ON halyard_ops.rule_overrides TO "b18ba3d7-1529-48c0-ae90-dde5403373b0"
OK  GRANT USAGE ON SEQUENCE halyard_ops.rule_overrides_override_id_seq TO "b18ba3d7-1529-48c0-ae90-dde5403373b0"
App SP can read rules and insert overrides. It cannot UPDATE or DELETE history (audit trail by design).
```

## 3. Edge lookup benchmark: point reads by rule key

### Cell 10
```python
import statistics
keys = [r.rule_id for r in spark.table(f"{fq}.throttle_rules").sample(fraction=0.05, seed=1).limit(500).collect()]
with pg_connect() as conn:
    q = f"SELECT effective_action, effective_send_rate FROM ({EFFECTIVE_SQL}) e WHERE rule_id = %s"
    for k in keys[:20]:
        conn.execute(q, (k,)).fetchone()  # warm up
    lat = []
    for k in keys:
        t0 = time.perf_counter()
        conn.execute(q, (k,)).fetchone()
        lat.append((time.perf_counter() - t0) * 1000)
lat.sort()
print(f"{len(lat)} point lookups from a serverless notebook (includes network round trip):")
print(f"  p50 {statistics.median(lat):.2f} ms   p95 {lat[int(len(lat) * 0.95)]:.2f} ms   p99 {lat[int(len(lat) * 0.99)]:.2f} ms")
with pg_connect() as conn:
    print("\nEffective action mix served from Lakebase:")
    for row in conn.execute(f"SELECT effective_action, count(*), round(sum(est_daily_calls_saved)) FROM ({EFFECTIVE_SQL}) e GROUP BY 1 ORDER BY 2 DESC"):
        print(f"  {row[0]:<9} {row[1]:>7,} rules   {int(row[2]):>12,} est. calls saved/day (sample scale)")
```
Output:
```text
500 point lookups from a serverless notebook (includes network round trip):
  p50 2.65 ms   p95 5.11 ms   p99 8.45 ms

Effective action mix served from Lakebase:
  ALLOW      63,864 rules              0 est. calls saved/day (sample scale)
  THROTTLE   36,241 rules        100,782 est. calls saved/day (sample scale)
  REDUCE     13,657 rules         20,406 est. calls saved/day (sample scale)
```
