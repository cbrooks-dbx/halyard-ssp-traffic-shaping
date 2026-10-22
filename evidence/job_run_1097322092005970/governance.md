# Task `governance`

- task run id: 126802945392040
- notebook: `src/notebooks/02_governance`
- started: 2026-10-20 18:14:04 UTC, duration 61s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 02 - Unity Catalog governance

Floor prices are contractually confidential between Halyard and each publisher, and DSP partner
managers should only ever see their own partner's traffic. This notebook puts that policy in
Unity Catalog, once, so it holds for SQL, Genie, notebooks and the app alike:

1. **Classification tags** on every table and on the sensitive column. Tag keys `domain`, `layer`, `contains_pii` and
   `sensitivity` are governed tags in this account, so values are chosen from their allowed lists; the
   Halyard-specific keys (`halyard_confidentiality`, `halyard_data_origin`) are free-form.
2. **An access-policy table** (`governance_access`) mapping principals to a role and DSP.
3. **A governed view** for partner managers: row-filtered to their DSP, floors masked to a price band.
4. **Least-privilege grants** for the app's service principal (read gold, nothing in bronze/silver).
5. **Lineage evidence** from system tables.

### Cell 2
```python
dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
dbutils.widgets.text("app_name", "halyard-traffic-shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
app_name = dbutils.widgets.get("app_name")
fq = f"{catalog}.{schema}"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
me = w.current_user.me().user_name
app_sp = w.apps.get(app_name).service_principal_client_id
print(f"Owner: {me}\nApp service principal: {app_sp}")
```
Output:
```text
Owner: chandler.brooks@databricks.com
App service principal: b18ba3d7-1529-48c0-ae90-dde5403373b0
```

## 1. Classification tags

### Cell 4
```python
layer_tags = {
    "bronze_auctions": "bronze", "silver_auctions": "silver", "silver_dsp_calls": "silver",
    "gold_segment_daily": "gold", "gold_dsp_daily": "gold", "gold_exchange_daily": "gold",
    "dim_dsp_partners": "silver", "dim_publishers": "silver",
}
results = []
for t, layer in layer_tags.items():
    kind = "MATERIALIZED VIEW" if t.startswith(("silver_dsp", "gold", "dim")) else "STREAMING TABLE"
    stmt = f"ALTER {kind} {fq}.{t} SET TAGS ('domain' = 'operations', 'layer' = '{layer}', 'contains_pii' = 'false', 'sensitivity' = 'internal', 'halyard_data_origin' = 'synthetic')"
    try:
        spark.sql(stmt)
        results.append((t, "tagged"))
    except Exception as e:
        results.append((t, f"skipped: {str(e).splitlines()[0][:120]}"))
for col_stmt in [
    f"ALTER MATERIALIZED VIEW {fq}.silver_dsp_calls ALTER COLUMN floor_cpm SET TAGS ('sensitivity' = 'internal', 'halyard_confidentiality' = 'publisher_floor_price')",
    f"ALTER MATERIALIZED VIEW {fq}.dim_publishers ALTER COLUMN avg_floor_cpm SET TAGS ('sensitivity' = 'internal', 'halyard_confidentiality' = 'publisher_floor_price')",
]:
    try:
        spark.sql(col_stmt)
        results.append((col_stmt.split(" ALTER COLUMN ")[1].split(" ")[0], "column tagged sensitivity=internal, halyard_confidentiality=publisher_floor_price"))
    except Exception as e:
        results.append(("column tag", f"skipped: {str(e).splitlines()[0][:120]}"))
for r in results:
    print(f"{r[0]:<28} {r[1]}")
```
Output:
```text
bronze_auctions              tagged
silver_auctions              tagged
silver_dsp_calls             tagged
gold_segment_daily           tagged
gold_dsp_daily               tagged
gold_exchange_daily          tagged
dim_dsp_partners             tagged
dim_publishers               tagged
floor_cpm                    column tagged sensitivity=internal, halyard_confidentiality=publisher_floor_price
avg_floor_cpm                column tagged sensitivity=internal, halyard_confidentiality=publisher_floor_price
```

## 2. Access-policy table and 3. governed partner view

### Cell 6
```python
spark.sql(f"""
CREATE OR REPLACE TABLE {fq}.governance_access (principal STRING, role STRING, dsp_id STRING)
COMMENT 'Who may see what. role = yield_ops (all partners, true floors) or partner_manager (one DSP, floors banded)'
""")
spark.sql(f"""
INSERT INTO {fq}.governance_access VALUES
  ('{me}', 'yield_ops', NULL),
  ('{app_sp}', 'yield_ops', NULL),
  ('partner.manager.tidewater@halyard.example', 'partner_manager', 'DSP02'),
  ('partner.manager.lumen@halyard.example', 'partner_manager', 'DSP06')
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {fq}.v_partner_segment_performance
COMMENT 'Governed partner view: partner managers see only their DSP; floor prices are banded unless the caller is yield_ops'
AS
WITH caller AS (
  SELECT role, dsp_id FROM {fq}.governance_access WHERE principal = current_user()
)
SELECT
  s.event_date, s.dsp_id, s.dsp_name, s.publisher_id, s.vertical, s.ad_format, s.device_type,
  s.geo_country, s.supply_path, s.outbound_calls, s.bids, s.wins, s.bid_rate,
  CASE WHEN EXISTS (SELECT 1 FROM caller WHERE role = 'yield_ops') THEN p.avg_floor_cpm
       ELSE CASE WHEN p.avg_floor_cpm < 1 THEN 0.5 WHEN p.avg_floor_cpm < 3 THEN 2.0 ELSE 5.0 END
  END AS publisher_floor_cpm
FROM {fq}.gold_segment_daily s
JOIN {fq}.dim_publishers p USING (publisher_id)
WHERE EXISTS (SELECT 1 FROM caller WHERE role = 'yield_ops')
   OR s.dsp_id IN (SELECT dsp_id FROM caller WHERE role = 'partner_manager')
""")
visible = spark.sql(f"SELECT count(DISTINCT dsp_id) AS dsps_visible, count(*) AS rows_visible FROM {fq}.v_partner_segment_performance").first()
print(f"As {me} (yield_ops): {visible.dsps_visible} DSPs, {visible.rows_visible:,} rows visible")
print("A partner manager mapped to DSP02 would see 1 DSP, with floors banded to 0.5 / 2.0 / 5.0")
```
Output:
```text
As chandler.brooks@databricks.com (yield_ops): 10 DSPs, 511,889 rows visible
A partner manager mapped to DSP02 would see 1 DSP, with floors banded to 0.5 / 2.0 / 5.0
```

## 4. Least-privilege grants for the app service principal

### Cell 8
```python
grants = [
    f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app_sp}`",
    f"GRANT USE SCHEMA ON SCHEMA {fq} TO `{app_sp}`",
]
for t in ["gold_segment_daily", "gold_dsp_daily", "gold_exchange_daily", "dim_dsp_partners", "dim_publishers",
          "value_assumptions", "v_partner_segment_performance", "governance_access"]:
    grants.append(f"GRANT SELECT ON TABLE {fq}.{t} TO `{app_sp}`")
for g in grants:
    try:
        spark.sql(g)
        print(f"OK   {g}")
    except Exception as e:
        print(f"FAIL {g}: {str(e).splitlines()[0][:160]}")
print("\nNot granted (by design): bronze_auctions, silver_auctions, silver_dsp_calls. The app never reads raw or call-level data.")
```
Output:
```text
OK   GRANT USE CATALOG ON CATALOG cmegdemos_catalog TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT USE SCHEMA ON SCHEMA cmegdemos_catalog.ssp_traffic_shaping TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.gold_segment_daily TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.gold_dsp_daily TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.gold_exchange_daily TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.dim_dsp_partners TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.dim_publishers TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.value_assumptions TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.v_partner_segment_performance TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`
OK   GRANT SELECT ON TABLE cmegdemos_catalog.ssp_traffic_shaping.governance_access TO `b18ba3d7-1529-48c0-ae90-dde5403373b0`

Not granted (by design): bronze_auctions, silver_auctions, silver_dsp_calls. The app never reads raw or call-level data.
```

## 5. Evidence: tags, comments and lineage as recorded by Unity Catalog

### Cell 10
```python
print(spark.sql(f"""
SELECT table_name, table_type, left(comment, 90) AS comment
FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name
""").toPandas().to_string(index=False))
```
Output:
```text
table_name        table_type                                                                                    comment
    __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_bronze_auctions_1           MANAGED Raw auction logs exactly as emitted by the bidder fleet, incrementally ingested with Auto 
   __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_dim_dsp_partners_1           MANAGED                       DSP partners Halyard sends bid requests to, with contracted QPS caps
     __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_dim_publishers_1           MANAGED                                      Publishers (supply) selling inventory through Halyard
     __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_gold_dsp_daily_1           MANAGED Daily partner scorecard: how much traffic each DSP receives vs how often it bids, wins and
__materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_gold_exchange_daily_1           MANAGED                 Exchange-level daily KPIs: auctions, fan-out, bid rate, fill rate, revenue
 __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_gold_segment_daily_1           MANAGED Daily outbound call volume, bids, wins and revenue per traffic segment (DSP x publisher x 
    __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_silver_auctions_1           MANAGED Typed, validated auctions. Rows failing expectations are dropped and counted in the pipeli
   __materialization_mat_d88fa974_f1d1_4680_81fd_b54e1e2b6239_silver_dsp_calls_1           MANAGED One row per outbound bid request (auction x DSP). Replayed auctions are deduplicated. This
                                                                      app_config           MANAGED                                                    Runtime config the app reads at startup
                                                                 bronze_auctions   STREAMING_TABLE Raw auction logs exactly as emitted by the bidder fleet, incrementally ingested with Auto 
                                                                dim_dsp_partners MATERIALIZED_VIEW                       DSP partners Halyard sends bid requests to, with contracted QPS caps
                                                                  dim_publishers MATERIALIZED_VIEW                                      Publishers (supply) selling inventory through Halyard
                                                              dsp_partner_briefs           MANAGED LLM-drafted partner brief per DSP explaining the traffic-shaping change, grounded in dsp_t
                                                             dsp_throttle_impact           MANAGED   Per-DSP effect of the rule set: share of calls cut, revenue at risk, where the cuts land
                                                                  e2e_validation           MANAGED                                                                                       None
                                  event_log_371257e2_40c9_41a3_8b4a_6c52d356e50a           MANAGED                                                                                       None
                                  event_log_d88fa974_f1d1_4680_81fd_b54e1e2b6239           MANAGED                                                                                       None
                                                             exec_weekly_summary           MANAGED LLM-drafted weekly executive summary of the traffic-shaping program, grounded in throttle_
                                                                  gold_dsp_daily MATERIALIZED_VIEW Daily partner scorecard: how much traffic each DSP receives vs how often it bids, wins and
                                                             gold_exchange_daily MATERIALIZED_VIEW                 Exchange-level daily KPIs: auctions, fan-out, bid rate, fill rate, revenue
                                                              gold_segment_daily MATERIALIZED_VIEW Daily outbound call volume, bids, wins and revenue per traffic segment (DSP x publisher x 
                                                               governance_access           MANAGED Who may see what. role = yield_ops (all partners, true floors) or partner_manager (one DSP
                                                                 silver_auctions   STREAMING_TABLE Typed, validated auctions. Rows failing expectations are dropped and counted in the pipeli
                                                                silver_dsp_calls MATERIALIZED_VIEW One row per outbound bid request (auction x DSP). Replayed auctions are deduplicated. This
                                                                 threshold_curve           MANAGED Holdout policy curve: share of outbound calls cut vs share of net revenue at risk, per sco
                                                         throttle_impact_summary           MANAGED   Business case for the current rule set, scaled to Halyard volume using value_assumptions
                                                                  throttle_rules           MANAGED One traffic-shaping rule per segment: action, send rate, predicted vs observed bid rate, e
                                                          throttle_rules_serving           FOREIGN                                                Database table synced from another UC table
                                                   v_partner_segment_performance              VIEW Governed partner view: partner managers see only their DSP; floor prices are banded unless
                                                               value_assumptions           MANAGED                                                                                       None
```

### Cell 11
```python
try:
    print(spark.sql(f"""
    SELECT table_name, tag_name, tag_value FROM {catalog}.information_schema.table_tags
    WHERE schema_name = '{schema}' AND tag_name IN ('layer', 'domain') ORDER BY table_name, tag_name
    """).toPandas().to_string(index=False))
    print(spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags
    WHERE schema_name = '{schema}'
    """).toPandas().to_string(index=False))
except Exception as e:
    print(f"tag views unavailable: {e}")
```
Output:
```text
table_name tag_name  tag_value
    bronze_auctions   domain operations
    bronze_auctions    layer     bronze
   dim_dsp_partners   domain operations
   dim_dsp_partners    layer     silver
     dim_publishers   domain operations
     dim_publishers    layer     silver
     gold_dsp_daily   domain operations
     gold_dsp_daily    layer       gold
gold_exchange_daily   domain operations
gold_exchange_daily    layer       gold
 gold_segment_daily   domain operations
 gold_segment_daily    layer       gold
    silver_auctions   domain operations
    silver_auctions    layer     silver
   silver_dsp_calls   domain operations
   silver_dsp_calls    layer     silver
      table_name   column_name                tag_name             tag_value
  dim_publishers avg_floor_cpm             sensitivity              internal
  dim_publishers avg_floor_cpm halyard_confidentiality publisher_floor_price
silver_dsp_calls     floor_cpm             sensitivity              internal
silver_dsp_calls     floor_cpm halyard_confidentiality publisher_floor_price
```

### Cell 12
```python
try:
    lineage = spark.sql(f"""
    SELECT DISTINCT source_table_name AS upstream, target_table_name AS downstream
    FROM system.access.table_lineage
    WHERE target_table_schema = '{schema}' AND target_table_catalog = '{catalog}'
      AND source_table_name IS NOT NULL AND event_date >= current_date() - INTERVAL 7 DAYS
    ORDER BY downstream, upstream
    """).toPandas()
    print(lineage.to_string(index=False) if len(lineage) else "Lineage rows not yet materialized (system tables lag by minutes)")
except Exception as e:
    print(f"system.access.table_lineage not readable by this user: {str(e).splitlines()[0][:160]}")
```
Output:
```text
upstream                    downstream
  silver_dsp_calls                gold_dsp_daily
  silver_dsp_calls           gold_exchange_daily
   bronze_auctions               silver_auctions
  dim_dsp_partners              silver_dsp_calls
    dim_publishers              silver_dsp_calls
   silver_auctions              silver_dsp_calls
    throttle_rules        throttle_rules_serving
    dim_publishers v_partner_segment_performance
gold_segment_daily v_partner_segment_performance
 governance_access v_partner_segment_performance
```
