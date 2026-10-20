# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Unity Catalog governance
# MAGIC
# MAGIC Floor prices are contractually confidential between Halyard and each publisher, and DSP partner
# MAGIC managers should only ever see their own partner's traffic. This notebook puts that policy in
# MAGIC Unity Catalog, once, so it holds for SQL, Genie, notebooks and the app alike:
# MAGIC
# MAGIC 1. **Classification tags** on every table and on the sensitive column. Tag keys `domain`, `layer`, `contains_pii` and
# MAGIC    `sensitivity` are governed tags in this account, so values are chosen from their allowed lists; the
# MAGIC    Halyard-specific keys (`halyard_confidentiality`, `halyard_data_origin`) are free-form.
# MAGIC 2. **An access-policy table** (`governance_access`) mapping principals to a role and DSP.
# MAGIC 3. **A governed view** for partner managers: row-filtered to their DSP, floors masked to a price band.
# MAGIC 4. **Least-privilege grants** for the app's service principal (read gold, nothing in bronze/silver).
# MAGIC 5. **Lineage evidence** from system tables.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 1. Classification tags

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 2. Access-policy table and 3. governed partner view

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 4. Least-privilege grants for the app service principal

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 5. Evidence: tags, comments and lineage as recorded by Unity Catalog

# COMMAND ----------

print(spark.sql(f"""
SELECT table_name, table_type, left(comment, 90) AS comment
FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name
""").toPandas().to_string(index=False))

# COMMAND ----------

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

# COMMAND ----------

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
