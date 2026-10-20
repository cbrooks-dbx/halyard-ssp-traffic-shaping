# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: schema, landing volume, reference data, value assumptions
# MAGIC
# MAGIC **Halyard Exchange** is a fictional mid-size supply-side platform (SSP). Every inbound ad auction
# MAGIC is fanned out to up to 10 DSP partners. Most of those outbound bid requests come back empty,
# MAGIC but Halyard still pays to build, serialize, send and wait on every one of them.
# MAGIC
# MAGIC This notebook creates the Unity Catalog objects the rest of the journey writes into, plus the
# MAGIC reference files that land alongside the raw bid logs (the way a real SSP drops partner and
# MAGIC publisher config exports next to its log files).

# COMMAND ----------

dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Halyard Exchange SSP traffic shaping - FE Bar build (synthetic data only)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_landing COMMENT 'Landing zone for raw auction logs (JSON.gz) and partner/publisher reference exports (CSV)'")
landing = f"/Volumes/{catalog}/{schema}/raw_landing"
print(f"Schema {catalog}.{schema} and volume {landing} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data
# MAGIC DSP partners and publishers are fictional. The *buying behaviour* of each DSP (what it actually bids on)
# MAGIC lives only inside the generator, the same way an SSP never sees a DSP's bidding logic.
# MAGIC The model has to learn it from the logs.

# COMMAND ----------

import os
import numpy as np
import pandas as pd

dsps = pd.DataFrame([
    ("DSP01", "Northstar DSP",      "Broad open-web display",                 9000, "NA"),
    ("DSP02", "Tidewater Media",    "CTV and premium video",                  4000, "NA"),
    ("DSP03", "Quanta Ads",         "Mobile app performance",                 7000, "Global"),
    ("DSP04", "Kestrel Retail",     "Retail media, US shoppers",              3000, "NA"),
    ("DSP05", "Ember Exchange",     "EU and LATAM brand campaigns",           3500, "EMEA"),
    ("DSP06", "Lumen Bid",          "Long-tail arbitrage",                    8000, "Global"),
    ("DSP07", "Orchid Native",      "Native and content recommendation",      2500, "NA"),
    ("DSP08", "Atlas Programmatic", "Agency trading desk, direct paths only", 6000, "Global"),
    ("DSP09", "Vireo Brand",        "Brand-safety-first CPG buyer",           2000, "NA"),
    ("DSP10", "Sable Asia",         "APAC mobile and web",                    3000, "APAC"),
], columns=["dsp_id", "dsp_name", "specialty", "contracted_qps_cap", "hq_region"])

rng = np.random.default_rng(7)
verticals = ["news", "sports", "entertainment", "gaming", "ugc", "lifestyle", "finance", "streaming"]
countries = ["US", "CA", "GB", "DE", "FR", "BR", "MX", "JP", "AU", "IN"]
country_w = np.array([0.42, 0.06, 0.08, 0.06, 0.05, 0.07, 0.05, 0.08, 0.05, 0.08])
name_a = ["Harbor", "Summit", "Pine", "Copper", "Lantern", "Willow", "Granite", "Beacon", "Meadow", "Falcon",
          "Riverside", "Bluebird", "Ironwood", "Juniper", "Maple", "Cascade", "Orbit", "Prairie", "Quarry", "Redwood"]
name_b = {"news": "Daily", "sports": "Sports Net", "entertainment": "Buzz", "gaming": "Games", "ugc": "Forums",
          "lifestyle": "Living", "finance": "Markets", "streaming": "TV"}
rows = []
for i in range(80):
    v = verticals[i % len(verticals)]
    inv = "ctv" if v == "streaming" else str(rng.choice(["app", "web"], p=[0.45, 0.55]))
    rows.append((
        f"PUB{i+1:03d}",
        f"{name_a[i % 20]} {name_b[v]}" + ("" if i < 20 else f" {i // 20 + 1}"),
        v,
        str(rng.choice(countries, p=country_w)),
        inv,
        str(rng.choice(["direct", "reseller_1hop", "reseller_2hop"], p=[0.5, 0.3, 0.2])),
        str(rng.choice(["authorized", "authorized", "authorized", "unlisted"])),
        round(float(rng.lognormal(np.log(1.2 if inv != "ctv" else 9.0), 0.35)), 2),
    ))
publishers = pd.DataFrame(rows, columns=["publisher_id", "publisher_name", "vertical", "primary_country",
                                         "inventory_type", "default_supply_path", "ads_txt_status", "avg_floor_cpm"])

os.makedirs(f"{landing}/reference", exist_ok=True)
dsps.to_csv(f"{landing}/reference/dsp_partners.csv", index=False)
publishers.to_csv(f"{landing}/reference/publishers.csv", index=False)
print(f"Wrote {len(dsps)} DSP partners and {len(publishers)} publishers to {landing}/reference/")
print(dsps.to_string(index=False))
print(publishers.head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Value assumptions
# MAGIC The synthetic logs are a sample. These parameters scale sample results to Halyard's real footprint,
# MAGIC so the business case is explicit and editable: Finance changes a number and every downstream KPI moves.

# COMMAND ----------

assumptions = [
    ("inbound_auctions_per_day", 25_000_000_000.0, "auctions/day", "Halyard inbound ad requests after IVT pre-filter"),
    ("cost_per_million_outbound_calls_usd", 0.12, "USD", "Request build + serialization + egress (~2KB at $0.02/GB) + DSP connection fleet, blended"),
    ("annual_gross_spend_usd", 1_200_000_000.0, "USD/yr", "Gross media spend clearing through Halyard"),
    ("take_rate", 0.15, "ratio", "Halyard net revenue share of gross spend"),
    ("qps_reallocation_uplift_pct", 0.015, "ratio", "Upside scenario: freed DSP QPS re-spent on high-propensity traffic, as % of net revenue (not booked)"),
    ("min_revenue_retention", 0.99, "ratio", "Guardrail: throttling may put at most 1% of net revenue at risk"),
]
spark.createDataFrame(pd.DataFrame(assumptions, columns=["parameter", "value", "unit", "rationale"])) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.value_assumptions")
print(spark.table(f"{catalog}.{schema}.value_assumptions").toPandas().to_string(index=False))
