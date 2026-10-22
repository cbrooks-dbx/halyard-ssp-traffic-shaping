# Task `genie_space`

- task run id: 915322192317950
- notebook: `src/notebooks/07_genie_space`
- started: 2026-10-20 18:20:06 UTC, duration 53s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 07 - Genie space: natural-language Q&A for yield, partner and finance teams

Partner managers and finance ask the same ten questions every week ("how much are we cutting from
Tidewater?", "what is this worth?", "which publishers lose the most bids?"). Today each one is a ticket to
the data team. The Genie space answers them directly over the governed gold and rules tables, and the
app embeds it.

Created idempotently through the Genie REST API. Re-running updates the space in place.

### Cell 2
```python
dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
dbutils.widgets.text("warehouse_id", "9cd919d96b11bf1c")
dbutils.widgets.text("app_name", "halyard-traffic-shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
app_name = dbutils.widgets.get("app_name")
fq = f"{catalog}.{schema}"
TITLE = "Halyard Exchange - Traffic Shaping"
```
Output:
```text
[]
```

### Cell 3
```python
import json
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
app_sp = w.apps.get(app_name).service_principal_client_id

tables = ["gold_exchange_daily", "gold_dsp_daily", "gold_segment_daily", "throttle_rules", "dsp_throttle_impact",
          "throttle_impact_summary", "dsp_partner_briefs", "dim_dsp_partners", "dim_publishers", "value_assumptions"]
questions = [
    "What is the annual infra savings and revenue at risk from the current throttle rules?",
    "Which DSPs have the lowest bid rate on the traffic we send them?",
    "How many outbound calls per day are we sending to Lumen Bid, and what share will be throttled?",
    "Show daily bid rate and fill rate for the last 14 days",
    "Which publishers have the most throttled segments?",
    "What share of Tidewater Media's traffic is banner on mobile web?",
    "Which countries drive the most wasted calls?",
    "Show the partner brief for Atlas Programmatic",
]
instructions = """You answer questions for Halyard Exchange, a supply-side ad platform (SSP), about traffic shaping:
not sending bid requests to DSP partners that will not bid on them.

DEFINITIONS
- An outbound call is one bid request sent to one DSP. An auction fans out to several DSPs.
- bid_rate = bids / outbound_calls. Fill rate = auctions with a winner / auctions.
- Waste = outbound calls that get no bid. Throttled calls = calls not sent because of a THROTTLE or REDUCE rule.
- Actions: THROTTLE sends 5% (exploration), REDUCE sends 50%, ALLOW sends 100%.
- net_revenue_usd is Halyard's take (15% of gross_spend_usd).

WHICH TABLE TO USE
- Program-level dollar impact: throttle_impact_summary (one row per metric; read the value column, unit tells you the format).
- Per-DSP impact of the rules: dsp_throttle_impact. Per-segment rules: throttle_rules.
- Trends over time: gold_exchange_daily (exchange) and gold_dsp_daily (per DSP).
- Segment drill-downs: gold_segment_daily.
- Partner brief text: dsp_partner_briefs.
- Assumptions behind the dollar figures: value_assumptions.

FORMATTING
- Lead with the answer. Percentages to one decimal place. Dollars as $1.2M. Name DSPs by dsp_name, not dsp_id.
- Columns ending in _pct and bid_rate are ratios (0.25 = 25%)."""

example_sqls = [
    ("What is the annual infra savings from traffic shaping?",
     f"SELECT value AS annual_infra_savings_usd FROM {fq}.throttle_impact_summary WHERE metric = 'annual_infra_savings_usd'"),
    ("Which DSPs have the lowest bid rate?",
     f"SELECT dsp_name, sum(bids) / sum(outbound_calls) AS bid_rate, sum(outbound_calls) AS outbound_calls FROM {fq}.gold_dsp_daily GROUP BY dsp_name ORDER BY bid_rate"),
]


def new_id(i, prefix):
    return f"{prefix}{i:031x}"


space = {
    "version": 2,
    "config": {"sample_questions": [{"id": new_id(i + 1, "a"), "question": [q]} for i, q in enumerate(questions)]},
    "data_sources": {"tables": sorted([{"identifier": f"{fq}.{t}"} for t in tables], key=lambda x: x["identifier"])},
    "instructions": {
        "text_instructions": [{"id": new_id(1, "b"), "content": [l + "\n" for l in instructions.split("\n")]}],
        "example_question_sqls": [{"id": new_id(i + 1, "c"), "question": [q], "sql": [s]} for i, (q, s) in enumerate(example_sqls)],
    },
}

existing = [s for s in w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", []) if s.get("title") == TITLE]
body = {"title": TITLE, "warehouse_id": warehouse_id,
        "description": "Ask about DSP bid rates, wasted outbound calls, throttle rules and their dollar impact.",
        "serialized_space": json.dumps(space)}
if existing:
    space_id = existing[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"Updated Genie space {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    print(f"Created Genie space {space_id}")

w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{space_id}",
                body={"access_control_list": [{"service_principal_name": app_sp, "permission_level": "CAN_RUN"}]})
print(f"Granted CAN_RUN to app SP {app_sp}")

spark.sql(f"CREATE TABLE IF NOT EXISTS {fq}.app_config (key STRING, value STRING) COMMENT 'Runtime config the app reads at startup'")
spark.sql(f"MERGE INTO {fq}.app_config t USING (SELECT 'genie_space_id' AS key, '{space_id}' AS value) s ON t.key = s.key "
          "WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *")
spark.sql(f"GRANT SELECT ON TABLE {fq}.app_config TO `{app_sp}`")
for t in ["throttle_rules", "dsp_throttle_impact", "throttle_impact_summary", "dsp_partner_briefs", "threshold_curve", "exec_weekly_summary"]:
    spark.sql(f"GRANT SELECT ON TABLE {fq}.{t} TO `{app_sp}`")
```
Output:
```text
Updated Genie space 01f1ccb10c7f12b78b235fea709b4bc5
Granted CAN_RUN to app SP b18ba3d7-1529-48c0-ae90-dde5403373b0
```

### Smoke test: ask the space a real question

### Cell 5
```python
def ask(question):
    msg = w.api_client.do("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", body={"content": question})
    conv, mid = msg["conversation_id"], msg["message_id"]
    for _ in range(60):
        m = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{mid}")
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    out = {"status": m["status"], "text": None, "sql": None, "rows": None}
    for a in m.get("attachments", []):
        if a.get("text"):
            out["text"] = a["text"]["content"]
        if a.get("query"):
            out["sql"] = a["query"]["query"]
            res = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{mid}/attachments/{a['attachment_id']}/query-result")
            out["rows"] = res.get("statement_response", {}).get("result", {}).get("data_array")
    return out


for q in ["Which 3 DSPs have the lowest bid rate on the traffic we send them?",
          "What is the annual infra savings from the current throttle rules?"]:
    r = ask(q)
    print(f"Q: {q}\nstatus: {r['status']}\nSQL:\n{r['sql']}\nrows: {r['rows']}\nanswer: {r['text']}\n")
```
Output:
```text
Q: Which 3 DSPs have the lowest bid rate on the traffic we send them?
status: COMPLETED
SQL:
WITH dsp_bid_rates AS (
  SELECT
    `dsp_name`,
    try_divide(SUM(`bids`),SUM(`outbound_calls`)) AS `bid_rate`,
    SUM(`outbound_calls`) AS `outbound_calls`
  FROM `cmegdemos_catalog`.`ssp_traffic_shaping`.`gold_dsp_daily`
  WHERE `dsp_name` IS NOT NULL
    AND `bids` IS NOT NULL
    AND `outbound_calls` IS NOT NULL
  GROUP BY `dsp_name`
  HAVING SUM(`outbound_calls`) > 0
), ranked AS (
  SELECT
    `dsp_name`,
    `bid_rate`,
    `outbound_calls`,
    RANK() OVER (ORDER BY `bid_rate` ASC) AS `bid_rate_rank`
  FROM dsp_bid_rates
)
SELECT
  `dsp_name`,
  `bid_rate`,
  `outbound_calls`
FROM ranked
WHERE `bid_rate_rank` <= 3
ORDER BY `bid_rate` ASC, `dsp_name` ASC
rows: [['Lumen Bid', '0.004375206801801154', '556088'], ['Vireo Brand', '0.05148285882809915', '556088'], ['Orchid Native', '0.06348815295420869', '556088']]
answer: The **3 DSPs with the lowest bid rate** are **Lumen Bid** at **0.4%**, **Vireo Brand** at **5.1%**, and **Orchid Native** at **6.3%**. Each was sent **556,088** outbound calls, and **Lumen Bid** is a clear low outlier with a bid rate far below the other two.

Q: What is the annual infra savings from the current throttle rules?
status: COMPLETED
SQL:
SELECT `value` AS `annual_infra_savings_usd`
FROM `cmegdemos_catalog`.`ssp_traffic_shaping`.`throttle_impact_summary`
WHERE `metric` = 'annual_infra_savings_usd'
rows: [['3340903.9958488815']]
answer: The annual infrastructure savings from the current throttle rules is **$3.3M** per year. Based on the single result returned, the reported value for `annual_infra_savings_usd` is **$3,340,903.9958488815**, which rounds to **$3.3M**.
```
