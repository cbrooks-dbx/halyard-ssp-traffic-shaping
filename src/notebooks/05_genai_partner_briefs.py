# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - GenAI partner briefs (`ai_query` on Foundation Model APIs)
# MAGIC
# MAGIC Throttling a DSP partner's traffic is a commercial conversation, not just an engineering change. Partner
# MAGIC managers have to explain to each DSP *what* is being reduced and *why it is good for them too* (fewer wasted
# MAGIC calls, same access to the inventory they actually buy). Today that is a spreadsheet and a blank email.
# MAGIC
# MAGIC This notebook drafts that brief for every DSP, grounded only in the numbers computed upstream, plus a
# MAGIC weekly executive summary for the CRO. The prompt forbids inventing numbers, and the output stays in a
# MAGIC governed UC table that the app and Genie both read.

# COMMAND ----------

dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm = dbutils.widgets.get("llm_endpoint")
fq = f"{catalog}.{schema}"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fq}.dsp_partner_briefs
COMMENT 'LLM-drafted partner brief per DSP explaining the traffic-shaping change, grounded in dsp_throttle_impact. Review before sending.'
AS
SELECT
  d.dsp_id, d.dsp_name, p.specialty,
  d.calls_cut_pct, d.revenue_at_risk_pct, d.observed_bid_rate,
  ai_query(
    '{llm}',
    concat(
      'You are a partner manager at Halyard Exchange, a supply-side ad platform. Draft a short brief (max 120 words) to send to the DSP partner below, ',
      'explaining an upcoming traffic-shaping change. Tone: collaborative, specific, no hype. Structure: one sentence on what changes, ',
      'one on why it benefits them (fewer wasted bid requests, lower infra cost on their side, same access to inventory they buy), ',
      'then one ask (confirm targeting or flag any segments they want kept). Use ONLY the numbers given. Do not invent figures. ',
      'Percentages to one decimal place. Plain text, no markdown.\\n\\n',
      'DSP: ', d.dsp_name, ' (', p.specialty, ')\\n',
      'Their observed bid rate on traffic we send today: ', format_number(d.observed_bid_rate * 100, 1), '%\\n',
      'Share of their bid requests we will stop sending: ', format_number(d.calls_cut_pct * 100, 1), '%\\n',
      'Segments throttled: ', cast(d.segments_throttled AS STRING), '\\n',
      'Where the cuts concentrate - formats: ', coalesce(nullif(d.top_throttled_formats, ''), 'none'),
      '; devices: ', coalesce(nullif(d.top_throttled_devices, ''), 'none'),
      '; countries: ', coalesce(nullif(d.top_throttled_countries, ''), 'none'), '\\n',
      'Share of their winning spend that sits in throttled traffic: ', format_number(d.revenue_at_risk_pct * 100, 2), '%'
    )
  ) AS partner_brief,
  '{llm}' AS model_endpoint,
  current_timestamp() AS generated_at
FROM {fq}.dsp_throttle_impact d
JOIN {fq}.dim_dsp_partners p USING (dsp_id)
""")
for r in spark.table(f"{fq}.dsp_partner_briefs").orderBy("dsp_id").collect():
    print(f"=== {r.dsp_name}  (calls cut {r.calls_cut_pct:.1%}, revenue at risk {r.revenue_at_risk_pct:.2%})\n{r.partner_brief}\n")

# COMMAND ----------

# MAGIC %md ### Executive summary for the CRO

# COMMAND ----------

facts = "\n".join(f"- {r.metric}: {r.value:.4f} ({r.unit}) - {r.definition}" for r in spark.table(f"{fq}.throttle_impact_summary").collect())
dsp_lines = "\n".join(f"- {r.dsp_name}: {r.calls_cut_pct:.1%} of calls cut, {r.revenue_at_risk_pct:.2%} of its revenue at risk"
                      for r in spark.table(f"{fq}.dsp_throttle_impact").orderBy("calls_cut_pct", ascending=False).collect())
prompt = (
    "You are the VP of Yield at Halyard Exchange (an SSP) writing a weekly update for the CRO and CFO about the traffic-shaping "
    "program. Max 150 words. Lead with the annual dollar impact, then revenue safety, then which partners are most affected and the "
    "one decision you need from leadership. Use ONLY these facts, convert ratios to percentages, round dollars to $0.1M. Plain text.\n\n"
    f"Program metrics:\n{facts}\n\nPer-DSP effect:\n{dsp_lines}"
)
spark.createDataFrame([(prompt,)], "prompt string").createOrReplaceTempView("exec_prompt")
spark.sql(f"""
CREATE OR REPLACE TABLE {fq}.exec_weekly_summary
COMMENT 'LLM-drafted weekly executive summary of the traffic-shaping program, grounded in throttle_impact_summary'
AS SELECT ai_query('{llm}', prompt) AS summary, '{llm}' AS model_endpoint, current_timestamp() AS generated_at FROM exec_prompt
""")
print(spark.table(f"{fq}.exec_weekly_summary").first().summary)
