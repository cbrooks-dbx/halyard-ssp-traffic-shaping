# Task `genai_partner_briefs`

- task run id: 186679299249132
- notebook: `src/notebooks/05_genai_partner_briefs`
- started: 2026-10-20 18:19:31 UTC, duration 34s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 05 - GenAI partner briefs (`ai_query` on Foundation Model APIs)

Throttling a DSP partner's traffic is a commercial conversation, not just an engineering change. Partner
managers have to explain to each DSP *what* is being reduced and *why it is good for them too* (fewer wasted
calls, same access to the inventory they actually buy). Today that is a spreadsheet and a blank email.

This notebook drafts that brief for every DSP, grounded only in the numbers computed upstream, plus a
weekly executive summary for the CRO. The prompt forbids inventing numbers, and the output stays in a
governed UC table that the app and Genie both read.

### Cell 2
```python
dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
llm = dbutils.widgets.get("llm_endpoint")
fq = f"{catalog}.{schema}"
```
Output:
```text
[]
```

### Cell 3
```python
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
```
Output:
```text
=== Northstar DSP  (calls cut 9.6%, revenue at risk 0.71%)
Hi Northstar DSP team,

Starting next cycle, Halyard Exchange will reduce the bid requests we send you by 9.6%, throttling 496 segments concentrated in CTV and native formats, CTV device and mobile app inventory, and US, GB, and IN traffic where your observed bid rate is 12.7% and only 0.71% of your winning spend originates. This means fewer low-signal requests hitting your infrastructure with no meaningful loss to the inventory you actually convert on, reducing your processing overhead while preserving full access to the supply driving your results. Could you confirm these segments align with your current targeting strategy, or flag any specific segments within those 496 you would like us to keep open before we finalize the change?

Thanks,
Halyard Exchange Partner Management

=== Tidewater Media  (calls cut 60.3%, revenue at risk 0.44%)
Hi Tidewater Media team,

Starting next month, Halyard Exchange will reduce the volume of bid requests we send you by 60.3%, concentrating cuts across banner and native formats on mobile_web and mobile_app inventory in the US, AU, and GB, spanning 7,781 segments where your observed bid rate is 8.2%. Because only 0.44% of your winning spend originates from this throttled traffic, the change preserves virtually all inventory you actually buy while meaningfully lowering the request volume hitting your infrastructure. We want to confirm this aligns with your targeting strategy, so please let us know by end of week whether any of the 7,781 segments should be kept active or if you have concerns we should address before rollout.

=== Quanta Ads  (calls cut 14.3%, revenue at risk 0.18%)
Hi Quanta Ads team,

Starting next cycle, Halyard Exchange will stop sending you 14.3% of current bid requests, concentrating reductions across CTV and banner formats, CTV and desktop devices, and US, IN, and GB traffic within 744 segments where your observed bid rate is 21.8% and winning spend in the affected traffic is just 0.18% of your total. This means fewer irrelevant requests hitting your infrastructure while preserving access to essentially all inventory you actually buy. We want to confirm this aligns with your targeting strategy, so please let us know if any of the 744 throttled segments should be kept active or if anything looks off before we finalize the change.

Thanks,
Halyard Exchange Partner Team

=== Kestrel Retail  (calls cut 49.0%, revenue at risk 1.91%)
Hi Kestrel Retail team,

Starting next cycle, Halyard Exchange will reduce the volume of bid requests we send you by 49.0%, concentrating cuts on banner and video formats, mobile web and mobile app inventory, and AU, GB, and IN traffic across 7,532 segments where your current bid rate is 9.8%. This change means fewer low-match requests hitting your infrastructure while preserving access to the inventory that drives your spend, given that only 1.9% of your winning spend sits within the throttled traffic. We want to make sure nothing meaningful falls through, so please confirm the targeting parameters above look correct for your US shopper campaigns, or flag any specific segments within those 7,532 you would like us to keep open. Happy to jump on a call this week.

=== Ember Exchange  (calls cut 1.3%, revenue at risk 0.54%)
Hi team,

Starting [date], Halyard Exchange will stop sending Ember Exchange approximately 1.3% of current bid request volume, concentrating cuts on CTV format, CTV device traffic, and FR inventory within one segment where your observed bid rate is 18.2% and winning spend exposure is 0.54% of total. This means fewer irrelevant requests hitting your infrastructure with no meaningful impact on the inventory you are actually buying. We wanted to flag this proactively and ask that you confirm this aligns with your current EU and LATAM brand campaign targeting, or let us know if you want the FR CTV segment preserved before we move forward.

Thanks,
[Name]
Halyard Exchange

=== Lumen Bid  (calls cut 81.5%, revenue at risk 79.77%)
Hi Lumen Bid team,

Starting next cycle, Halyard Exchange will stop sending you 81.5% of your current bid request volume, concentrating cuts across banner and video formats on mobile_web and mobile_app in the US, AU, and IN, spanning 10,074 segments where your observed bid rate is 0.4%. This reduces infrastructure overhead on your end while preserving access to the inventory that matters, noting that 79.77% of your winning spend currently sits within the throttled traffic, so we want to align on that before proceeding. Could you confirm your targeting priorities across those segments or flag any specific ones you want kept open so we can calibrate accordingly?

Thanks,
Halyard Exchange Partner Management

=== Orchid Native  (calls cut 47.8%, revenue at risk 2.29%)
Hi Orchid Native team,

Starting next cycle, Halyard Exchange will stop sending Orchid Native approximately 47.8% of current bid requests, concentrating cuts on banner and video formats, mobile_web and ctv_device traffic, and AU, US, and GB inventory where your observed bid rate runs at 6.3%. This reduces infrastructure overhead on your end by eliminating low-engagement requests while preserving access to the inventory that actually drives your spend, given that only 2.3% of your winning spend sits within the 5,189 throttled segments. We want to make sure nothing falls through the cracks, so could you confirm these segments align with your current targeting strategy, or flag any specific segments within the throttled pool you would like us to keep open? Happy to adjust before we finalize.

=== Atlas Programmatic  (calls cut 8.5%, revenue at risk 0.53%)
Hi Atlas Programmatic team,

Starting next cycle, Halyard Exchange will stop routing approximately 8.5% of current bid requests to your seat, concentrating reductions across banner and native formats on mobile web and desktop inventory in AU, JP, and IN, spanning 814 segments where your observed bid rate is 12.4%. Because only 0.5% of your winning spend sits within the throttled traffic, you retain access to virtually everything you actually buy while eliminating the infrastructure cost of evaluating requests you consistently pass on. We would appreciate confirmation that your active targeting parameters are unaffected, or if any of the 814 segments should be kept open, please flag those to us by end of week so we can adjust before the change goes live.

Thanks,
Halyard Exchange Partner Management

=== Vireo Brand  (calls cut 32.9%, revenue at risk 1.67%)
Hi Vireo Brand team,

Starting next month, Halyard Exchange will stop sending you 32.9% of current bid requests, concentrating cuts in banner and native formats across mobile web and desktop inventory in AU, GB, and IN, affecting 3,562 segments where your observed bid rate is 5.2%. Because only 1.7% of your winning spend sits within this throttled traffic, you retain meaningful access to the inventory you actually buy while reducing unnecessary request volume and the infrastructure overhead that comes with it on your end. We want to confirm this aligns with your targeting priorities before we proceed, so please let us know if any of the 3,562 segments flagged for throttling are ones you would like kept active.

=== Sable Asia  (calls cut 2.8%, revenue at risk 0.59%)
Hi team,

Starting [date], Halyard Exchange will stop sending Sable Asia approximately 2.8% of current bid request volume, concentrating cuts across CTV format, CTV devices, and traffic from IN, AU, and JP where your observed bid rate is 18.7% and winning spend in the affected segments represents only 0.59% of your total. This means fewer irrelevant requests hitting your infrastructure while preserving access to the inventory you actually buy. The 48 throttled segments were selected precisely because they sit outside your active spend patterns, so your effective reach should remain intact. Could you confirm these segments align with your current targeting, or flag any of the 48 you would like kept in the feed? Happy to share the full segment list before we go live.
```

### Executive summary for the CRO

### Cell 5
```python
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
```
Output:
```text
Traffic Shaping Program Update — Week of [Date]

Annual net benefit is $2.1M (worst case $1.8M), driven by $3.3M in hard infrastructure savings against an expected revenue loss of $1.3M. The $2.7M QPS reallocation upside is excluded from both figures.

Revenue safety is strong. Conservative modeling assumes every throttled win is permanently lost, putting 0.83% of net revenue at risk. Auction replay analysis — where the runner-up bid still wins — reduces expected loss to 0.70% of revenue. Retained revenue of 99.2% holds above our 99% guardrail. Fill rate impact is minimal: 57.2% to 56.8%.

Most affected partners: Lumen Bid (82% of calls cut, 79.8% revenue at risk) is an outlier requiring attention. Tidewater Media (60%), Kestrel Retail (49%), and Orchid Native (48%) see heavy call reductions but minimal revenue exposure.

Decision needed: Should we initiate a direct conversation with Lumen Bid given their concentrated revenue risk before we expand the program?
```
