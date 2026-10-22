# Key results

Queried live from Unity Catalog at 2026-10-22 15:17 UTC. Every number in the README and deck traces to a row below.


## Business case (`throttle_impact_summary`)

| metric | value | unit | definition | model_version |
|---|---|---|---|---|
| outbound_calls_cut_pct | 35.45% | pct | Share of outbound bid requests no longer sent | 3 |
| net_revenue_at_risk_pct | 0.83% | pct | Conservative: assumes every throttled win is lost, ignoring the runner-up bid | 3 |
| revenue_retained_pct | 99.17% | pct | Conservative bound; guardrail is >= 99% | 3 |
| replay_net_revenue_loss_pct | 0.70% | pct | Expected: auction replay where the runner-up bid can still win | 3 |
| replay_fill_rate_before | 57.16% | pct | Share of auctions with a winner today | 3 |
| replay_fill_rate_after | 56.79% | pct | Share of auctions with a winner under the rules | 3 |
| segments_throttled | 36,241 | count | Segments sent at 5% | 3 |
| segments_reduced | 13,657 | count | Segments sent at 50% | 3 |
| segments_total | 113,762 | count | Segments with traffic in the last 7 days | 3 |
| annual_outbound_calls | 78,527,142,857,143 | calls/yr | Inbound auctions x observed fan-out x 365 | 3 |
| annual_outbound_cost_usd | $9,423,257 | USD/yr | Cost of sending every call today | 3 |
| annual_infra_savings_usd | $3,340,904 | USD/yr | Hard savings: calls not sent x cost per call | 3 |
| annual_revenue_at_risk_usd | $1,502,866 | USD/yr | Upper bound on net revenue exposed | 3 |
| annual_expected_revenue_loss_usd | $1,258,747 | USD/yr | Expected net revenue lost, from the auction replay | 3 |
| annual_qps_reallocation_upside_usd | $2,700,000 | USD/yr | Scenario: freed DSP QPS spent on traffic they do buy | 3 |
| annual_net_benefit_usd | $2,082,157 | USD/yr | Hard savings minus expected revenue loss (upside excluded) | 3 |
| annual_net_benefit_worst_case_usd | $1,838,038 | USD/yr | Hard savings minus the conservative revenue-at-risk bound | 3 |

## Value assumptions (`value_assumptions`)

| parameter | value | unit | rationale |
|---|---|---|---|
| inbound_auctions_per_day | 2.5E10 | auctions/day | Halyard inbound ad requests after IVT pre-filter |
| cost_per_million_outbound_calls_usd | 0.12 | USD | Request build + serialization + egress (~2KB at $0.02/GB) + DSP connection fleet, blended |
| annual_gross_spend_usd | 1.2E9 | USD/yr | Gross media spend clearing through Halyard |
| take_rate | 0.15 | ratio | Halyard net revenue share of gross spend |
| qps_reallocation_uplift_pct | 0.015 | ratio | Upside scenario: freed DSP QPS re-spent on high-propensity traffic, as % of net revenue (not booked) |
| min_revenue_retention | 0.99 | ratio | Guardrail: throttling may put at most 1% of net revenue at risk |

## Exchange KPIs by day (`gold_exchange_daily`)

| event_date | auctions | outbound_calls | calls_per_auction | bid_rate | fill_rate | gross_spend_usd | net_revenue_usd |
|---|---|---|---|---|---|---|---|
| 2026-10-06 | 39720 | 341975 | 8.61 | 0.10194 | 0.5719 | 61.84 | 9.28 |
| 2026-10-07 | 39721 | 341757 | 8.6 | 0.10207 | 0.5692 | 61.43 | 9.21 |
| 2026-10-08 | 39720 | 341817 | 8.61 | 0.10185 | 0.5715 | 61.18 | 9.18 |
| 2026-10-09 | 39722 | 341758 | 8.6 | 0.10271 | 0.5766 | 62.93 | 9.44 |
| 2026-10-10 | 39722 | 341887 | 8.61 | 0.1023 | 0.573 | 62.24 | 9.34 |
| 2026-10-11 | 39720 | 341779 | 8.6 | 0.10214 | 0.5728 | 62.4 | 9.36 |
| 2026-10-12 | 39720 | 341806 | 8.61 | 0.10197 | 0.5692 | 61.57 | 9.24 |
| 2026-10-13 | 39720 | 341976 | 8.61 | 0.10195 | 0.5702 | 61.28 | 9.19 |
| 2026-10-14 | 39720 | 341835 | 8.61 | 0.10223 | 0.5721 | 62.15 | 9.32 |
| 2026-10-15 | 39720 | 341918 | 8.61 | 0.10249 | 0.5717 | 61.15 | 9.17 |
| 2026-10-16 | 39720 | 341630 | 8.6 | 0.10313 | 0.5722 | 63.34 | 9.5 |
| 2026-10-17 | 39721 | 341794 | 8.6 | 0.10187 | 0.5697 | 61.3 | 9.19 |
| 2026-10-18 | 39720 | 341723 | 8.6 | 0.10236 | 0.5748 | 62.17 | 9.33 |
| 2026-10-19 | 39722 | 341870 | 8.61 | 0.10184 | 0.5706 | 61.61 | 9.24 |

## Policy curve at the guardrail: ML vs historical rate (`threshold_curve`)

| method | threshold | calls_cut_pct | revenue_at_risk_pct | segments_shaped |
|---|---|---|---|---|
| historical_rate | 0.002 | 24.36 | 0.675 | 17091 |
| historical_rate | 0.004 | 30.77 | 0.898 | 23897 |
| historical_rate | 0.0058 | 34.7 | 1.096 | 27981 |
| historical_rate | 0.008 | 37.6 | 1.405 | 28373 |
| historical_rate | 0.01 | 39.63 | 1.644 | 29011 |
| historical_rate | 0.015 | 43.71 | 2.3 | 29721 |
| historical_rate | 0.02 | 46.18 | 2.915 | 29986 |
| historical_rate | 0.03 | 51.1 | 4.585 | 39760 |
| historical_rate | 0.05 | 56.91 | 7.759 | 46841 |
| ml_model | 0.002 | 18.93 | 0.179 | 23831 |
| ml_model | 0.004 | 30.01 | 0.499 | 36036 |
| ml_model | 0.0058 | 35.48 | 0.879 | 41530 |
| ml_model | 0.008 | 40.36 | 1.223 | 45645 |
| ml_model | 0.01 | 43.34 | 1.606 | 48118 |
| ml_model | 0.015 | 48.44 | 2.4 | 52658 |
| ml_model | 0.02 | 51.72 | 3.258 | 56201 |
| ml_model | 0.03 | 56.65 | 4.832 | 61817 |
| ml_model | 0.05 | 63.32 | 8.518 | 70053 |

## Per-DSP impact (`dsp_throttle_impact`)

| dsp_name | calls_7d | bid_rate_pct | calls_cut_pct | revenue_at_risk_pct | segments_throttled | top_throttled_formats | top_throttled_devices | top_throttled_countries |
|---|---|---|---|---|---|---|---|---|
| Lumen Bid | 278043 | 0.44 | 81.5 | 79.77 | 10074 | banner, video | mobile_web, mobile_app | US, AU, IN |
| Tidewater Media | 278043 | 8.22 | 60.3 | 0.44 | 7781 | banner, native | mobile_web, mobile_app | US, AU, GB |
| Kestrel Retail | 278043 | 9.81 | 49.0 | 1.91 | 7532 | banner, video | mobile_web, mobile_app | AU, GB, IN |
| Orchid Native | 278043 | 6.35 | 47.8 | 2.29 | 5189 | banner, video | mobile_web, ctv_device | AU, US, GB |
| Vireo Brand | 278043 | 5.15 | 32.9 | 1.67 | 3562 | banner, native | mobile_web, desktop | AU, GB, IN |
| Quanta Ads | 278043 | 21.8 | 14.3 | 0.18 | 744 | ctv, banner | ctv_device, desktop | US, IN, GB |
| Northstar DSP | 278043 | 12.71 | 9.6 | 0.71 | 496 | ctv, native | ctv_device, mobile_app | US, GB, IN |
| Atlas Programmatic | 278043 | 12.36 | 8.5 | 0.53 | 814 | banner, native | mobile_web, desktop | AU, JP, IN |
| Sable Asia | 75551 | 18.72 | 2.8 | 0.59 | 48 | ctv | ctv_device | IN, AU, JP |
| Ember Exchange | 92851 | 18.22 | 1.3 | 0.54 | 1 | ctv | ctv_device | FR |

## Rule mix (`throttle_rules`)

| action | send_rate | rules | calls_7d | avg_pred_bid_rate | observed_bid_rate | model_version |
|---|---|---|---|---|---|---|
| THROTTLE | 0.05 | 36241 | 742606 | 0.0026 | 0.0019 | 3 |
| REDUCE | 0.5 | 13657 | 285688 | 0.0081 | 0.007 | 3 |
| ALLOW | 1.0 | 63864 | 1364452 | 0.1428 | 0.1769 | 3 |

## GenAI: weekly CRO summary (`exec_weekly_summary`, databricks-claude-sonnet-4-6, 2026-10-20T18:19:57.450Z)

```text
Traffic Shaping Program Update — Week of [Date]

Annual net benefit is $2.1M (worst case $1.8M), driven by $3.3M in hard infrastructure savings against an expected revenue loss of $1.3M. The $2.7M QPS reallocation upside is excluded from both figures.

Revenue safety is strong. Conservative modeling assumes every throttled win is permanently lost, putting 0.83% of net revenue at risk. Auction replay analysis — where the runner-up bid still wins — reduces expected loss to 0.70% of revenue. Retained revenue of 99.2% holds above our 99% guardrail. Fill rate impact is minimal: 57.2% to 56.8%.

Most affected partners: Lumen Bid (82% of calls cut, 79.8% revenue at risk) is an outlier requiring attention. Tidewater Media (60%), Kestrel Retail (49%), and Orchid Native (48%) see heavy call reductions but minimal revenue exposure.

Decision needed: Should we initiate a direct conversation with Lumen Bid given their concentrated revenue risk before we expand the program?
```

## GenAI: partner briefs (`dsp_partner_briefs`)

### Northstar DSP (databricks-claude-sonnet-4-6)

```text
Hi Northstar DSP team,

Starting next cycle, Halyard Exchange will reduce the bid requests we send you by 9.6%, throttling 496 segments concentrated in CTV and native formats, CTV device and mobile app inventory, and US, GB, and IN traffic where your observed bid rate is 12.7% and only 0.71% of your winning spend originates. This means fewer low-signal requests hitting your infrastructure with no meaningful loss to the inventory you actually convert on, reducing your processing overhead while preserving full access to the supply driving your results. Could you confirm these segments align with your current targeting strategy, or flag any specific segments within those 496 you would like us to keep open before we finalize the change?

Thanks,
Halyard Exchange Partner Management
```

### Tidewater Media (databricks-claude-sonnet-4-6)

```text
Hi Tidewater Media team,

Starting next month, Halyard Exchange will reduce the volume of bid requests we send you by 60.3%, concentrating cuts across banner and native formats on mobile_web and mobile_app inventory in the US, AU, and GB, spanning 7,781 segments where your observed bid rate is 8.2%. Because only 0.44% of your winning spend originates from this throttled traffic, the change preserves virtually all inventory you actually buy while meaningfully lowering the request volume hitting your infrastructure. We want to confirm this aligns with your targeting strategy, so please let us know by end of week whether any of the 7,781 segments should be kept active or if you have concerns we should address before rollout.
```

### Quanta Ads (databricks-claude-sonnet-4-6)

```text
Hi Quanta Ads team,

Starting next cycle, Halyard Exchange will stop sending you 14.3% of current bid requests, concentrating reductions across CTV and banner formats, CTV and desktop devices, and US, IN, and GB traffic within 744 segments where your observed bid rate is 21.8% and winning spend in the affected traffic is just 0.18% of your total. This means fewer irrelevant requests hitting your infrastructure while preserving access to essentially all inventory you actually buy. We want to confirm this aligns with your targeting strategy, so please let us know if any of the 744 throttled segments should be kept active or if anything looks off before we finalize the change.

Thanks,
Halyard Exchange Partner Team
```

### Kestrel Retail (databricks-claude-sonnet-4-6)

```text
Hi Kestrel Retail team,

Starting next cycle, Halyard Exchange will reduce the volume of bid requests we send you by 49.0%, concentrating cuts on banner and video formats, mobile web and mobile app inventory, and AU, GB, and IN traffic across 7,532 segments where your current bid rate is 9.8%. This change means fewer low-match requests hitting your infrastructure while preserving access to the inventory that drives your spend, given that only 1.9% of your winning spend sits within the throttled traffic. We want to make sure nothing meaningful falls through, so please confirm the targeting parameters above look correct for your US shopper campaigns, or flag any specific segments within those 7,532 you would like us to keep open. Happy to jump on a call this week.
```

### Ember Exchange (databricks-claude-sonnet-4-6)

```text
Hi team,

Starting [date], Halyard Exchange will stop sending Ember Exchange approximately 1.3% of current bid request volume, concentrating cuts on CTV format, CTV device traffic, and FR inventory within one segment where your observed bid rate is 18.2% and winning spend exposure is 0.54% of total. This means fewer irrelevant requests hitting your infrastructure with no meaningful impact on the inventory you are actually buying. We wanted to flag this proactively and ask that you confirm this aligns with your current EU and LATAM brand campaign targeting, or let us know if you want the FR CTV segment preserved before we move forward.

Thanks,
[Name]
Halyard Exchange
```

### Lumen Bid (databricks-claude-sonnet-4-6)

```text
Hi Lumen Bid team,

Starting next cycle, Halyard Exchange will stop sending you 81.5% of your current bid request volume, concentrating cuts across banner and video formats on mobile_web and mobile_app in the US, AU, and IN, spanning 10,074 segments where your observed bid rate is 0.4%. This reduces infrastructure overhead on your end while preserving access to the inventory that matters, noting that 79.77% of your winning spend currently sits within the throttled traffic, so we want to align on that before proceeding. Could you confirm your targeting priorities across those segments or flag any specific ones you want kept open so we can calibrate accordingly?

Thanks,
Halyard Exchange Partner Management
```

### Orchid Native (databricks-claude-sonnet-4-6)

```text
Hi Orchid Native team,

Starting next cycle, Halyard Exchange will stop sending Orchid Native approximately 47.8% of current bid requests, concentrating cuts on banner and video formats, mobile_web and ctv_device traffic, and AU, US, and GB inventory where your observed bid rate runs at 6.3%. This reduces infrastructure overhead on your end by eliminating low-engagement requests while preserving access to the inventory that actually drives your spend, given that only 2.3% of your winning spend sits within the 5,189 throttled segments. We want to make sure nothing falls through the cracks, so could you confirm these segments align with your current targeting strategy, or flag any specific segments within the throttled pool you would like us to keep open? Happy to adjust before we finalize.
```

### Atlas Programmatic (databricks-claude-sonnet-4-6)

```text
Hi Atlas Programmatic team,

Starting next cycle, Halyard Exchange will stop routing approximately 8.5% of current bid requests to your seat, concentrating reductions across banner and native formats on mobile web and desktop inventory in AU, JP, and IN, spanning 814 segments where your observed bid rate is 12.4%. Because only 0.5% of your winning spend sits within the throttled traffic, you retain access to virtually everything you actually buy while eliminating the infrastructure cost of evaluating requests you consistently pass on. We would appreciate confirmation that your active targeting parameters are unaffected, or if any of the 814 segments should be kept open, please flag those to us by end of week so we can adjust before the change goes live.

Thanks,
Halyard Exchange Partner Management
```

### Vireo Brand (databricks-claude-sonnet-4-6)

```text
Hi Vireo Brand team,

Starting next month, Halyard Exchange will stop sending you 32.9% of current bid requests, concentrating cuts in banner and native formats across mobile web and desktop inventory in AU, GB, and IN, affecting 3,562 segments where your observed bid rate is 5.2%. Because only 1.7% of your winning spend sits within this throttled traffic, you retain meaningful access to the inventory you actually buy while reducing unnecessary request volume and the infrastructure overhead that comes with it on your end. We want to confirm this aligns with your targeting priorities before we proceed, so please let us know if any of the 3,562 segments flagged for throttling are ones you would like kept active.
```

### Sable Asia (databricks-claude-sonnet-4-6)

```text
Hi team,

Starting [date], Halyard Exchange will stop sending Sable Asia approximately 2.8% of current bid request volume, concentrating cuts across CTV format, CTV devices, and traffic from IN, AU, and JP where your observed bid rate is 18.7% and winning spend in the affected segments represents only 0.59% of your total. This means fewer irrelevant requests hitting your infrastructure while preserving access to the inventory you actually buy. The 48 throttled segments were selected precisely because they sit outside your active spend patterns, so your effective reach should remain intact. Could you confirm these segments align with your current targeting, or flag any of the 48 you would like kept in the feed? Happy to share the full segment list before we go live.
```
