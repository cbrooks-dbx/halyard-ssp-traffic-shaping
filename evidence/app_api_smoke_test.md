# App API smoke test against https://halyard-traffic-shaping-7474656585748611.aws.databricksapps.com
# run at 2026-10-22 11:17:44 EDT

GET /api/health -> 200 in 2312 ms
{"ok": true, "lakebase_rules": 113762}

GET /api/kpis -> 200 in 3943 ms
  outbound_calls_cut_pct                   0.3545
  net_revenue_at_risk_pct                  0.0083
  revenue_retained_pct                     0.9917
  replay_net_revenue_loss_pct              0.0070
  replay_fill_rate_before                  0.5716
  replay_fill_rate_after                   0.5679
  segments_throttled                       36,241.0000
  segments_reduced                         13,657.0000
  segments_total                           113,762.0000
  annual_outbound_calls                    78,527,142,857,142.8438
  annual_outbound_cost_usd                 9,423,257.1429
  annual_infra_savings_usd                 3,340,903.9958
  annual_revenue_at_risk_usd               1,502,866.4095
  annual_expected_revenue_loss_usd         1,258,747.3125
  annual_qps_reallocation_upside_usd       2,700,000.0000
  annual_net_benefit_usd                   2,082,156.6833
  annual_net_benefit_worst_case_usd        1,838,037.5864
  trend days: 14, curve points: 458
  exec summary: Traffic Shaping Program Update — Week of [Date]

Annual net benefit is $2.1M (worst case $1.8M), driven by $3.3M in hard infrastructure savings against an expected revenue loss of $1.3M. The $2.7M QPS reallocation upside is excluded from both figures.

Revenue safety is strong. Conservative modeling assumes every throttled win is permanently lost, putting 0.83% of net revenue at risk. Auction replay analysis — where the runner-up bid still wins — reduces expected loss to 0.70% of revenue. Retained revenue of 99.2% holds above our 99% guardrail. Fill rate impact is minimal: 57.2% to 56.8%.

Mos

GET /api/partners -> 200 in 2318 ms
  Lumen Bid            calls_7d   278043  bid_rate 0.004  cut 0.815  brief: Hi Lumen Bid team,

Starting next cycle, Halyard Exchange will stop sending you 81.5% of y...
  Tidewater Media      calls_7d   278043  bid_rate 0.082  cut 0.603  brief: Hi Tidewater Media team,

Starting next month, Halyard Exchange will reduce the volume of ...
  Kestrel Retail       calls_7d   278043  bid_rate 0.098  cut 0.490  brief: Hi Kestrel Retail team,

Starting next cycle, Halyard Exchange will reduce the volume of b...
  Orchid Native        calls_7d   278043  bid_rate 0.063  cut 0.478  brief: Hi Orchid Native team,

Starting next cycle, Halyard Exchange will stop sending Orchid Nat...
  Vireo Brand          calls_7d   278043  bid_rate 0.052  cut 0.329  brief: Hi Vireo Brand team,

Starting next month, Halyard Exchange will stop sending you 32.9% of...
  Quanta Ads           calls_7d   278043  bid_rate 0.218  cut 0.143  brief: Hi Quanta Ads team,

Starting next cycle, Halyard Exchange will stop sending you 14.3% of ...
  Northstar DSP        calls_7d   278043  bid_rate 0.127  cut 0.096  brief: Hi Northstar DSP team,

Starting next cycle, Halyard Exchange will reduce the bid requests...
  Atlas Programmatic   calls_7d   278043  bid_rate 0.124  cut 0.085  brief: Hi Atlas Programmatic team,

Starting next cycle, Halyard Exchange will stop routing appro...
  Sable Asia           calls_7d    75551  bid_rate 0.187  cut 0.028  brief: Hi team,

Starting [date], Halyard Exchange will stop sending Sable Asia approximately 2.8...
  Ember Exchange       calls_7d    92851  bid_rate 0.182  cut 0.013  brief: Hi team,

Starting [date], Halyard Exchange will stop sending Ember Exchange approximately...

GET /api/rules?dsp_id=DSP02&action=THROTTLE&limit=5 -> 200 in 262 ms (served from Lakebase)
  THROTTLE Tidewater Media | Harbor Daily | banner/mobile_web | AU | pred 0.0008 | saves 376 calls/day
  THROTTLE Tidewater Media | Harbor Daily | banner/mobile_web | AU | pred 0.0011 | saves 315 calls/day
  THROTTLE Tidewater Media | Harbor Daily | banner/desktop | AU | pred 0.0008 | saves 260 calls/day
  THROTTLE Tidewater Media | Summit Sports Net | banner/mobile_web | US | pred 0.0020 | saves 258 calls/day
  THROTTLE Tidewater Media | Meadow Daily | banner/mobile_app | US | pred 0.0026 | saves 228 calls/day

GET /api/edge/decision -> 200, round trip 225 ms
{"rule_id": "DSP02|PUB001|banner|mobile_web|AU|reseller_1hop|False", "action": "THROTTLE", "send_rate": 0.05, "source": "lakebase", "lookup_ms": 4.12}

POST /api/overrides -> 200 in 236 ms
{"override_id": 1, "rule_id": "DSP02|PUB001|banner|mobile_web|AU|reseller_1hop|False", "action": "ALLOW", "send_rate": 1.0, "reason": "Tidewater confirmed a CTV-adjacent banner test on this publisher next week (smoke test)", "created_by": "chandler.brooks@databricks.com", "created_at": "2026-10-22T15:17:54.321423+00:00", "expires_at": "2026-10-29T15:17:54.321423+00:00"}
GET /api/edge/decision after override -> 200
{"rule_id": "DSP02|PUB001|banner|mobile_web|AU|reseller_1hop|False", "action": "ALLOW", "send_rate": 1.0, "source": "lakebase", "lookup_ms": 5.35}

POST /api/explain -> 200 in 3565 ms (model databricks-claude-sonnet-5-5)
[{'type': 'reasoning', 'summary': [{'type': 'summary_text', 'text': '', 'signature': 'CAQSpwoKEggTEAEYAjgBQgh0aGlua2luZxIM1Wk3vYTFwA8lyATxGgw5GZlTC3/V+201anYiMOWWEpEBeXB2/Lujwi5YtpDTFFvfUx4Js/zjFP/UHMJaemtQbnOzv5Cq3RdkJe2LXSrCCXrY66c0SghMxhgux6Ktotu1jO4S8yhy+AK0mQbBTnOqxPUzkWcArRbrY9up/+vrm4iQVvjEicPqVdJotkAoVO6YydT+TEQLyriNw6nKdB/xVs0CAqI/XzZKidsNe4eaSTRGAwO8ygST5z9FciHKKkHV1FoYCPv7bNmiauzzJXZj4rqPR6REW3H03aLgQ9giA3zU5IfYglAj/xhcsDE3XfcAohPYknlR3+dte12lkOhrOwLZTKVVtbvXIGhxBoio0lAEyMEjc1oYQCc5rU81DcaBwA5LRGTb3i8pLTWn44Rt7rW+L3Vzlkto7GpeLrFsWQIF3EjJZfr53Hwq8ErxOTEuoOpnuO5umjBlr9UpBBuiKK4QeNg9sdKQ2VaneJKR8WU1VU/2MSW4FmDw3B5UNUeH/Czw014MD4ey97tyiiR+oNn9BygMMCpk7EojXquO2jFjzntBe+5hVzhDLVAN/TJ94iZDs00yDrp0AXUk3QpVsz3bkOuzAJUyY82A8hlDKVOGtVCxNV11RTnmPs2RQZBuC1f6klHKrm8pK6CK+mYYAU4nigOCo4J5uHBi+0RnDFvjW2sEP8n/6+A6dewpFtdsQ00pVOs+BxwZEdal3MVh2wKV5ke8QUOj+zjSIkJ/k4C9O14qzHpu6fjzxRzz1VjETJK94oUDslsJKGKKiHycnWGApGw2lEHZKfdYRbFdbYG0qT7CkwRiB44go+Tj+/CLiKQt/TTUICriYGISuuNk38zRm6fkJgsIG/mXUGleZrVz+hvJ0/CX1Uq5e4k0Lz/ZzRcc6WvGixX6vBuf++NXbBpMl8rKaegYccyiGaPkNPvj1zLjlxaJCZ03d2y/Irc7meXE2r1ncB18OVUrzkPBDAQwwSwi+EuXFSs77bHWPIgKIShaSYDRLDmWGig2UShM3DlMco+TBFHV70gwadgFHIDF+TTQr2bsW2vJQn0tuMaVY+t+JJI7iMJThFWNNq7jiGe0T+LMLVZDzCeQGtLuhz07HTmCcVmooGdHC1KHbFOav57aL+YS80y2N8JAymbWv78UtxQ7GMDr4GtsY/Zgnsx5kM6fTR02eWGYaTGWsNyx7y9O+HpRNU4dA+JsbChUGlC2n82pi3MY2EJisevQAWuSgFxE3N6bnwS0a4tHS2UzmIZ91LkbjxE4M+k19YszQcmdzJ2bdtBOdJJdnJ6YWzQinvaDlAymlPj3X5QGPLynjD5ks/BgkTmvsodvf0R0kKnRMkZOEvRysVk+P3RntiZonhDupe7N0l2UOBeG6I/f1u1C7uGFZJ/MW2eR+JEtlUqYj/2MXPlurz2RlmdF0vVmg1Wr1DX00z0DMD/ONpPfjqGnsMVD3LD6fAMad6mevbrUt7VEeLiqAqHdOwZcxXwpEXVGAr+YpGa7CP50HNcqowiN5y0cz4SW1ORYtKvOgYChts4IMl5ee6YOTPf+CPJfwIEF7yfeWFF92vjXLs2IerR+yMsNIhg4pHrZOSPnU1lZWUy/MSkpHmHgn50Lv+QynWVZfzi404O+Xng6eaIB1A7K3drgptKL+s+Yq8tb71/sHf+XCj3jJZqnfdvfgJDktedWGPADYZhbJ1OEb3m0kodzI/h3JJ5MuV6zjh6q7BgB'}]}]

POST /api/genie -> 200 in 19000 ms
  status COMPLETED
  SQL: WITH dsp_bid_rates AS (
  SELECT
    `dsp_name`,
    SUM(`bids`) AS `bids`,
    SUM(`outbound_calls`) AS `outbound_calls`,
    try_divide(SUM(`bids`),SUM(`outbound_calls`)) AS `bid_rate`
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
  columns ['dsp_name', 'bid_rate', 'outbound_calls']
  rows [['Lumen Bid', '0.004375206801801154', '556088'], ['Vireo Brand', '0.05148285882809915', '556088'], ['Orchid Native', '0.06348815295420869', '556088']]
  text Would you prefer to see the DSPs with the lowest bid rate for a specific recent time period instead of all available data?
