# End-to-end validation (18/18 checks passed)

Queried from `cmegdemos_catalog.ssp_traffic_shaping.e2e_validation` at 2026-10-22 15:17 UTC. Written by `src/notebooks/08_validate_e2e.py`; the job fails if any check fails.

| layer | check_name | passed | detail | checked_at |
|---|---|---|---|---|
| raw | hourly auction log files landed | true | 336 files | 2026-10-20T18:22:15.742Z |
| lakeflow | latest pipeline update completed | true | d88fa974-f1d1-4680-81fd-b54e1e2b6239 UpdateStateInfoState.COMPLETED | 2026-10-20T18:22:15.742Z |
| lakeflow | bronze -> silver drops bad rows | true | bronze 562,800 -> silver 558,864 auctions (3,936 dropped or replayed) | 2026-10-20T18:22:15.742Z |
| lakeflow | silver_dsp_calls has no replayed duplicates | true | 4,785,525 calls, 0 duplicates | 2026-10-20T18:22:15.742Z |
| lakeflow | expectations recorded in event log | true | has_dsp_calls=0; known_dsp=0; known_publisher=0; valid_auction_id=0; valid_floor=1124; valid_publisher=2258; valid_timestamp=562 | 2026-10-20T18:22:15.742Z |
| lakeflow | gold reconciles to silver | true | gold 4,785,525 == silver 4,785,525 | 2026-10-20T18:22:15.742Z |
| unity_catalog | tables carry layer/domain tags | true | 8 tagged tables | 2026-10-20T18:22:15.742Z |
| unity_catalog | governed partner view resolves for yield_ops | true | 511,889 rows visible to chandler.brooks@databricks.com | 2026-10-20T18:22:15.742Z |
| unity_catalog | app SP has SELECT on throttle_rules | true | 5 grant rows | 2026-10-20T18:22:15.742Z |
| ml | champion model registered in UC | true | bid_propensity v3, holdout AUC 0.907 | 2026-10-20T18:22:15.742Z |
| ml | model beats historical-rate baseline | true | 35.5% vs 30.8% calls cut at 99% revenue retained | 2026-10-20T18:22:15.742Z |
| ml | throttle rules come from the champion model | true | 113,762 rules from v3 | 2026-10-20T18:22:15.742Z |
| ml | revenue guardrail held (>= 99% retained) | true | 99.17% retained, 35.5% calls cut | 2026-10-20T18:22:15.742Z |
| genai | one partner brief per DSP | true | 10 briefs, shortest 675 chars | 2026-10-20T18:22:15.742Z |
| lakebase | serving table matches Delta rules | true | Lakebase 113,762 == Delta 113,762 | 2026-10-20T18:22:15.742Z |
| lakebase | ops override table reachable | true | 0 overrides recorded | 2026-10-20T18:22:15.742Z |
| genie | Genie space over gold + rules tables | true | space 01f1ccb10c7f12b78b235fea709b4bc5, 10 tables | 2026-10-20T18:22:15.742Z |
| app | Databricks App deployed and running | true | https://halyard-traffic-shaping-7474656585748611.aws.databricksapps.com (ApplicationState.RUNNING) | 2026-10-20T18:22:15.742Z |
