# Execution evidence

Everything here is text produced by real runs in the `fevm-cmegdemos` workspace, not hand-written.

| File | What it shows |
|---|---|
| [`job_run_1097322092005970/`](job_run_1097322092005970/README.md) | Final end-to-end run of `halyard-traffic-shaping-e2e`: all 10 tasks SUCCESS, ~13 min. One file per task with every code cell and the output it produced in that run (exported from the Jobs API by `scripts/export_run_evidence.py`), the Lakeflow pipeline event log, and `run.json`. |
| [`e2e_validation.md`](e2e_validation.md) | The 18 cross-layer checks written by `08_validate_e2e` (18/18 passed): raw -> Lakeflow -> UC -> ML -> GenAI -> Lakebase -> Genie -> App. |
| [`key_results.md`](key_results.md) | Result tables queried from Unity Catalog: business case, value assumptions, daily exchange KPIs, ML vs baseline policy curve, per-DSP impact, rule mix, the GenAI CRO summary and all 10 partner briefs. |
| [`app_api_smoke_test.md`](app_api_smoke_test.md) | Live calls against the deployed Databricks App: Lakebase rule reads, edge lookup (~4-5 ms inside the app), an override written to Lakebase that flips the edge decision from THROTTLE to ALLOW, an on-demand LLM rule explanation, and a Genie answer with the SQL it ran. |

## Notes for the reader

- The final run is a re-run on existing data. `generate_bid_logs` reports all 14 days already present, and the Lakeflow update is incremental (Auto Loader finds no new files; materialized views refresh). The 14 days of raw files (336 hourly files, 562,800 auction records incl. injected defects) were generated and ingested by the first run of the same job. Row counts and expectation drop counts for the full ingest are in `e2e_validation.md` (bronze 562,800 -> silver 558,864 auctions; 4,785,525 DSP calls).
- Earlier runs failed and were fixed from their error traces (a `LATERAL VIEW` + `JOIN` parse error, an FM endpoint that does not support `ai_query` batch inference, governed tag policies rejecting tag values, an ANSI array-index error). The README's "How I used AI" section summarizes them.
- Dollar figures are the sample results scaled with `value_assumptions` (see `key_results.md`).
