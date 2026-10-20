# Decisions and trade-offs

Each choice below was made for Halyard's specific problem (cutting outbound bid-request waste without touching revenue), not as a default.

## 1. Segment-level rules served from Lakebase, not per-request model scoring at the edge

**Chose:** score offline, publish one rule per traffic segment (DSP x publisher x format x device x country x supply path x user-ID present), and have the bidder edge do a primary-key lookup in Lakebase.

**Instead of:** calling a Model Serving endpoint for every outbound bid request.

**Why:** Halyard fans out on the order of 200B outbound calls a day. A model call per request would add network latency inside a ~150 ms auction timeout and would cost more than the waste it removes. Segment rules change slowly (bidding patterns move over days, not seconds), so a nightly refresh captures almost all of the value, and a key lookup is a single-digit-millisecond operation (benchmarked in `06_lakebase_serving`).

**Trade-off accepted:** a rule can lag a sudden change in DSP behaviour by up to a day. Two mitigations are built in: 5% exploration traffic keeps flowing to throttled segments, so the next retrain sees any change, and partner managers can override a segment immediately from the app.

## 2. A model, not just historical bid rates

**Chose:** a gradient-boosted bid-propensity model whose segment average drives the rule.

**Instead of:** throttling on each segment's observed bid rate.

**Why:** the segment space is long-tailed. Most segments see only a handful of calls a week, so their observed rate is noise, and new publishers or new segments have no history. The model learns a DSP's preferences once ("Tidewater only buys video and CTV") and applies them across every publisher. `03_train_bid_propensity` measures this on an out-of-time holdout: at the same 99% revenue guardrail, the model removes more outbound calls than the historical-rate rule.

**Trade-off accepted:** less transparent than a lookup of last week's rate. That is why the app has an on-demand GenAI "explain this rule" that cites the predicted and observed rates side by side.

## 3. The threshold is set by a business guardrail, not by model accuracy

**Chose:** the largest score cut-off that keeps at least 99% of net revenue on the holdout. The guardrail is a row in `value_assumptions`, so Finance owns it.

**Why:** AUC does not tell the CRO anything. "We cut X% of calls and put at most 1% of revenue at risk" does. The revenue-at-risk number is deliberately conservative: it assumes every throttled win is lost, although in practice the runner-up bid usually fills the impression.

## 4. Three actions with exploration, not a binary block

THROTTLE sends 5%, REDUCE sends 50%, ALLOW sends 100%. A hard block would starve the model of the data it needs to notice when a DSP starts buying a segment, so the system would never un-throttle on its own. The 5% costs a little savings and buys self-correction.

## 5. Lakeflow Spark Declarative Pipelines with Auto Loader on a volume

**Chose:** SQL-defined pipeline, Auto Loader over hourly JSON files, expectations on silver, materialized views for gold, serverless.

**Instead of:** hand-written Structured Streaming jobs, or Zerobus or Kafka ingest.

**Why:** Halyard's bidder fleet already rotates hourly log files to object storage; Auto Loader picks up just the new ones with no extra infrastructure. Expectations make data quality visible (counts of dropped rows land in the event log and are checked in `08_validate_e2e`). Replayed auctions are deduplicated in the silver materialized view rather than with stateful streaming dedup, which keeps the pipeline simple at this volume.

**What changes in production:** at real scale (billions of auctions a day) bronze would come from a streaming source and gold would be partitioned and incrementally refreshed. The pipeline code does not change shape.

## 6. Snapshot sync to Lakebase, plus a native Postgres table for overrides

Rules are fully recomputed each run, so a snapshot sync is simpler and cheaper than continuous CDC. Overrides originate in the business, not the lakehouse, so they live natively in Postgres (transactional, append-only, `INSERT`-only grant for the app). The edge applies override-then-rule with one query. That query is kept in code rather than as a Postgres view because snapshot syncs recreate the serving table, and a dependent view would block that.

**Deployment note:** the workspace had hit its quota for provisioned Lakebase instances, so the build uses Lakebase Autoscaling (project, branch, endpoint), which also scales to zero between runs.

## 7. Governance in Unity Catalog, not in the app

Floor prices are contractually confidential and partner managers may only see their own DSP. That rule lives in UC (an access-policy table plus a governed view that row-filters by DSP and bands floors), so it holds for SQL, Genie, notebooks and the app alike. The app's service principal gets `SELECT` on gold tables only. It cannot read bronze or call-level silver data.

## 8. GenAI where the work is language, grounded in governed numbers

Partner briefs and the CRO summary are writing tasks, so they use an LLM (`ai_query` on Foundation Model APIs). The prompt passes only computed figures and forbids inventing numbers; output lands in UC tables that Genie and the app both read, with the generating endpoint recorded per row. A human reviews before anything goes to a partner.

## 9. Synthetic data, with hidden ground truth

DSP buying behaviour exists only inside `src/halyard_sim.py`. The model never sees it and has to recover it from logs, which keeps the ML evaluation honest. Data quality defects (null publisher IDs, negative floors, bad timestamps, replayed auctions) are injected on purpose so the expectations have real work to do.
