# Halyard Exchange: cut a third of SSP bid traffic, keep 99% of revenue

**Industry:** Ad tech / programmatic advertising (supply-side platform)
**Customer (fictional, synthetic data):** Halyard Exchange, a mid-size SSP clearing ~$1.2B a year in gross media spend
**Buyers:** CRO and CFO (executive sponsors), VP of Yield / Head of Partner Ops (domain owner), Head of Data Platform (technical owner)

## The business problem

Every ad auction Halyard runs is fanned out to up to 10 DSP buyers, about 8.6 outbound bid requests per auction. **Nine out of ten of those requests get no bid.** Halyard still pays to build, serialize, send and wait on every one of them: compute, egress and the connection fleet to each DSP, about **$9.4M a year** at Halyard's ~25B auctions a day. The waste also burns DSP QPS caps, so buyers see junk traffic instead of inventory they would pay for.

Partner managers know some of this intuitively ("Tidewater only buys video"), but nobody can say, per segment, which traffic a DSP will never buy without risking revenue.

## The outcome

A traffic-shaping system that learns each DSP's buying behaviour from bid logs and stops sending requests a DSP will not bid on, with a hard revenue guardrail.

| KPI (buyer's language) | Result | Source |
|---|---|---|
| Outbound bid requests not sent | **35.5%** | `throttle_impact_summary.outbound_calls_cut_pct` |
| Hard infrastructure savings | **$3.3M / year** | calls not sent x $0.12 per million calls |
| Net revenue retained | **99.3%** expected (auction replay), **99.2%** worst case | guardrail >= 99% |
| Fill rate | 57.2% -> 56.8% | counterfactual auction replay |
| Annual net benefit | **$2.1M** expected ($1.8M worst case) | savings minus revenue loss |
| Upside (scenario, not booked) | +$2.7M / year | freed DSP QPS re-spent on traffic they buy |
| Time for a partner manager to answer "how much are we cutting from DSP X and why?" | seconds, in Genie or the app | was a ticket to the data team |

For the **CFO**: $3.3M of cost comes out of the infrastructure line, with every assumption in one editable table (`value_assumptions`).
For the **CRO**: revenue risk is capped by a guardrail Finance owns, and each DSP gets a ready-to-review partner brief.
For the **VP of Yield**: rules per segment, one-click overrides with an audit trail, and plain-English explanations of every rule.

All numbers are from the run committed in [`evidence/`](evidence/), scaled to Halyard volume with the assumptions in `00_setup.py`. Scaling assumptions, not model output, drive the dollar figures. Change a row in `value_assumptions` and every KPI recomputes.

## The integrated data journey

```
Bidder fleet logs (hourly JSON.gz, synthetic)
   |  Lakeflow Spark Declarative Pipelines: Auto Loader -> bronze -> silver (expectations, dedup) -> gold
   v
Unity Catalog: governed tags, access-policy table, governed partner view (row filter + floor masking),
               least-privilege grants for the app SP, lineage, model registry
   |
   +--> ML: bid-propensity model (scikit-learn HGB, MLflow, UC registry @champion)
   |        -> throttle_rules (one per segment: THROTTLE 5% / REDUCE 50% / ALLOW)
   |        -> counterfactual auction replay -> throttle_impact_summary ($)
   |
   +--> GenAI: ai_query drafts a partner brief per DSP + a weekly CRO summary (grounded in the impact tables)
   |
   +--> Lakebase (Postgres): throttle_rules synced as a serving table + native rule_overrides table
   |        -> bidder edge / app do a primary-key lookup (~4 ms) with override-then-rule
   |
   +--> Genie space: NL questions over gold, rules, impact and briefs
   |
   v
Databricks App "Halyard Traffic Shaping": KPIs, partner briefs, rules + overrides (writes Lakebase),
                                         on-demand LLM rule explanations, embedded Genie, edge lookup
```

Each layer reads the previous layer's output, nothing is mocked, and `08_validate_e2e` asserts every hand-off (see the evidence).

| Stage | What it does here | Code |
|---|---|---|
| Raw data | 14 days x 40k auctions/day of synthetic OpenRTB-style auction logs (562,800 auctions, 4.79M DSP calls), with injected defects | `src/halyard_sim.py`, `src/notebooks/01_generate_bid_logs.py` |
| **Lakeflow** | Auto Loader ingest, 5 expectations on silver, replay dedup, call-level silver, 3 gold MVs (serverless) | `src/pipeline/bid_log_pipeline.sql` |
| **Unity Catalog** | Governed tags, access-policy table, governed partner view, grants, lineage, model registry | `src/notebooks/02_governance.py` |
| **ML** | Bid-propensity model, guardrail-tuned threshold, segment rules, auction replay | `03_train_bid_propensity.py`, `04_score_throttle_rules.py` |
| **GenAI** | Partner briefs + CRO summary via `ai_query`; on-demand rule explanations in the app via FMAPI | `05_genai_partner_briefs.py`, `app/main.py` |
| **Lakebase** | Synced serving table, native override table, Postgres grants, lookup benchmark | `06_lakebase_serving.py` |
| **Genie** | Space with instructions, sample questions and example SQL, created via API; smoke-tested | `07_genie_space.py` |
| **Databricks App** | FastAPI + JS console, reads UC + Lakebase, writes Lakebase, calls Genie and FMAPI | `app/` |
| Validation | 18 end-to-end checks, fails the job if any break | `08_validate_e2e.py` |

Everything deploys as one **Declarative Automation Bundle** (`databricks.yml`, `resources/`): pipeline, 10-task job, Lakebase project, app.

## Evidence the build ran

The evaluator reads text, so the run outputs are committed as text:

- [`evidence/job_run_<id>/`](evidence/): one markdown file per task with every code cell **and the output it produced** in that run (exported from the Jobs API), plus the pipeline event log, `run.json` and a task summary.
- [`evidence/e2e_validation.md`](evidence/e2e_validation.md): the 18 checks and their results.
- [`evidence/app_api_smoke_test.md`](evidence/app_api_smoke_test.md): live calls against the deployed app (Lakebase reads, override write, edge lookup, LLM explanation, Genie answer with its SQL).
- [`evidence/key_results.md`](evidence/key_results.md): model metrics, policy curve, per-DSP impact, a sample GenAI brief, the CRO summary.

## Why these choices

The short version is below. The full reasoning, with the alternatives considered, is in [`docs/decisions_and_tradeoffs.md`](docs/decisions_and_tradeoffs.md).

- **Segment rules in Lakebase, not a model call per request.** ~200B outbound calls a day inside a ~150 ms auction timeout: a key lookup fits, an inference call does not.
- **A model, not last week's bid rates.** At the same 99% revenue guardrail the model cuts 35.5% of calls vs 30.8% for a (smoothed) historical-rate rule, because it generalizes across sparse segments and new publishers.
- **Threshold set by a business guardrail, not AUC.** Finance owns the 99% number; tuning keeps 10% headroom so it holds on a new week of traffic.
- **Exploration built in.** Throttled segments still get 5% of traffic, so the next retrain notices if a DSP starts buying.
- **Two revenue numbers, honestly labelled.** A worst-case bound (every throttled win lost) and an auction replay (runner-up bids still clear).
- **Governance in UC, once.** Floors are contractually confidential and partner managers see only their DSP, enforced for SQL, Genie and the app alike.

## Running it

```bash
databricks bundle deploy -p <profile>
databricks bundle run traffic_shaping_e2e -p <profile>      # ~25 min end to end, serverless
databricks bundle run halyard_app -p <profile>
python scripts/export_run_evidence.py <job_run_id>          # regenerate evidence/
```

Defaults target `cmegdemos_catalog.ssp_traffic_shaping` and warehouse `9cd919d96b11bf1c`. Override with `--var catalog=... --var warehouse_id=...`.

## How I used AI

I built this with Claude Code (Opus) as a pair programmer, and treated it as a fast junior engineer whose output I steered and verified rather than trusted:

- **What I decided:** the customer problem and KPIs, the segment-rule-in-Lakebase architecture, the guardrail-first threshold, the exploration policy, the two-number revenue framing, and which governance controls matter for an SSP.
- **What AI accelerated:** scaffolding the bundle, notebooks, pipeline SQL and app; writing the simulator's hidden DSP behaviour; and debugging against real run output. Each failure (a `LATERAL VIEW` join the SQL parser rejected, an endpoint that does not support batch inference, governed tag policies rejecting tag values, an ANSI array-index error) was diagnosed from the job's actual error trace and fixed, not guessed around.
- **Where I overrode it:** the first model run showed a 0% baseline and a guardrail breach (98.5% retained). Rather than ship the flattering number, I made the baseline fair (smoothing sparse segments), made the policy curve model the actual three-tier policy, and added the counterfactual replay.
- **Patterns:** "verify against the platform before relying on it" (probing which FM endpoints support `ai_query` batch, reading the account's governed tag policies), and an end-to-end validation notebook so the AI-written code has to prove itself on every run.

## Repository layout

```
databricks.yml, resources/      Declarative Automation Bundle (pipeline, job, Lakebase project, app)
src/halyard_sim.py              synthetic auction-log simulator (hidden DSP behaviour = ground truth)
src/pipeline/                   Lakeflow declarative pipeline (SQL)
src/notebooks/00..08            setup, generate, governance, train, score, GenAI, Lakebase, Genie, validate
app/                            Databricks App (FastAPI + static JS)
scripts/export_run_evidence.py  turns a job run into text evidence
evidence/                       execution evidence (text)
docs/                           decisions and trade-offs, deck
```

All data is synthetic. No customer data, names or customer-identifying content is used.
