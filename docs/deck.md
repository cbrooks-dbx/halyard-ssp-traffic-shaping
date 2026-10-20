# Halyard Exchange: Smarter Traffic Shaping

**Cut a third of outbound bid traffic. Keep 99% of revenue.**

Prepared for: CRO, CFO, VP of Yield, Head of Data Platform

---

## 1. The outcome

| | |
|---|---|
| **$3.3M / year** | infrastructure cost removed (hard savings) |
| **35.5%** | of outbound bid requests no longer sent |
| **99.3%** | of net revenue retained (99.2% worst case, guardrail 99%) |
| **$2.1M / year** | net benefit after expected revenue loss |
| **+$2.7M / year** | upside if freed DSP capacity is re-spent on traffic they buy |

Partner managers get a brief for every DSP and can answer "what are we cutting and why?" in seconds.

---

## 2. The problem: we pay to send requests nobody bids on

- Every auction fans out to ~8.6 DSPs. **About 90% of those requests get no bid.**
- Each request costs compute, egress and connection capacity: **~$9.4M a year** in outbound cost at our volume.
- Waste burns DSP QPS caps, so buyers see junk instead of inventory they would pay for.
- Today, throttling is a few hand-written rules ("don't send banner to Tidewater"). Nobody can say segment by segment what is safe to cut.

**Why now:** outbound cost scales with volume, and every point of fill we protect is revenue. Guessing is no longer good enough.

---

## 3. What we built, in one picture

Bid logs -> **Lakeflow** (ingest, quality checks) -> **Unity Catalog** (governed, one copy) -> **ML** (learns what each DSP buys) -> **throttle rules** -> **Lakebase** (millisecond lookups for the bidder) -> **App + Genie** (partner managers act and ask questions) -> **GenAI** (partner briefs, exec summary)

One platform, one governed copy of the data, one deployment bundle.

---

## 4. How it decides, and why it is safe

- A model predicts, for every traffic segment (DSP x publisher x format x device x country x supply path), how likely the DSP is to bid.
- Segments it is confident about are **throttled to 5%**, borderline ones **reduced to 50%**, the rest flow as today.
- **The cut-off is set by a revenue guardrail Finance owns (99%), not by model accuracy.**
- **5% exploration traffic** keeps flowing to throttled segments, so if a DSP starts buying, the next refresh catches it.
- Partner managers can **override any segment instantly**, with a reason and an audit trail.

---

## 5. Proof: tested on traffic the model never saw

| | ML model | Historical bid rates |
|---|---|---|
| Calls cut at the same revenue guardrail | **35.5%** | 30.8% |
| Holdout AUC | 0.91 | n/a |

- Revenue impact measured two ways: **worst case** (every throttled win lost) 0.8%, **auction replay** (the runner-up bid still wins) 0.7%.
- Fill rate moves from 57.2% to 56.8%.

---

## 6. What it means for each partner

| DSP | Calls cut | Their revenue at risk | Story |
|---|---|---|---|
| Lumen Bid | 81% | high | bids on 0.4% of what we send: commercial conversation needed |
| Tidewater Media | 60% | 0.4% | CTV/video buyer, we were sending banner |
| Kestrel Retail | 49% | 1.9% | US retail buyer, we were sending global traffic |
| Orchid Native | 48% | 2.3% | native buyer, we were sending display |
| Northstar, Atlas, Sable, Ember | 1-10% | < 1% | already well matched |

Each DSP gets a GenAI-drafted brief (reviewed before sending): what changes, why it helps them, one ask.

---

## 7. For the CFO and CRO

- **$3.3M of cost out** of the infrastructure line, from a model you can audit.
- **Revenue risk is capped** by a number Finance sets, and reported weekly in plain English.
- **Every assumption lives in one table** ($ per million calls, volume, take rate). Change it and the business case recomputes.
- **Decision needed:** approve the rollout, and decide how to approach Lumen Bid (keep, renegotiate, or accept the cut).

---

## 8. For the VP of Yield and partner managers

- See every rule for every partner, with predicted and observed bid rates.
- **Override in one click** (e.g. "Tidewater launching CTV on this publisher next week"), effective on the next request.
- Ask **"why is this throttled?"** and get a plain-English explanation.
- Ask **Genie** anything ("How much Lumen traffic are we cutting?", "Which countries waste the most calls?") and get an answer with the SQL behind it.

---

## 9. For the data platform team

- **One bundle** deploys pipeline, jobs, Lakebase, Genie and app. Serverless throughout.
- **Governance in Unity Catalog:** confidential floor prices banded for partner managers, row-level DSP isolation, least-privilege app identity, lineage end to end.
- **Lakebase** holds rules for the bidder edge (~4 ms lookup) and overrides as a transactional audit trail.
- **Quality you can see:** 5 data-quality expectations, replay deduplication, 18 automated end-to-end checks on every run.

---

## 10. Rollout plan

| Weeks | Step | Exit criterion |
|---|---|---|
| 1-2 | Shadow mode: rules computed, not enforced; compare to live auctions | replay revenue loss < 1% on live data |
| 3-4 | Enforce for 2 low-risk DSPs (Tidewater, Orchid) after partner briefs | fill rate within 0.5 pt |
| 5-8 | All DSPs except Lumen; weekly CRO summary | $ savings tracking to plan |
| 9+ | Lumen decision; feed overrides back into training | |

**Ask:** sponsor for weeks 1-4, one yield analyst, one platform engineer.
