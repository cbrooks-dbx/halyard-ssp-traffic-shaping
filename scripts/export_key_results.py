"""Dump the result tables of the latest run to markdown (evidence/key_results.md, evidence/e2e_validation.md).

    python scripts/export_key_results.py [--profile fevm-cmegdemos] [--warehouse 9cd919d96b11bf1c]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path

FQ = "cmegdemos_catalog.ssp_traffic_shaping"


def sql(stmt: str, profile: str, warehouse: str) -> tuple[list[str], list[list]]:
    body = {"warehouse_id": warehouse, "wait_timeout": "50s", "statement": stmt}
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "-p", profile, "--json", json.dumps(body)],
                         capture_output=True, text=True, check=True).stdout
    d = json.loads(out)
    if d["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(d["status"])
    cols = [c["name"] for c in d["manifest"]["schema"]["columns"]]
    return cols, (d.get("result") or {}).get("data_array", [])


def table(cols, rows) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join("" if v is None else str(v).replace("\n", " ").replace("|", "/") for v in r) + " |" for r in rows]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="fevm-cmegdemos")
    ap.add_argument("--warehouse", default="9cd919d96b11bf1c")
    ap.add_argument("--out", default="evidence")
    a = ap.parse_args()
    q = lambda s: sql(s, a.profile, a.warehouse)
    out = Path(a.out)
    out.mkdir(exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    cols, rows = q(f"SELECT layer, check_name, passed, detail, checked_at FROM {FQ}.e2e_validation")
    passed = sum(1 for r in rows if str(r[2]).lower() == "true")
    n_checks = len(rows)
    (out / "e2e_validation.md").write_text(
        f"# End-to-end validation ({passed}/{len(rows)} checks passed)\n\nQueried from `{FQ}.e2e_validation` at {now}. "
        "Written by `src/notebooks/08_validate_e2e.py`; the job fails if any check fails.\n\n" + table(cols, rows) + "\n")

    sections = [f"# Key results\n\nQueried live from Unity Catalog at {now}. Every number in the README and deck traces to a row below.\n"]
    sections.append("## Business case (`throttle_impact_summary`)\n\n" + table(*q(
        f"SELECT metric, CASE WHEN unit = 'pct' THEN concat(format_number(value * 100, 2), '%') "
        f"WHEN unit LIKE 'USD%' THEN concat('$', format_number(value, 0)) ELSE format_number(value, 0) END AS value, unit, definition, model_version "
        f"FROM {FQ}.throttle_impact_summary")))
    sections.append("## Value assumptions (`value_assumptions`)\n\n" + table(*q(f"SELECT * FROM {FQ}.value_assumptions")))
    sections.append("## Exchange KPIs by day (`gold_exchange_daily`)\n\n" + table(*q(f"SELECT * FROM {FQ}.gold_exchange_daily ORDER BY event_date")))
    sections.append("## Policy curve at the guardrail: ML vs historical rate (`threshold_curve`)\n\n" + table(*q(
        f"SELECT method, round(threshold, 4) AS threshold, round(calls_cut_pct * 100, 2) AS calls_cut_pct, "
        f"round(revenue_at_risk_pct * 100, 3) AS revenue_at_risk_pct, segments_shaped FROM {FQ}.threshold_curve "
        f"WHERE round(threshold, 4) IN (0.002, 0.004, 0.0058, 0.008, 0.01, 0.015, 0.02, 0.03, 0.05) ORDER BY method, threshold")))
    sections.append("## Per-DSP impact (`dsp_throttle_impact`)\n\n" + table(*q(
        f"SELECT dsp_name, calls_7d, round(observed_bid_rate * 100, 2) AS bid_rate_pct, round(calls_cut_pct * 100, 1) AS calls_cut_pct, "
        f"round(revenue_at_risk_pct * 100, 2) AS revenue_at_risk_pct, segments_throttled, top_throttled_formats, top_throttled_devices, top_throttled_countries "
        f"FROM {FQ}.dsp_throttle_impact ORDER BY calls_cut_pct DESC")))
    sections.append("## Rule mix (`throttle_rules`)\n\n" + table(*q(
        f"SELECT action, send_rate, count(*) AS rules, sum(calls_7d) AS calls_7d, round(avg(predicted_bid_rate), 4) AS avg_pred_bid_rate, "
        f"round(sum(calls_7d * observed_bid_rate) / sum(calls_7d), 4) AS observed_bid_rate, max(model_version) AS model_version "
        f"FROM {FQ}.throttle_rules GROUP BY action, send_rate ORDER BY send_rate")))
    cols, rows = q(f"SELECT summary, model_endpoint, generated_at FROM {FQ}.exec_weekly_summary")
    sections.append(f"## GenAI: weekly CRO summary (`exec_weekly_summary`, {rows[0][1]}, {rows[0][2]})\n\n```text\n{rows[0][0]}\n```")
    cols, rows = q(f"SELECT dsp_name, partner_brief, model_endpoint FROM {FQ}.dsp_partner_briefs ORDER BY dsp_id")
    sections.append("## GenAI: partner briefs (`dsp_partner_briefs`)\n\n" + "\n\n".join(f"### {r[0]} ({r[2]})\n\n```text\n{r[1]}\n```" for r in rows))
    (out / "key_results.md").write_text("\n\n".join(sections) + "\n")
    print(f"Wrote {out / 'key_results.md'} and {out / 'e2e_validation.md'} ({passed}/{n_checks} checks)")


if __name__ == "__main__":
    main()
