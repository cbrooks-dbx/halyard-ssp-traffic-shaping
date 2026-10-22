"""Export a finished job run as plain-text execution evidence.

For every notebook task, pulls the run's rendered notebook (code + the outputs it actually produced) through the
Jobs export API, decodes the embedded notebook model, and writes one markdown file per task. Also writes the run
summary (task states, durations) as JSON and markdown.

    python scripts/export_run_evidence.py <job_run_id> [--profile fevm-cmegdemos] [--out evidence]
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import urllib.parse
from pathlib import Path

from databricks.sdk import WorkspaceClient


def decode_notebook_model(html: str) -> dict:
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    if not m:
        raise ValueError("notebook model not found in export")
    return json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode("utf-8")))


def render_result(res: dict | None) -> str:
    if not res:
        return ""
    kind, data = res.get("type"), res.get("data")
    if isinstance(data, list) and data and all(isinstance(x, dict) and "type" in x for x in data):
        parts = []  # multi-part cell output (stdout, stderr, display); keep the readable text, drop widget/progress noise
        for x in data:
            if x.get("type") in ("ansi", "text") and isinstance(x.get("data"), str):
                parts.append(x["data"])
            elif x.get("type") == "mimeBundle" and isinstance(x.get("data"), dict):
                t = x["data"].get("text/plain", "")
                if t and not t.startswith(("Uploading artifacts", "Downloading artifacts", "DataFrame[")):
                    parts.append(t)
            elif x.get("type") == "table":
                parts.append(render_result(x))
        return "\n".join(p.rstrip("\n") for p in parts)
    if kind in ("text", "ansi") or isinstance(data, str):
        return str(data or "")
    if kind == "table" and isinstance(data, list):
        cols = [c.get("name") for c in res.get("schema", [])]
        lines = [" | ".join(cols), " | ".join("---" for _ in cols)]
        lines += [" | ".join(str(v) for v in row) for row in data[:50]]
        return "\n".join(lines)
    if kind == "error":
        return f"ERROR: {res.get('summary') or ''}\n{res.get('cause') or ''}"
    return json.dumps(data, default=str)[:4000]


def notebook_markdown(task_key: str, task_run: dict, model: dict) -> str:
    start = dt.datetime.fromtimestamp(task_run["start_time"] / 1000, dt.timezone.utc)
    secs = (task_run.get("end_time", 0) - task_run["start_time"]) / 1000
    out = [f"# Task `{task_key}`", "",
           f"- task run id: {task_run['run_id']}",
           f"- notebook: `{task_run['notebook_task']['notebook_path'].split('/files/')[-1]}`",
           f"- started: {start:%Y-%m-%d %H:%M:%S} UTC, duration {secs:.0f}s",
           f"- result: **{task_run['state'].get('result_state')}**", "",
           "Code cells and the output each one produced in this run, exported from the Jobs API.", ""]
    for i, cmd in enumerate(model.get("commands", []), 1):
        src = cmd.get("command", "")
        if src.lstrip().startswith("%md"):
            out += [src.replace("%md", "", 1).strip(), ""]
            continue
        out += [f"### Cell {i}", "```python", src.strip(), "```"]
        text = render_result(cmd.get("results")).strip()
        if cmd.get("error"):
            text = (text + "\n" + str(cmd.get("error"))).strip()
        if text:
            out += ["Output:", "```text", text[:20000], "```"]
        out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id", type=int)
    ap.add_argument("--profile", default="fevm-cmegdemos")
    ap.add_argument("--out", default="evidence")
    args = ap.parse_args()

    w = WorkspaceClient(profile=args.profile)
    run = w.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": args.run_id})
    out = Path(args.out) / f"job_run_{args.run_id}"
    out.mkdir(parents=True, exist_ok=True)

    summary = []
    for t in sorted(run["tasks"], key=lambda t: t.get("start_time", 0)):
        secs = (t.get("end_time", 0) - t.get("start_time", 0)) / 1000
        summary.append((t["task_key"], t["state"].get("result_state"), round(secs), t["run_id"]))
        if "notebook_task" in t:
            exp = w.api_client.do("GET", "/api/2.1/jobs/runs/export", query={"run_id": t["run_id"], "views_to_export": "CODE"})
            model = decode_notebook_model(exp["views"][0]["content"])
            (out / f"{t['task_key']}.md").write_text(notebook_markdown(t["task_key"], t, model))
        elif "pipeline_task" in t:
            pid = t["pipeline_task"]["pipeline_id"]
            events = w.api_client.do("GET", f"/api/2.0/pipelines/{pid}/events", query={"max_results": 100, "order_by": "timestamp desc"})
            lines = [f"# Task `{t['task_key']}` (Lakeflow pipeline {pid})", "", f"- result: **{t['state'].get('result_state')}**, {secs:.0f}s", "",
                     "Most recent pipeline events (newest first):", "", "```text"]
            for e in events.get("events", []):
                lines.append(f"{e['timestamp']}  {e.get('level', ''):<5} {e.get('event_type', ''):<22} {e.get('message', '')[:220]}")
            lines.append("```")
            (out / f"{t['task_key']}.md").write_text("\n".join(lines))

    started = dt.datetime.fromtimestamp(run["start_time"] / 1000, dt.timezone.utc)
    md = [f"# Job run {args.run_id}: {run['run_name']}", "",
          f"- started {started:%Y-%m-%d %H:%M} UTC, overall result **{run['state'].get('result_state')}**",
          f"- run page: {run['run_page_url']}", "", "| task | result | seconds | task run id |", "|---|---|---|---|"]
    md += [f"| {k} | {r} | {s} | {i} |" for k, r, s, i in summary]
    (out / "README.md").write_text("\n".join(md) + "\n")
    (out / "run.json").write_text(json.dumps({k: run[k] for k in ("run_id", "run_name", "state", "start_time", "end_time", "tasks")}, indent=2, default=str))
    print(f"Wrote evidence for {len(summary)} tasks to {out}")


if __name__ == "__main__":
    main()
