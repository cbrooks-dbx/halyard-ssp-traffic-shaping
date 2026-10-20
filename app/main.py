"""Halyard Exchange traffic-shaping console (Databricks App).

Reads:   KPIs, partner impact, GenAI briefs  -> Unity Catalog gold tables via the SQL warehouse
         throttle rules + ops overrides       -> Lakebase Postgres (operational, millisecond reads)
Writes:  ops overrides                        -> Lakebase (transactional, append-only audit trail)
Asks:    natural-language questions           -> Genie space
         "explain this rule"                  -> Foundation Model API endpoint
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

CATALOG = os.environ.get("CATALOG", "cmegdemos_catalog")
SCHEMA = os.environ.get("SCHEMA", "ssp_traffic_shaping")
FQ = f"{CATALOG}.{SCHEMA}"
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5-5")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "projects/halyard-traffic-shaping/branches/production/endpoints/primary")

w = WorkspaceClient()
app = FastAPI(title="Halyard Traffic Shaping")

# --------------------------------------------------------------------------- SQL warehouse (Unity Catalog)


def sql(statement: str) -> list[dict]:
    res = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s")
    if res.status.state.value != "SUCCEEDED":
        raise HTTPException(502, f"SQL failed: {res.status.error.message if res.status.error else res.status.state}")
    cols = [c.name for c in res.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (res.result.data_array or [])] if res.result else []


_genie_space_id = os.environ.get("GENIE_SPACE_ID") or None


def genie_space_id() -> str:
    global _genie_space_id
    if not _genie_space_id:
        rows = sql(f"SELECT value FROM {FQ}.app_config WHERE key = 'genie_space_id'")
        _genie_space_id = rows[0]["value"] if rows else None
    if not _genie_space_id:
        raise HTTPException(503, "Genie space not configured yet")
    return _genie_space_id

# --------------------------------------------------------------------------- Lakebase (Postgres)


class Lakebase:
    """One long-lived connection, re-opened when the OAuth token nears expiry or the connection drops."""

    def __init__(self):
        self._conn = None
        self._opened = 0.0
        self._lock = threading.Lock()

    def _connect(self):
        host = os.environ.get("PGHOST") or w.api_client.do("GET", f"/api/2.0/postgres/{LAKEBASE_ENDPOINT}")["status"]["hosts"]["host"]
        token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": LAKEBASE_ENDPOINT})["token"]
        user = os.environ.get("PGUSER") or w.current_user.me().user_name
        self._conn = psycopg.connect(host=host, dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
                                     user=user, password=token, sslmode="require", autocommit=True)
        self._opened = time.time()

    def query(self, statement: str, params=None, fetch=True):
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._conn is None or self._conn.closed or time.time() - self._opened > 45 * 60:
                        self._connect()
                    cur = self._conn.execute(statement, params)
                    if not fetch:
                        return None
                    cols = [d.name for d in cur.description]
                    return [dict(zip(cols, r)) for r in cur.fetchall()]
                except psycopg.OperationalError:
                    self._conn = None
                    if attempt == 2:
                        raise


pg = Lakebase()

# Override-then-rule. The bidder edge runs the same query.
EFFECTIVE_SQL = f"""
SELECT r.rule_id, r.dsp_id, r.dsp_name, r.publisher_id, r.publisher_name, r.vertical, r.ad_format, r.device_type,
       r.geo_country, r.supply_path, r.user_id_present, r.calls_7d, r.bids_7d, r.net_revenue_7d,
       r.predicted_bid_rate, r.observed_bid_rate, r.est_daily_calls_saved, r.est_daily_revenue_at_risk_usd,
       r.action AS model_action, r.send_rate AS model_send_rate,
       COALESCE(o.action, r.action) AS effective_action, COALESCE(o.send_rate, r.send_rate) AS effective_send_rate,
       o.reason AS override_reason, o.created_by AS override_by, o.created_at AS override_at
FROM "{SCHEMA}"."throttle_rules_serving" r
LEFT JOIN LATERAL (
  SELECT action, send_rate, reason, created_by, created_at FROM halyard_ops.rule_overrides x
  WHERE x.rule_id = r.rule_id AND (x.expires_at IS NULL OR x.expires_at > now())
  ORDER BY x.created_at DESC LIMIT 1) o ON true
"""

# --------------------------------------------------------------------------- API


@app.get("/api/health")
def health():
    return {"ok": True, "lakebase_rules": pg.query(f'SELECT count(*) AS n FROM "{SCHEMA}"."throttle_rules_serving"')[0]["n"]}


@app.get("/api/kpis")
def kpis():
    summary = {r["metric"]: float(r["value"]) for r in sql(f"SELECT metric, value FROM {FQ}.throttle_impact_summary")}
    trend = sql(f"SELECT event_date, outbound_calls, calls_per_auction, bid_rate, fill_rate, net_revenue_usd FROM {FQ}.gold_exchange_daily ORDER BY event_date")
    curve = sql(f"SELECT method, threshold, calls_cut_pct, revenue_at_risk_pct FROM {FQ}.threshold_curve WHERE threshold <= 0.12 ORDER BY method, threshold")
    exec_summary = sql(f"SELECT summary, generated_at FROM {FQ}.exec_weekly_summary")
    return {"summary": summary, "trend": trend, "curve": curve, "exec_summary": exec_summary[0] if exec_summary else None}


@app.get("/api/partners")
def partners():
    return sql(f"""
      SELECT i.dsp_id, i.dsp_name, d.specialty, d.contracted_qps_cap, i.calls_7d, i.observed_bid_rate, i.calls_cut_pct,
             i.revenue_at_risk_pct, i.net_revenue_7d, i.segments_throttled, b.partner_brief
      FROM {FQ}.dsp_throttle_impact i JOIN {FQ}.dim_dsp_partners d USING (dsp_id)
      LEFT JOIN {FQ}.dsp_partner_briefs b USING (dsp_id)
      ORDER BY i.calls_cut_pct DESC""")


@app.get("/api/rules")
def rules(dsp_id: str | None = None, action: str | None = None, limit: int = 100):
    where, params = [], []
    if dsp_id:
        where.append("dsp_id = %s"); params.append(dsp_id)
    if action:
        where.append("effective_action = %s"); params.append(action)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(min(limit, 500))
    return pg.query(f"SELECT * FROM ({EFFECTIVE_SQL}) e {clause} ORDER BY est_daily_calls_saved DESC LIMIT %s", params)


@app.get("/api/edge/decision")
def edge_decision(rule_id: str):
    t0 = time.perf_counter()
    rows = pg.query(f"SELECT effective_action, effective_send_rate FROM ({EFFECTIVE_SQL}) e WHERE rule_id = %s", (rule_id,))
    ms = (time.perf_counter() - t0) * 1000
    if not rows:
        return {"rule_id": rule_id, "action": "ALLOW", "send_rate": 1.0, "source": "default (no rule)", "lookup_ms": round(ms, 2)}
    return {"rule_id": rule_id, "action": rows[0]["effective_action"], "send_rate": float(rows[0]["effective_send_rate"]),
            "source": "lakebase", "lookup_ms": round(ms, 2)}


class OverrideIn(BaseModel):
    rule_id: str
    action: str = Field(pattern="^(ALLOW|REDUCE|THROTTLE)$")
    reason: str = Field(min_length=5, max_length=500)
    days: int = Field(default=14, ge=1, le=90)


@app.post("/api/overrides")
def create_override(body: OverrideIn, request: Request):
    user = request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or "unknown"
    send_rate = {"ALLOW": 1.0, "REDUCE": 0.5, "THROTTLE": 0.05}[body.action]
    if not pg.query(f'SELECT 1 FROM "{SCHEMA}"."throttle_rules_serving" WHERE rule_id = %s', (body.rule_id,)):
        raise HTTPException(404, "Unknown rule_id")
    row = pg.query("""INSERT INTO halyard_ops.rule_overrides (rule_id, action, send_rate, reason, created_by, expires_at)
                      VALUES (%s, %s, %s, %s, %s, now() + make_interval(days => %s))
                      RETURNING override_id, rule_id, action, send_rate, reason, created_by, created_at, expires_at""",
                   (body.rule_id, body.action, send_rate, body.reason, user, body.days))
    return row[0]


@app.get("/api/overrides")
def list_overrides(limit: int = 50):
    return pg.query("SELECT * FROM halyard_ops.rule_overrides ORDER BY created_at DESC LIMIT %s", (min(limit, 200),))


class ExplainIn(BaseModel):
    rule_id: str


@app.post("/api/explain")
def explain(body: ExplainIn):
    rows = pg.query(f"SELECT * FROM ({EFFECTIVE_SQL}) e WHERE rule_id = %s", (body.rule_id,))
    if not rows:
        raise HTTPException(404, "Unknown rule_id")
    r = rows[0]
    facts = (f"DSP {r['dsp_name']}, publisher {r['publisher_name']} ({r['vertical']}), {r['ad_format']} on {r['device_type']}, "
             f"country {r['geo_country']}, supply path {r['supply_path']}, user ID present {r['user_id_present']}. "
             f"Last 7 days: {r['calls_7d']} calls, {r['bids_7d']} bids (observed bid rate {float(r['observed_bid_rate']):.2%}), "
             f"model predicted bid rate {float(r['predicted_bid_rate']):.2%}, net revenue ${float(r['net_revenue_7d']):.2f}. "
             f"Model action {r['model_action']} at send rate {float(r['model_send_rate']):.0%}; effective action {r['effective_action']}"
             + (f" (override by {r['override_by']}: {r['override_reason']})" if r['override_reason'] else "") + ".")
    resp = w.serving_endpoints.query(name=LLM_ENDPOINT, max_tokens=300, messages=[
        ChatMessage(role=ChatMessageRole.SYSTEM, content="You explain SSP traffic-shaping rules to a partner manager in 3 short sentences: what the rule does, why the data supports it, and what would justify overriding it. Use only the facts given. Plain text."),
        ChatMessage(role=ChatMessageRole.USER, content=facts)])
    return {"rule_id": body.rule_id, "explanation": resp.choices[0].message.content, "facts": facts, "model": LLM_ENDPOINT}


class GenieIn(BaseModel):
    question: str = Field(min_length=3, max_length=500)
    conversation_id: str | None = None


@app.post("/api/genie")
def genie(body: GenieIn):
    sid = genie_space_id()
    if body.conversation_id:
        msg = w.api_client.do("POST", f"/api/2.0/genie/spaces/{sid}/conversations/{body.conversation_id}/messages", body={"content": body.question})
        conv, mid = body.conversation_id, msg.get("message_id") or msg.get("id")
    else:
        msg = w.api_client.do("POST", f"/api/2.0/genie/spaces/{sid}/start-conversation", body={"content": body.question})
        conv, mid = msg["conversation_id"], msg["message_id"]
    for _ in range(80):
        m = w.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}/conversations/{conv}/messages/{mid}")
        if m["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    out = {"conversation_id": conv, "status": m["status"], "text": None, "sql": None, "description": None, "columns": [], "rows": []}
    for a in m.get("attachments", []):
        if a.get("text"):
            out["text"] = a["text"]["content"]
        if a.get("query"):
            out["sql"] = a["query"].get("query")
            out["description"] = a["query"].get("description")
            res = w.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}/conversations/{conv}/messages/{mid}/attachments/{a['attachment_id']}/query-result")
            sr = res.get("statement_response", {})
            out["columns"] = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            out["rows"] = (sr.get("result", {}) or {}).get("data_array", [])[:50]
    return out


STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")
