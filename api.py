"""
FastAPI layer — exposes the analytics pipeline as an HTTP API.

Endpoints:
  POST /query        — run the full pipeline (SQL + interpret + viz)
  POST /query/sql    — SQL agent only
  GET  /health       — liveness check
"""

from __future__ import annotations

import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from agents.orchestrator import run_pipeline, stream_pipeline
from agents.sql_agent import run_sql_agent

app = FastAPI(title="Multi-Agent Analytics API", version="0.1.0")


# ── Request / Response models ─────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    session_id: str | None = None
    conversation_history: list[dict] | None = None


class SQLResult(BaseModel):
    sql: str | None
    explanation: str | None
    tables_used: list[str]
    confidence: float | None
    results: list[dict] | None
    error: str | None


class AnalyticsResult(BaseModel):
    question: str

    # SQL
    sql: str | None
    sql_explanation: str | None
    tables_used: list[str]
    sql_confidence: float | None
    results: list[dict] | None
    sql_error: str | None

    # Interpretation
    summary: str | None
    key_findings: list[str]
    follow_up_suggestions: list[str]
    interpret_error: str | None

    # Viz
    chart_type: str | None
    chart_spec: dict | None
    viz_error: str | None
    # Note: the Plotly figure is returned as JSON so the frontend
    # can render it with plotly.js
    figure_json: dict | None


# ── Endpoints ─────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query", response_model=AnalyticsResult)
def query(req: QueryRequest):
    """Run the full pipeline: SQL → Interpreter → Viz."""
    state = run_pipeline(
        question=req.question,
        session_id=req.session_id,
        conversation_history=req.conversation_history,
    )

    if state.get("sql_error"):
        raise HTTPException(status_code=422, detail=state["sql_error"])

    figure_json = None
    if state.get("figure") is not None:
        figure_json = json.loads(state["figure"].to_json())

    return AnalyticsResult(
        question=req.question,
        sql=state.get("sql"),
        sql_explanation=state.get("sql_explanation"),
        tables_used=state.get("tables_used", []),
        sql_confidence=state.get("sql_confidence"),
        results=state.get("results"),
        sql_error=state.get("sql_error"),
        summary=state.get("summary"),
        key_findings=state.get("key_findings", []),
        follow_up_suggestions=state.get("follow_up_suggestions", []),
        interpret_error=state.get("interpret_error"),
        chart_type=state.get("chart_type"),
        chart_spec=state.get("chart_spec"),
        viz_error=state.get("viz_error"),
        figure_json=figure_json,
    )


@app.post("/query/stream")
def query_stream(req: QueryRequest):
    """Stream pipeline events via Server-Sent Events as each agent completes."""
    def event_generator():
        for event in stream_pipeline(
            question=req.question,
            session_id=req.session_id,
            conversation_history=req.conversation_history,
        ):
            yield f"data: {json.dumps(event, default=str)}\n\n"
        yield 'data: {"node": "done"}\n\n'

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/query/sql", response_model=SQLResult)
def query_sql(req: QueryRequest):
    """Run the SQL agent only — skips interpretation and viz."""
    result = run_sql_agent(
        question=req.question,
        conversation_history=req.conversation_history,
    )

    if result["error"]:
        raise HTTPException(status_code=422, detail=result["error"])

    return SQLResult(
        sql=result["sql"],
        explanation=result["explanation"],
        tables_used=result["tables_used"],
        confidence=result["confidence"],
        results=result["results"],
        error=result["error"],
    )
