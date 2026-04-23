"""
Orchestrator — LangGraph pipeline that chains Context → SQL → Interpreter → Viz.

State flows through four nodes:
  1. context_node  — retrieves RAG docs + optional web search results
  2. sql_node      — translates the question into SQL and executes it
  3. interpret_node — turns results into human-readable insights
  4. viz_node      — builds a Plotly figure from the results
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import StateGraph, END

from agents.sql_agent import run_sql_agent, fix_sql_for_viz
from agents.interpreter_agent import run_interpreter_agent
from agents.viz_agent import run_viz_agent
from agents.rag_agent import run_rag_agent
from agents.search_agent import run_search_agent
from agents.memory import SessionMemory


# ── State schema ─────────────────────────────────────────────

class AnalyticsState(TypedDict, total=False):
    # Input
    question: str
    conversation_history: list[dict]

    # Context (RAG + web search)
    rag_context: str | None
    search_context: str | None

    # SQL agent output
    sql: str | None
    sql_explanation: str | None
    tables_used: list[str]
    sql_confidence: float | None
    results: list[dict] | None
    sql_error: str | None

    # Interpreter agent output
    summary: str | None
    key_findings: list[str]
    follow_up_suggestions: list[str]
    interpret_error: str | None

    # Viz agent output
    figure: object | None
    chart_type: str | None
    chart_spec: dict | None
    viz_error: str | None


# ── Nodes ─────────────────────────────────────────────────────

def context_node(state: AnalyticsState) -> AnalyticsState:
    """Retrieve RAG context and optionally web search results."""
    question = state["question"]
    rag_context = run_rag_agent(question)
    search_context = run_search_agent(question)
    return {"rag_context": rag_context, "search_context": search_context}


def sql_node(state: AnalyticsState) -> AnalyticsState:
    extra_context = "\n\n".join(
        c for c in [state.get("rag_context"), state.get("search_context")] if c
    )
    result = run_sql_agent(
        question=state["question"],
        conversation_history=state.get("conversation_history"),
        extra_context=extra_context,
    )
    sql = fix_sql_for_viz(result["sql"]) if result["sql"] else None
    return {
        "sql": sql,
        "sql_explanation": result["explanation"],
        "tables_used": result["tables_used"],
        "sql_confidence": result["confidence"],
        "results": result["results"],
        "sql_error": result["error"],
    }


def interpret_node(state: AnalyticsState) -> AnalyticsState:
    result = run_interpreter_agent(
        question=state["question"],
        sql=state["sql"],
        results=state["results"],
    )
    return {
        "summary": result["summary"],
        "key_findings": result["key_findings"],
        "follow_up_suggestions": result["follow_up_suggestions"],
        "interpret_error": result["error"],
    }


def viz_node(state: AnalyticsState) -> AnalyticsState:
    result = run_viz_agent(
        question=state["question"],
        results=state["results"],
    )
    return {
        "figure": result["figure"],
        "chart_type": result["chart_type"],
        "chart_spec": result["spec"],
        "viz_error": result["error"],
    }


# ── Routing ───────────────────────────────────────────────────

def should_continue(state: AnalyticsState) -> str:
    if state.get("sql_error"):
        return END
    return "interpret"


# ── Build the graph ───────────────────────────────────────────

def build_graph():
    graph = StateGraph(AnalyticsState)

    graph.add_node("context", context_node)
    graph.add_node("sql", sql_node)
    graph.add_node("interpret", interpret_node)
    graph.add_node("viz", viz_node)

    graph.set_entry_point("context")
    graph.add_edge("context", "sql")
    graph.add_conditional_edges("sql", should_continue, {"interpret": "interpret", END: END})
    graph.add_edge("interpret", "viz")
    graph.add_edge("viz", END)

    return graph.compile()


analytics_graph = build_graph()

_memory = SessionMemory()


def run_pipeline(
    question: str,
    session_id: str | None = None,
    conversation_history: list[dict] | None = None,
) -> AnalyticsState:
    """Run the full analytics pipeline for a question."""
    if conversation_history is None and session_id:
        conversation_history = _memory.load(session_id)

    state = analytics_graph.invoke({
        "question": question,
        "conversation_history": conversation_history or [],
    })

    if session_id and not state.get("sql_error"):
        _memory.save_turn(
            session_id=session_id,
            question=question,
            sql=state.get("sql"),
            summary=state.get("summary"),
        )

    return state


def stream_pipeline(
    question: str,
    session_id: str | None = None,
    conversation_history: list[dict] | None = None,
):
    """Stream pipeline events as each node completes. Yields dicts."""
    if conversation_history is None and session_id:
        conversation_history = _memory.load(session_id)

    final_state = {}
    for chunk in analytics_graph.stream(
        {"question": question, "conversation_history": conversation_history or []},
        stream_mode="updates",
    ):
        node_name = list(chunk.keys())[0]
        node_data = {k: v for k, v in chunk[node_name].items() if k != "figure"}
        yield {"node": node_name, "data": node_data}
        final_state.update(chunk[node_name])

    if session_id and not final_state.get("sql_error"):
        _memory.save_turn(
            session_id=session_id,
            question=question,
            sql=final_state.get("sql"),
            summary=final_state.get("summary"),
        )


# ── CLI entry point ───────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="*", help="Natural language question")
    parser.add_argument("--session", "-s", default="default", help="Session ID for memory")
    parser.add_argument("--sessions", action="store_true", help="List all sessions")
    parser.add_argument("--clear", metavar="SESSION_ID", help="Clear a session's history")
    args = parser.parse_args()

    mem = SessionMemory()

    if args.sessions:
        sessions = mem.list_sessions()
        if not sessions:
            print("No sessions found.")
        for s in sessions:
            print(f"  {s['session_id']:20s}  {s['turns']} turns  last active: {s['last_active'][:19]}")
        sys.exit(0)

    if args.clear:
        mem.delete_session(args.clear)
        print(f"Cleared session '{args.clear}'.")
        sys.exit(0)

    if not args.question:
        parser.print_help()
        sys.exit(1)

    question = " ".join(args.question)
    print(f"\nQuestion: {question}  [session: {args.session}]\n{'─' * 60}")

    state = run_pipeline(question, session_id=args.session)

    if state.get("sql_error"):
        print(f"SQL Error: {state['sql_error']}")
        sys.exit(1)

    print(f"SQL:\n{state['sql']}\n")
    print(f"Confidence: {state['sql_confidence']:.0%}")
    print(f"Tables: {', '.join(state['tables_used'])}\n")

    if state.get("interpret_error"):
        print(f"Interpreter Error: {state['interpret_error']}")
    else:
        print(f"Summary: {state['summary']}\n")
        for finding in state.get("key_findings", []):
            print(f"  • {finding}")
        suggestions = state.get("follow_up_suggestions", [])
        if suggestions:
            print("\nFollow-ups:")
            for s in suggestions:
                print(f"  → {s}")

    if state.get("viz_error"):
        print(f"\nViz Error: {state['viz_error']}")
    elif state.get("figure"):
        print(f"\nChart: {state['chart_type']}")
        state["figure"].show()
    else:
        print("\nNo chart for this result.")
