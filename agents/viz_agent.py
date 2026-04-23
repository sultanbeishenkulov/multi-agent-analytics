"""
Viz Agent — Decides the best chart type and builds a Plotly figure from SQL results.

Flow:
  1. Send question + column names + sample rows to Claude
  2. Claude returns a chart spec (type, x/y columns, title, labels)
  3. Build and return a Plotly figure object using that spec
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from agents.llm import chat

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "viz_agent.md"

_CHART_BUILDERS = {}


def _chart(chart_type: str):
    def decorator(fn):
        _CHART_BUILDERS[chart_type] = fn
        return fn
    return decorator


def _load_system_prompt() -> str:
    return PROMPT_PATH.read_text()


def _build_user_message(question: str, results: list[dict]) -> str:
    df = pd.DataFrame(results)
    columns_info = {col: str(df[col].dtype) for col in df.columns}
    sample = results[:5]
    return (
        f"User question: {question}\n\n"
        f"Columns and types: {json.dumps(columns_info)}\n\n"
        f"Sample rows (first 5 of {len(results)}):\n"
        f"{json.dumps(sample, indent=2, default=str)}\n\n"
        "Choose the best chart type and return the spec. "
        "Respond ONLY with valid JSON, no markdown fences."
    )


def _get_chart_spec(question: str, results: list[dict]) -> dict:
    content = chat(
        system=_load_system_prompt(),
        user=_build_user_message(question, results),
    )
    if content.startswith("```"):
        content = content.split("\n", 1)[1].rsplit("```", 1)[0]
    return json.loads(content)


@_chart("bar")
def _bar(df: pd.DataFrame, spec: dict) -> go.Figure:
    return px.bar(
        df,
        x=spec["x_column"],
        y=spec["y_column"],
        color=spec.get("color_column") or None,
        title=spec["title"],
        labels={spec["x_column"]: spec["x_label"], spec["y_column"]: spec["y_label"]},
    )


@_chart("horizontal_bar")
def _horizontal_bar(df: pd.DataFrame, spec: dict) -> go.Figure:
    return px.bar(
        df,
        x=spec["y_column"],
        y=spec["x_column"],
        color=spec.get("color_column") or None,
        orientation="h",
        title=spec["title"],
        labels={spec["x_column"]: spec["x_label"], spec["y_column"]: spec["y_label"]},
    )


@_chart("line")
def _line(df: pd.DataFrame, spec: dict) -> go.Figure:
    return px.line(
        df,
        x=spec["x_column"],
        y=spec["y_column"],
        color=spec.get("color_column") or None,
        title=spec["title"],
        labels={spec["x_column"]: spec["x_label"], spec["y_column"]: spec["y_label"]},
        markers=True,
    )


@_chart("pie")
def _pie(df: pd.DataFrame, spec: dict) -> go.Figure:
    return px.pie(
        df,
        names=spec["x_column"],
        values=spec["y_column"],
        title=spec["title"],
    )


@_chart("scatter")
def _scatter(df: pd.DataFrame, spec: dict) -> go.Figure:
    return px.scatter(
        df,
        x=spec["x_column"],
        y=spec["y_column"],
        color=spec.get("color_column") or None,
        title=spec["title"],
        labels={spec["x_column"]: spec["x_label"], spec["y_column"]: spec["y_label"]},
    )


@_chart("heatmap")
def _heatmap(df: pd.DataFrame, spec: dict) -> go.Figure:
    pivot = df.pivot_table(
        index=spec["y_column"],
        columns=spec["x_column"],
        values=spec["color_column"],
        aggfunc="sum",
    )
    return px.imshow(
        pivot,
        title=spec["title"],
        labels={"color": spec["color_column"]},
        aspect="auto",
    )


def _build_figure(spec: dict, results: list[dict]) -> go.Figure | None:
    chart_type = spec.get("chart_type")
    if not chart_type:
        return None

    builder = _CHART_BUILDERS.get(chart_type)
    if not builder:
        return None

    df = pd.DataFrame(results)
    # Parse date-like columns so Plotly renders them correctly on time axes
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_datetime(df[col])
            except (ValueError, TypeError):
                pass

    return builder(df, spec)


def run_viz_agent(
    question: str,
    results: list[dict],
) -> dict:
    """Build a Plotly figure for query results.

    Args:
        question: The user's original natural language question.
        results: The query results as a list of dicts.

    Returns a dict with:
        - figure: A Plotly Figure object (or None if not applicable)
        - chart_type: The chosen chart type string (or None)
        - spec: The raw chart spec from Claude
        - error: Error message if something went wrong
    """
    if not results:
        return {"figure": None, "chart_type": None, "spec": None, "error": None}

    try:
        spec = _get_chart_spec(question, results)

        if not spec.get("chart_type"):
            return {"figure": None, "chart_type": None, "spec": spec, "error": None}

        figure = _build_figure(spec, results)
        return {
            "figure": figure,
            "chart_type": spec["chart_type"],
            "spec": spec,
            "error": None,
        }

    except json.JSONDecodeError as e:
        return {"figure": None, "chart_type": None, "spec": None, "error": f"Failed to parse viz spec: {e}"}
    except Exception as e:
        return {"figure": None, "chart_type": None, "spec": None, "error": f"Viz agent error: {e}"}


if __name__ == "__main__":
    import sys
    from agents.sql_agent import run_sql_agent

    if len(sys.argv) < 2:
        print('Usage: python -m agents.viz_agent "your question"')
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"\nQuestion: {question}\n")

    sql_result = run_sql_agent(question)
    if sql_result["error"]:
        print(f"SQL Agent error: {sql_result['error']}")
        sys.exit(1)

    print(f"SQL:\n{sql_result['sql']}\n")

    viz_result = run_viz_agent(question, sql_result["results"])

    if viz_result["error"]:
        print(f"Viz Agent error: {viz_result['error']}")
    elif viz_result["figure"] is None:
        print("No chart applicable for this result.")
    else:
        print(f"Chart type: {viz_result['chart_type']}")
        print(f"Spec: {json.dumps(viz_result['spec'], indent=2)}")
        viz_result["figure"].show()
