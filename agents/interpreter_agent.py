"""
Interpreter Agent — Translates SQL query results into human-readable insights.

Flow:
  1. Receive the user's original question, the SQL that ran, and the results
  2. Load the system prompt from prompts/interpreter_agent.md
  3. Send to Claude → get back JSON with summary, key_findings, follow_up_suggestions
  4. Return structured interpretation
"""

from __future__ import annotations

import json
from pathlib import Path

from agents.llm import chat

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "interpreter_agent.md"


def _load_system_prompt() -> str:
    return PROMPT_PATH.read_text()


def _build_user_message(
    question: str,
    sql: str,
    results: list[dict],
    max_rows: int = 50,
) -> str:
    truncated = results[:max_rows]
    truncation_note = (
        f"\n(Showing first {max_rows} of {len(results)} rows)"
        if len(results) > max_rows
        else ""
    )
    return (
        f"User question: {question}\n\n"
        f"SQL that was executed:\n{sql}\n\n"
        f"Query results ({len(results)} rows total){truncation_note}:\n"
        f"{json.dumps(truncated, indent=2, default=str)}\n\n"
        "Interpret these results. Respond ONLY with valid JSON, no markdown fences."
    )


def run_interpreter_agent(
    question: str,
    sql: str,
    results: list[dict],
) -> dict:
    """Interpret SQL query results as human-readable insights.

    Args:
        question: The user's original natural language question.
        sql: The SQL query that was executed.
        results: The query results as a list of dicts.

    Returns a dict with:
        - summary: Headline answer to the question
        - key_findings: List of specific data points
        - follow_up_suggestions: Suggested next questions
        - error: Error message if something went wrong
    """
    if not results:
        return {
            "summary": "The query returned no results.",
            "key_findings": [],
            "follow_up_suggestions": [],
            "error": None,
        }

    try:
        content = chat(
            system=_load_system_prompt(),
            user=_build_user_message(question, sql, results),
        )

        if content.startswith("```"):
            content = content.split("\n", 1)[1]
            content = content.rsplit("```", 1)[0]

        parsed = json.loads(content)

        return {
            "summary": parsed.get("summary", ""),
            "key_findings": parsed.get("key_findings", []),
            "follow_up_suggestions": parsed.get("follow_up_suggestions", []),
            "error": None,
        }

    except json.JSONDecodeError as e:
        return {
            "summary": None,
            "key_findings": [],
            "follow_up_suggestions": [],
            "error": f"Failed to parse interpreter response: {e}",
        }
    except Exception as e:
        return {
            "summary": None,
            "key_findings": [],
            "follow_up_suggestions": [],
            "error": f"Interpreter error: {e}",
        }


if __name__ == "__main__":
    import sys
    from agents.sql_agent import run_sql_agent

    if len(sys.argv) < 2:
        print('Usage: python -m agents.interpreter_agent "your question"')
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"\nQuestion: {question}\n")

    sql_result = run_sql_agent(question)

    if sql_result["error"]:
        print(f"SQL Agent error: {sql_result['error']}")
        sys.exit(1)

    print(f"SQL:\n{sql_result['sql']}\n")

    interpretation = run_interpreter_agent(
        question=question,
        sql=sql_result["sql"],
        results=sql_result["results"],
    )

    if interpretation["error"]:
        print(f"Interpreter error: {interpretation['error']}")
    else:
        print(f"Summary: {interpretation['summary']}\n")
        if interpretation["key_findings"]:
            print("Key findings:")
            for finding in interpretation["key_findings"]:
                print(f"  • {finding}")
        if interpretation["follow_up_suggestions"]:
            print("\nFollow-up suggestions:")
            for suggestion in interpretation["follow_up_suggestions"]:
                print(f"  → {suggestion}")
