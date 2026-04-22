"""
SQL Agent — Translates natural language questions into PostgreSQL queries.

This is the core agent. It takes a user's question, combines it with
the database schema, sends it to Claude, and returns validated SQL
with results.

Flow:
  1. Load the system prompt from prompts/sql_agent.md
  2. Get the schema description from db.py
  3. Build a message list (system prompt + schema + conversation history + question)
  4. Send to Claude → get back JSON with SQL
  5. Validate the SQL using EXPLAIN
  6. If valid, execute and return results
  7. If invalid, retry up to 2 times
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from agents.db import get_schema_description, validate_sql, execute_query
from agents.llm import chat


def _fix_sql(sql: str) -> str:
    """Post-process generated SQL to fix common model mistakes."""
    # Cast DATE_TRUNC to DATE to avoid timestamp/timezone rendering issues
    sql = re.sub(
        r"DATE_TRUNC\(('(?:month|week|year|day|quarter)')\s*,\s*([^)]+)\)(?!::)",
        r"DATE_TRUNC(\1, \2)::DATE",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def fix_sql_for_viz(sql: str) -> str:
    """Additional fixes applied only in pipeline/viz context (not benchmark).

    Replaces LIMIT 1 with LIMIT 100 when the query groups data, so charts
    show all groups rather than just the top row.
    """
    if re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
        sql = re.sub(r"\bLIMIT\s+1\b", "LIMIT 100", sql, flags=re.IGNORECASE)
    return sql

# ── Load the system prompt ───────────────────────────────────
# We keep the prompt in a separate .md file so you can edit it
# without touching Python code. This is a best practice for
# prompt engineering — version control your prompts separately.

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "sql_agent.md"


def _load_system_prompt(extra_context: str = "") -> str:
    """Read the system prompt and append the live schema and any extra context."""
    base_prompt = PROMPT_PATH.read_text()
    schema = get_schema_description()
    parts = [base_prompt, schema]
    if extra_context:
        parts.append(extra_context)
    return "\n\n".join(parts)


# ── Build the messages for Claude ────────────────────────────

def _build_messages(
    question: str,
    conversation_history: list[dict] | None = None,
) -> list[dict]:
    """Build the message list that gets sent to Claude.

    If there's conversation history (for follow-up questions),
    we include the last few exchanges so Claude has context.
    """
    messages = []

    # Add conversation history for follow-up context
    # We only keep the last 6 messages (3 exchanges) to stay
    # within token limits while preserving enough context.
    if conversation_history:
        for turn in conversation_history[-6:]:
            messages.append({
                "role": turn["role"],
                "content": turn["content"],
            })

    # Add the current question
    messages.append({
        "role": "user",
        "content": (
            f"User question: {question}\n\n"
            "Generate the SQL query. Respond ONLY with valid JSON, "
            "no markdown fences or extra text."
        ),
    })

    return messages


# ── The main function ────────────────────────────────────────

def run_sql_agent(
    question: str,
    conversation_history: list[dict] | None = None,
    max_retries: int = 2,
    extra_context: str = "",
    provider: str | None = None,
) -> dict:
    """Run the SQL agent on a natural language question.

    Args:
        question: The user's question in plain English.
        conversation_history: Prior turns for follow-up context.
        max_retries: How many times to retry if SQL validation fails.

    Returns a dict with:
        - sql: The generated SQL query
        - explanation: Why this SQL was generated
        - tables_used: Which tables the query touches
        - confidence: How confident the agent is (0-1)
        - results: The query results as a list of dicts
        - error: Error message if something went wrong
    """
    system_prompt = _load_system_prompt(extra_context)
    base_user_msg = _build_messages(question, conversation_history)[-1]["content"]
    user_msg = base_user_msg
    content = ""

    for attempt in range(max_retries + 1):
        try:
            # ── Step 1: Ask the model to generate SQL ────────
            content = chat(system=system_prompt, user=user_msg, provider=provider)

            if content.startswith("```"):
                content = content.split("\n", 1)[1]
                content = content.rsplit("```", 1)[0]

            parsed = json.loads(content)

            sql = _fix_sql(parsed["sql"])
            explanation = parsed.get("explanation", "")
            tables_used = parsed.get("tables_used", [])
            confidence = parsed.get("confidence", 0.5)

            # ── Step 2: Validate with EXPLAIN ────────────────
            is_valid, error_msg = validate_sql(sql)

            if not is_valid:
                if attempt < max_retries:
                    user_msg = (
                        f"{base_user_msg}\n\nPrevious attempt produced invalid SQL:\n{content}\n"
                        f"Error: {error_msg}\nPlease fix the query. Respond ONLY with valid JSON."
                    )
                    continue
                else:
                    return {
                        "sql": sql,
                        "explanation": explanation,
                        "tables_used": tables_used,
                        "confidence": confidence,
                        "results": None,
                        "error": f"SQL invalid after {max_retries + 1} attempts: {error_msg}",
                    }

            # ── Step 3: Execute the query ────────────────────
            results = execute_query(sql)

            return {
                "sql": sql,
                "explanation": explanation,
                "tables_used": tables_used,
                "confidence": confidence,
                "results": results,
                "error": None,
            }

        except json.JSONDecodeError as e:
            if attempt < max_retries:
                user_msg = (
                    f"{base_user_msg}\n\nPrevious attempt was not valid JSON:\n{content}\n"
                    "Please respond ONLY with a JSON object, no markdown fences or extra text."
                )
                continue
            return {
                "sql": None,
                "explanation": None,
                "tables_used": [],
                "confidence": 0,
                "results": None,
                "error": f"Failed to parse response as JSON: {e}",
            }

        except Exception as e:
            return {
                "sql": None,
                "explanation": None,
                "tables_used": [],
                "confidence": 0,
                "results": None,
                "error": f"Agent error: {e}",
            }


# ── CLI entry point ──────────────────────────────────────────
# So you can test it directly: python -m agents.sql_agent "your question"

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m agents.sql_agent \"your question here\"")
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"\nQuestion: {question}\n")

    result = run_sql_agent(question)

    if result["error"]:
        print(f"Error: {result['error']}")
    else:
        print(f"SQL:\n{result['sql']}\n")
        print(f"Explanation: {result['explanation']}")
        print(f"Confidence: {result['confidence']}")
        print(f"Tables used: {', '.join(result['tables_used'])}")
        print(f"\nResults ({len(result['results'])} rows):")
        for row in result["results"][:10]:
            print(f"  {row}")
        if len(result["results"]) > 10:
            print(f"  ... and {len(result['results']) - 10} more rows")