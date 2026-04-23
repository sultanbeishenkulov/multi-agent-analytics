"""
Session memory — persists conversation history to SQLite so follow-up questions
work across process restarts.

Each session is identified by a session_id string. Within a session, every
pipeline turn is stored as a (user, assistant) message pair, which is the
format run_sql_agent already expects for conversation_history.

Usage:
    memory = SessionMemory()
    history = memory.load(session_id)
    # ... run pipeline ...
    memory.save_turn(session_id, question, sql, summary)
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "memory.db"


class SessionMemory:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS turns (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id  TEXT    NOT NULL,
                    question    TEXT    NOT NULL,
                    sql         TEXT,
                    summary     TEXT,
                    created_at  TEXT    NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_session ON turns(session_id, id)")

    def save_turn(
        self,
        session_id: str,
        question: str,
        sql: str | None,
        summary: str | None,
    ):
        """Persist one pipeline turn."""
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO turns (session_id, question, sql, summary, created_at) VALUES (?,?,?,?,?)",
                (session_id, question, sql, summary, datetime.now(timezone.utc).isoformat()),
            )

    def load(self, session_id: str, max_turns: int = 6) -> list[dict]:
        """Load the last N turns as a conversation_history list.

        Returns messages in the format run_sql_agent expects:
          [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}, ...]
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT question, sql, summary FROM turns WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                (session_id, max_turns),
            ).fetchall()

        messages = []
        for row in reversed(rows):
            messages.append({"role": "user", "content": row["question"]})
            assistant_content = ""
            if row["summary"]:
                assistant_content += row["summary"]
            if row["sql"]:
                assistant_content += f"\n\nSQL used:\n{row['sql']}"
            messages.append({"role": "assistant", "content": assistant_content.strip()})

        return messages

    def list_sessions(self) -> list[dict]:
        """List all sessions with their turn count and last activity."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT session_id,
                       COUNT(*) AS turns,
                       MIN(created_at) AS started,
                       MAX(created_at) AS last_active
                FROM turns
                GROUP BY session_id
                ORDER BY last_active DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def delete_session(self, session_id: str):
        """Remove all turns for a session."""
        with self._connect() as conn:
            conn.execute("DELETE FROM turns WHERE session_id = ?", (session_id,))
