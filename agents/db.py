"""
Database connection, schema introspection, and query execution.

This module gives the SQL agent everything it needs to work with
the database: a connection, a description of what tables/columns
exist, and a safe way to run queries.
"""

import os
from functools import lru_cache

from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

# ── Connection ───────────────────────────────────────────────
# Reads DATABASE_URL from .env file, or falls back to the
# Docker Compose defaults we set up earlier.

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://analyst:analyst_pass@localhost:5432/analytics",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def get_engine():
    """Return the SQLAlchemy engine (used by other modules)."""
    return engine


# ── Schema Introspection ─────────────────────────────────────
# This is the key function. It reads the database structure and
# builds a text description that gets sent to Claude along with
# the user's question. Without this, Claude would have to guess
# what tables and columns exist.

@lru_cache(maxsize=1)  # Cache it — schema doesn't change at runtime
def get_schema_description() -> str:
    """Build a text description of every table in the database.

    Includes:
    - Table names with all columns, types, and nullability
    - Primary and foreign key relationships
    - Sample values for categorical columns (so Claude knows
      valid values for WHERE clauses, e.g. segment IN ('Enterprise','SMB'))
    - Row counts (so Claude can gauge data volume)

    This string gets injected into the SQL agent's system prompt.
    """
    inspector = inspect(engine)
    parts = ["## Database Schema\n"]

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        pk = inspector.get_pk_constraint(table_name)
        fks = inspector.get_foreign_keys(table_name)

        parts.append(f"### {table_name}")
        parts.append(f"Primary Key: {', '.join(pk['constrained_columns'])}\n")

        # Column listing
        parts.append("| Column | Type | Nullable |")
        parts.append("|--------|------|----------|")
        for col in columns:
            nullable = "Yes" if col["nullable"] else "No"
            parts.append(f"| {col['name']} | {col['type']} | {nullable} |")

        # Foreign keys — tells Claude how tables connect
        if fks:
            parts.append("\nForeign Keys:")
            for fk in fks:
                local = ", ".join(fk["constrained_columns"])
                remote_table = fk["referred_table"]
                remote_cols = ", ".join(fk["referred_columns"])
                parts.append(f"  - {local} → {remote_table}({remote_cols})")

        # Row count + sample values
        with engine.connect() as conn:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
            parts.append(f"\nRow count: {count:,}")

            # Show distinct values for text columns so Claude
            # knows what's valid (e.g. channel IN ('Email','Social',...))
            for col in columns:
                col_type = str(col["type"]).upper()
                if "VARCHAR" in col_type and col["name"] not in ("email", "full_name"):
                    try:
                        rows = conn.execute(
                            text(f"SELECT DISTINCT {col['name']} FROM {table_name} LIMIT 15")
                        ).fetchall()
                        values = [str(r[0]) for r in rows if r[0] is not None]
                        if values:
                            parts.append(f"  {col['name']} values: {', '.join(values)}")
                    except Exception:
                        pass

        parts.append("")  # blank line between tables

    # Also describe views (monthly_revenue, customer_cohorts)
    views = inspector.get_view_names()
    if views:
        parts.append("## Views")
        for view_name in views:
            columns = inspector.get_columns(view_name)
            parts.append(f"### {view_name}")
            parts.append("| Column | Type |")
            parts.append("|--------|------|")
            for col in columns:
                parts.append(f"| {col['name']} | {col['type']} |")
            parts.append("")

    return "\n".join(parts)


# ── Query Execution ──────────────────────────────────────────

def execute_query(sql: str) -> list[dict]:
    """Execute a SELECT query and return results as a list of dicts.

    Blocks any destructive operations (INSERT, UPDATE, DELETE, etc.)
    so even if Claude hallucinates a DROP TABLE, it won't run.
    """
    sql_upper = sql.strip().upper()
    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"]
    for keyword in blocked:
        if sql_upper.startswith(keyword):
            raise ValueError(f"Blocked: {keyword} operations are not allowed")

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def validate_sql(sql: str) -> tuple[bool, str]:
    """Check if SQL is valid without actually running it.

    Uses PostgreSQL's EXPLAIN command — it parses and plans the
    query but doesn't execute it. If the SQL has a syntax error
    or references a table that doesn't exist, EXPLAIN will fail.

    Returns (is_valid, error_message).
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"EXPLAIN {sql}"))
        return True, ""
    except Exception as e:
        return False, str(e)
