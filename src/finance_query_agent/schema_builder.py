"""Build the database schema context for the system prompt.

Fetches the semantic model from SSM at cold start, then merges with live
DB introspection. Result is cached across warm Lambda invocations.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml

from finance_query_agent.config import get_settings
from finance_query_agent.connection import Connection

logger = logging.getLogger(__name__)

_AGENT_TABLES = ["accounts", "account_movements", "credit_card_movements", "credit_cards", "tags"]

_SEMANTIC: dict[str, Any] | None = None  # populated once per cold start from SSM; update requires redeployment

_schema_context: str | None = None  # cached for the lifetime of the Lambda instance; update requires redeployment


def _fetch_semantic() -> dict[str, Any]:
    import boto3  # type: ignore[import-untyped]

    config = get_settings()
    value = boto3.client("ssm").get_parameter(Name=config.semantic_model_ssm_path)["Parameter"]["Value"]
    result: dict[str, Any] = yaml.safe_load(value)
    return result


def _get_semantic() -> dict[str, Any]:
    global _SEMANTIC
    if _SEMANTIC is None:
        _SEMANTIC = _fetch_semantic()
    return _SEMANTIC


async def get_schema_context(conn: Connection) -> str:
    global _schema_context
    if _schema_context is None:
        introspected = await _introspect(conn)
        _schema_context = _render(_get_semantic(), introspected)
    return _schema_context


async def _introspect(conn: Connection) -> dict[str, list[dict[str, Any]]]:
    rows = await conn.fetch(
        """SELECT table_name, column_name, data_type, is_nullable
           FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = ANY($1)
           ORDER BY table_name, ordinal_position""",
        _AGENT_TABLES,
    )
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(row["table_name"], []).append(
            {
                "name": row["column_name"],
                "data_type": row["data_type"],
                "nullable": row["is_nullable"] == "YES",
            }
        )
    return result


def _render(semantic: dict[str, Any], introspected: dict[str, list[dict[str, Any]]]) -> str:
    semantic_tables = {t["name"]: t for t in semantic.get("tables", [])}
    lines: list[str] = []
    lines.append(f"description: {semantic['description']}")
    lines.append("")
    lines.append("tables:")
    for table_name in _AGENT_TABLES:
        sem_table = semantic_tables.get(table_name, {})
        db_columns = introspected.get(table_name, [])
        lines.append(f"  {table_name}:")
        if desc := sem_table.get("description"):
            lines.append(f"    description: {desc}")
        sem_cols: dict[str, dict[str, Any]] = {}
        for group in ("dimensions", "time_dimensions", "facts"):
            for col in sem_table.get(group, []):
                sem_cols[col["name"]] = col
        lines.append("    columns:")
        for col in db_columns:
            col_name = col["name"]
            sem = sem_cols.get(col_name, {})
            nullable_note = ", nullable" if col["nullable"] else ""
            line = f'      {col_name}: "{col["data_type"]}{nullable_note}'
            if desc := sem.get("description"):
                line += f" — {desc}"
            if synonyms := sem.get("synonyms"):
                line += f" [also: {', '.join(synonyms)}]"
            if sem.get("is_enum") and (vals := sem.get("sample_values")):
                line += f" [values: {', '.join(vals)}]"
            lines.append(line + '"')
        if metrics := sem_table.get("metrics"):
            lines.append("    metrics:")
            for m in metrics:
                filt = f" WHERE {m['filter']['expr']}" if m.get("filter") else ""
                lines.append(f"      {m['name']}: {m['expr']}{filt}  # {m.get('description', '')}")
    lines.append("")
    lines.append("relationships:")
    for rel in semantic.get("relationships", []):
        cols = rel.get("relationship_columns", [])
        join_str = " AND ".join(
            f"{rel['left_table']}.{c['left_column']} = {rel['right_table']}.{c['right_column']}" for c in cols
        )
        lines.append(f"  - {rel['left_table']} {rel['join_type'].upper()} JOIN {rel['right_table']} ON {join_str}")
    if instructions := semantic.get("custom_instructions"):
        lines.append("")
        lines.append("business_rules:")
        for instruction_line in instructions.strip().splitlines():
            lines.append(f"  {instruction_line}")
    if queries := semantic.get("verified_queries"):
        lines.append("")
        lines.append("verified_queries:")
        for q in queries:
            lines.append(f"  - question: {q['question']}")
            lines.append("    sql: |")
            for sql_line in q["sql"].strip().splitlines():
                lines.append(f"      {sql_line}")
    return "\n".join(lines)
