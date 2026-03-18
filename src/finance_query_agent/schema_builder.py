"""Build the database schema context for the system prompt.

Reads the semantic model YAML from S3 at cold start. The YAML is the sole source
of truth — no DB introspection. Result is cached across warm Lambda invocations.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml

from finance_query_agent.config import get_settings

logger = logging.getLogger(__name__)

_SEMANTIC: dict[str, Any] | None = None  # populated once per cold start from S3; update requires redeployment

_schema_context: str | None = None  # cached for the lifetime of the Lambda instance; update requires redeployment


def _fetch_semantic() -> dict[str, Any]:
    config = get_settings()
    if config.semantic_model_local_path:
        from pathlib import Path

        result: dict[str, Any] = yaml.safe_load(Path(config.semantic_model_local_path).read_text())
        return result
    import boto3  # type: ignore[import-untyped]

    if not config.semantic_model_s3_bucket:
        raise ValueError("SEMANTIC_MODEL_S3_BUCKET must be set when semantic_model_local_path is not provided")
    body = (
        boto3.client("s3")
        .get_object(Bucket=config.semantic_model_s3_bucket, Key=config.semantic_model_s3_key)["Body"]
        .read()
    )
    result: dict[str, Any] = yaml.safe_load(body)  # type: ignore[no-redef]
    return result


def _get_semantic() -> dict[str, Any]:
    global _SEMANTIC
    if _SEMANTIC is None:
        _SEMANTIC = _fetch_semantic()
    return _SEMANTIC


def get_allowed_tables() -> frozenset[str]:
    """Return the set of table names defined in the semantic model."""
    semantic = _get_semantic()
    return frozenset(t["name"] for t in semantic.get("tables", []))


def get_schema_context() -> str:
    global _schema_context
    if _schema_context is None:
        _schema_context = _render(_get_semantic())
    return _schema_context


def _render(semantic: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"description: {semantic['description']}")
    lines.append("")
    lines.append("tables:")
    for table in semantic.get("tables", []):
        table_name = table["name"]
        lines.append(f"  {table_name}:")
        if desc := table.get("description"):
            lines.append(f"    description: {desc}")
        lines.append("    columns:")
        for group in ("dimensions", "time_dimensions", "facts"):
            for col in table.get(group, []):
                if col.get("access") == "private":
                    continue
                data_type = col.get("data_type", "unknown")
                nullable_note = ", nullable" if col.get("nullable") else ""
                line = f'      {col["name"]}: "{data_type}{nullable_note}'
                if desc := col.get("description"):
                    line += f" — {desc}"
                if synonyms := col.get("synonyms"):
                    line += f" [also: {', '.join(synonyms)}]"
                if col.get("is_enum") and (vals := col.get("sample_values")):
                    line += f" [values: {', '.join(vals)}]"
                lines.append(line + '"')
        if metrics := table.get("metrics"):
            lines.append("    metrics:")
            for m in metrics:
                filt = f" WHERE {m['filter']['expr']}" if m.get("filter") else ""
                non_additive = " [non-additive]" if m.get("non_additive") else ""
                lines.append(f"      {m['name']}: {m['expr']}{filt}  # {m.get('description', '')}{non_additive}")
    if filters := semantic.get("filters"):
        lines.append("")
        lines.append("filters:")
        for f in filters:
            synonyms_note = f"  # synonyms: {', '.join(f['synonyms'])}" if f.get("synonyms") else ""
            desc_note = f" — {f['description']}" if f.get("description") else ""
            requires_note = ""
            if f.get("requires_join"):
                requires_note = f"  # requires JOIN to {f['requires_join']}"
            lines.append(f"  {f['name']}: {f['expr']}{desc_note}{synonyms_note}{requires_note}")
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
