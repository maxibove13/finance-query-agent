"""Validate SchemaMapping against a live PostgreSQL database."""

from __future__ import annotations

import logging

from finance_query_agent.connection import Connection
from finance_query_agent.exceptions import SchemaValidationError
from finance_query_agent.schemas.mapping import ColumnRef, SchemaMapping, TableMapping

logger = logging.getLogger(__name__)


async def _get_db_columns(conn: Connection) -> dict[str, set[str]]:
    """Fetch all table->columns from information_schema. Returns {table: {col1, col2, ...}}."""
    rows = await conn.fetch(
        "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'"
    )
    result: dict[str, set[str]] = {}
    for row in rows:
        result.setdefault(row["table_name"], set()).add(row["column_name"])
    return result


def _resolve_column(col: str | ColumnRef) -> tuple[str | None, str]:
    """Return (table_or_none, column_name) for a column mapping value."""
    if isinstance(col, ColumnRef):
        return col.table, col.column
    return None, col


def _validate_table_mapping(
    name: str,
    mapping: TableMapping,
    db_columns: dict[str, set[str]],
    errors: list[str],
) -> None:
    """Validate a single TableMapping against DB columns."""
    # Check table exists
    if mapping.table not in db_columns:
        errors.append(f"{name}: table '{mapping.table}' does not exist in the database")
        return  # no point checking columns

    table_cols = db_columns[mapping.table]

    for key, col in mapping.columns.items():
        ref_table, ref_col = _resolve_column(col)
        if ref_table:
            # ColumnRef — check the referenced table and column exist
            if ref_table not in db_columns:
                errors.append(f"{name}.columns.{key}: referenced table '{ref_table}' does not exist")
            elif ref_col not in db_columns[ref_table]:
                errors.append(f"{name}.columns.{key}: column '{ref_col}' does not exist on table '{ref_table}'")
        else:
            # Direct column — must exist on the mapping's own table
            if ref_col not in table_cols:
                errors.append(f"{name}.columns.{key}: column '{ref_col}' does not exist on table '{mapping.table}'")

    # Validate join tables exist and join condition columns exist
    for join_def in mapping.joins:
        if join_def.table not in db_columns:
            errors.append(f"{name}: join table '{join_def.table}' does not exist")

    # Validate amount_convention direction_column exists if set
    if mapping.amount_convention and mapping.amount_convention.direction_column:
        dir_col = mapping.amount_convention.direction_column
        if dir_col not in table_cols:
            errors.append(
                f"{name}: amount_convention.direction_column '{dir_col}' does not exist on table '{mapping.table}'"
            )


async def validate_schema(schema: SchemaMapping, conn: Connection) -> None:
    """Validate that all mapped tables and columns exist in the live database.

    Raises SchemaValidationError with all collected errors.
    """
    db_columns = await _get_db_columns(conn)
    errors: list[str] = []

    # Validate each table mapping
    _validate_table_mapping("transactions", schema.transactions, db_columns, errors)
    _validate_table_mapping("categories", schema.categories, db_columns, errors)
    _validate_table_mapping("accounts", schema.accounts, db_columns, errors)

    if schema.secondary_transactions:
        _validate_table_mapping("secondary_transactions", schema.secondary_transactions, db_columns, errors)

    if errors:
        raise SchemaValidationError("Schema validation failed:\n  - " + "\n  - ".join(errors))

    logger.info("Schema validation passed")


async def introspect_schema(conn: Connection, tables: list[str]) -> str:
    """Return a DDL-like schema description for the given tables.

    Used as LLM context for the fallback SQL tool.
    """
    rows = await conn.fetch(
        "SELECT table_name, column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = ANY($1) "
        "ORDER BY table_name, ordinal_position",
        tables,
    )

    if not rows:
        return ""

    lines: list[str] = []
    current_table = ""
    for row in rows:
        if row["table_name"] != current_table:
            if current_table:
                lines.append(")")
                lines.append("")
            current_table = row["table_name"]
            lines.append(f"TABLE {current_table} (")

        nullable = " NULL" if row["is_nullable"] == "YES" else " NOT NULL"
        lines.append(f"  {row['column_name']} {row['data_type']}{nullable},")

    if current_table:
        lines.append(")")

    return "\n".join(lines)
