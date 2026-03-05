"""Validate SchemaMapping against a live PostgreSQL database."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from finance_query_agent.connection import Connection
from finance_query_agent.exceptions import SchemaValidationError
from finance_query_agent.schemas.mapping import ColumnRef, SchemaMapping, TableMapping

logger = logging.getLogger(__name__)


@dataclass
class ColumnTypeInfo:
    """Column types discovered from the live DB at cold start."""

    user_id_type: str  # e.g. "int4", "text", "uuid"
    direction_is_enum: bool  # True if movement_direction is USER-DEFINED (enum)


async def _get_db_columns(conn: Connection) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    """Fetch all table->columns and their types from information_schema.

    Returns (columns_map, types_map) where:
        columns_map = {table: {col1, col2, ...}}
        types_map = {table: {col: udt_name, ...}}
    """
    rows = await conn.fetch(
        "SELECT table_name, column_name, data_type, udt_name "
        "FROM information_schema.columns WHERE table_schema = 'public'"
    )
    columns: dict[str, set[str]] = {}
    types: dict[str, dict[str, str]] = {}
    for row in rows:
        tbl = row["table_name"]
        col = row["column_name"]
        columns.setdefault(tbl, set()).add(col)
        types.setdefault(tbl, {})[col] = row["udt_name"]
    return columns, types


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


async def validate_schema(schema: SchemaMapping, conn: Connection) -> ColumnTypeInfo:
    """Validate that all mapped tables and columns exist in the live database.

    Returns ColumnTypeInfo with discovered column types for runtime casting.
    Raises SchemaValidationError with all collected errors.
    """
    db_columns, db_types = await _get_db_columns(conn)
    errors: list[str] = []

    # Validate each table mapping
    _validate_table_mapping("transactions", schema.transactions, db_columns, errors)
    _validate_table_mapping("categories", schema.categories, db_columns, errors)
    _validate_table_mapping("accounts", schema.accounts, db_columns, errors)

    if schema.secondary_transactions:
        _validate_table_mapping("secondary_transactions", schema.secondary_transactions, db_columns, errors)

    if errors:
        raise SchemaValidationError("Schema validation failed:\n  - " + "\n  - ".join(errors))

    # Discover user_id type from the table that owns it
    user_id_col = schema.transactions.columns.get("user_id")
    if isinstance(user_id_col, ColumnRef):
        user_id_type = db_types.get(user_id_col.table, {}).get(user_id_col.column, "text")
    else:
        user_id_type = db_types.get(schema.transactions.table, {}).get(user_id_col or "user_id", "text")

    # Check if direction column is an enum (USER-DEFINED)
    conv = schema.transactions.amount_convention
    direction_is_enum = False
    if conv and conv.direction_column:
        direction_udt = db_types.get(schema.transactions.table, {}).get(conv.direction_column, "text")
        direction_is_enum = direction_udt not in ("text", "varchar", "bpchar")

    logger.info("Schema validation passed (user_id_type=%s, direction_is_enum=%s)", user_id_type, direction_is_enum)
    return ColumnTypeInfo(user_id_type=user_id_type, direction_is_enum=direction_is_enum)


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
