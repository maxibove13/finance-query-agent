"""SQL validation for the constrained fallback query tool.

Enforces: keyword rejection, single-SELECT, no CTEs/subqueries,
table/column allowlisting, LIMIT injection, and user filter injection.
"""

from __future__ import annotations

import re

from finance_query_agent.schemas.mapping import ColumnRef, SchemaMapping

# SQL keywords, functions, types, and date parts that are NOT column references.
# Used to avoid false positives when checking unqualified identifiers.
_SQL_SAFE_WORDS: frozenset[str] = frozenset(
    {
        # Keywords
        "select",
        "from",
        "where",
        "and",
        "or",
        "not",
        "in",
        "between",
        "like",
        "ilike",
        "is",
        "null",
        "as",
        "on",
        "join",
        "inner",
        "left",
        "right",
        "outer",
        "full",
        "cross",
        "natural",
        "group",
        "by",
        "order",
        "asc",
        "desc",
        "limit",
        "offset",
        "having",
        "distinct",
        "all",
        "union",
        "case",
        "when",
        "then",
        "else",
        "end",
        "true",
        "false",
        "exists",
        "any",
        "some",
        "cast",
        "over",
        "partition",
        "rows",
        "range",
        "unbounded",
        "preceding",
        "following",
        "current",
        "row",
        "within",
        "filter",
        "nulls",
        "first",
        "last",
        "lateral",
        "only",
        "do",
        "set",
        # Aggregate / scalar functions
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "abs",
        "coalesce",
        "nullif",
        "lower",
        "upper",
        "trim",
        "extract",
        "date_trunc",
        "to_char",
        "percentile_cont",
        "array_agg",
        "round",
        "floor",
        "ceil",
        "ceiling",
        "string_agg",
        "concat",
        "greatest",
        "least",
        "length",
        # Types / date parts
        "int",
        "integer",
        "bigint",
        "smallint",
        "numeric",
        "decimal",
        "real",
        "float",
        "double",
        "precision",
        "text",
        "varchar",
        "char",
        "boolean",
        "date",
        "time",
        "timestamp",
        "interval",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "week",
        "zone",
    }
)

# DML/DDL/dangerous keywords — case-insensitive word-boundary match.
# Defense-in-depth: the real security boundary is the read-only DB role.
_FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "COPY",
    "DO",
    "EXECUTE",
    "CALL",
    "SET",
    "RESET",
    "LISTEN",
    "NOTIFY",
    "LOAD",
    "VACUUM",
    "REINDEX",
)

_FORBIDDEN_RE = re.compile(
    r"\b(" + "|".join(_FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Reject double-quoted identifiers — no legitimate use in constrained fallback
_QUOTED_IDENT_RE = re.compile(r'"[^"]*"')

# Detect subqueries: SELECT inside parentheses
_SUBQUERY_RE = re.compile(r"\(\s*SELECT\b", re.IGNORECASE)

# Detect CTEs: WITH ... AS at the start
_CTE_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)

# Detect multiple statements (semicolons not at the very end)
_MULTI_STMT_RE = re.compile(r";(?!\s*$)")

# Match existing LIMIT clause
_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)


def _derive_allowlist(schema: SchemaMapping) -> tuple[set[str], set[str]]:
    """Derive allowed tables and columns from SchemaMapping."""
    tables: set[str] = set()
    columns: set[str] = set()

    for table_mapping in (
        schema.transactions,
        schema.categories,
        schema.accounts,
        schema.secondary_transactions,
    ):
        if table_mapping is None:
            continue

        tables.add(table_mapping.table.lower())

        for _key, col in table_mapping.columns.items():
            if isinstance(col, ColumnRef):
                tables.add(col.table.lower())
                columns.add(col.column.lower())
            else:
                columns.add(col.lower())

        for join in table_mapping.joins:
            tables.add(join.table.lower())

    return tables, columns


class SqlValidator:
    """Validates LLM-generated SQL for the constrained fallback tool."""

    def __init__(self, schema: SchemaMapping) -> None:
        self._schema = schema
        self._allowed_tables, self._allowed_columns = _derive_allowlist(schema)

    @property
    def allowed_tables(self) -> set[str]:
        return self._allowed_tables

    @property
    def allowed_columns(self) -> set[str]:
        return self._allowed_columns

    def validate(self, sql: str) -> list[str]:
        """Return a list of validation errors. Empty list means the SQL is valid."""
        errors: list[str] = []
        stripped = sql.strip()

        # 1. Must start with SELECT
        if not re.match(r"^\s*SELECT\b", stripped, re.IGNORECASE):
            errors.append("Query must start with SELECT")

        # 2. Forbidden keywords
        match = _FORBIDDEN_RE.search(stripped)
        if match:
            errors.append(f"Forbidden keyword: {match.group(0).upper()}")

        # 3. No CTEs
        if _CTE_RE.search(stripped):
            errors.append("CTEs (WITH ... AS) are not allowed")

        # 4. No subqueries
        if _SUBQUERY_RE.search(stripped):
            errors.append("Subqueries are not allowed")

        # 5. No multiple statements
        if _MULTI_STMT_RE.search(stripped):
            errors.append("Multiple statements are not allowed")

        # 5.5. No double-quoted identifiers
        if _QUOTED_IDENT_RE.search(stripped):
            errors.append("Double-quoted identifiers are not allowed")

        # 6. Table allowlist
        table_errors, alias_map = self._check_tables(stripped)
        errors.extend(table_errors)

        # 7. Column allowlist
        col_errors = self._check_columns(stripped, alias_map)
        errors.extend(col_errors)

        return errors

    # SQL keywords that should not be treated as aliases
    _SQL_KEYWORDS: frozenset[str] = frozenset(
        {
            "as",
            "on",
            "where",
            "inner",
            "left",
            "right",
            "cross",
            "full",
            "natural",
            "group",
            "order",
            "limit",
            "having",
            "and",
            "or",
            "set",
            "join",
            "select",
            "from",
            "not",
            "in",
            "between",
            "like",
            "is",
        }
    )

    def _check_tables(self, sql: str) -> tuple[list[str], dict[str, str]]:
        """Check that only allowed tables appear in FROM/JOIN clauses.

        Returns (errors, alias_map) where alias_map maps alias -> table name.
        """
        errors: list[str] = []
        alias_map: dict[str, str] = {}

        # Handles optional schema prefix and optional alias.
        # Negative lookahead prevents capturing SQL keywords as aliases.
        kw_alt = (
            r"ON|WHERE|INNER|LEFT|RIGHT|CROSS|FULL|NATURAL|GROUP|ORDER|"
            r"LIMIT|HAVING|JOIN|FROM|SELECT|SET|AND|OR|NOT|IN|BETWEEN|LIKE|IS"
        )
        pattern = re.compile(
            rf"\b(?:FROM|JOIN)\s+(?:[a-zA-Z_]\w*\.)?([a-zA-Z_]\w*)"
            rf"(?:\s+(?:AS\s+)?(?!(?:{kw_alt})\b)([a-zA-Z_]\w*))?",
            re.IGNORECASE,
        )
        for m in pattern.finditer(sql):
            table_name = m.group(1).lower()
            if table_name not in self._allowed_tables:
                errors.append(f"Table not in allowlist: {m.group(1)}")

            alias = m.group(2)
            if alias and alias.lower() not in self._SQL_KEYWORDS:
                alias_map[alias.lower()] = table_name

        return errors, alias_map

    def _check_columns(self, sql: str, alias_map: dict[str, str]) -> list[str]:
        """Check that only allowed columns appear in the query.

        Two passes:
        1. Qualified references (table.column or alias.column) — resolve aliases.
        2. Unqualified bare identifiers — reject anything that isn't a known
           column, SQL keyword/function, table name, table alias, or SELECT alias.
        """
        errors: list[str] = []

        # --- Pass 1: qualified references (prefix.column) ---
        qualified_re = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b")
        for m in qualified_re.finditer(sql):
            prefix = m.group(1).lower()
            col_name = m.group(2).lower()
            resolved_table = alias_map.get(prefix, prefix)
            if resolved_table in self._allowed_tables and col_name not in self._allowed_columns:
                errors.append(f"Column not in allowlist: {m.group(1)}.{m.group(2)}")

        # --- Pass 2: unqualified bare identifiers ---
        # Strip string literals and type casts to avoid false positives
        cleaned = re.sub(r"'[^']*'", "''", sql)
        cleaned = re.sub(r"::\w+", "", cleaned)

        # Bare identifiers: not preceded by '.', not followed by '(' (function)
        # or '.' (table prefix for qualified ref).
        bare_re = re.compile(r"(?<!\.)\b([a-zA-Z_]\w*)\b(?!\s*\(|\.[a-zA-Z_])")

        # Build exclusion set: SQL keywords/functions + table names + aliases
        safe = _SQL_SAFE_WORDS | self._allowed_tables | set(alias_map.keys())
        # SELECT aliases (identifiers after AS) are not column references
        as_re = re.compile(r"\bAS\s+([a-zA-Z_]\w*)\b", re.IGNORECASE)
        safe = safe | {m.group(1).lower() for m in as_re.finditer(cleaned)}

        seen: set[str] = set()
        for m in bare_re.finditer(cleaned):
            ident = m.group(1).lower()
            if ident in self._allowed_columns or ident in safe:
                continue
            # Deduplicate within a single validation run
            if ident not in seen:
                seen.add(ident)
                errors.append(f"Column not in allowlist: {m.group(1)}")

        return errors

    def inject_limit(self, sql: str, max_limit: int = 200) -> str:
        """Add LIMIT if not present; cap existing LIMIT if it exceeds max_limit."""
        match = _LIMIT_RE.search(sql)
        if match:
            existing = int(match.group(1))
            if existing > max_limit:
                sql = sql[: match.start(1)] + str(max_limit) + sql[match.end(1) :]
            return sql

        # Strip trailing semicolon, add LIMIT, restore semicolon
        stripped = sql.rstrip()
        if stripped.endswith(";"):
            return stripped[:-1] + f" LIMIT {max_limit};"
        return stripped + f" LIMIT {max_limit}"

    def inject_user_filter(
        self,
        sql: str,
        user_id_param: str,
    ) -> str:
        """Inject user_id = $N into the query's WHERE clause.

        Resolves the user_id column from the schema mapping (direct or ColumnRef).
        If a WHERE clause exists, appends AND. Otherwise inserts WHERE before
        GROUP BY / ORDER BY / LIMIT / end of query.
        """
        # Resolve user_id column reference
        user_col = self._schema.transactions.columns.get("user_id")
        if isinstance(user_col, ColumnRef):
            user_ref = f"{user_col.table}.{user_col.column}"
        elif isinstance(user_col, str):
            user_ref = f"{self._schema.transactions.table}.{user_col}"
        else:
            # Shouldn't happen if schema validation passed
            user_ref = "user_id"

        condition = f"{user_ref} = {user_id_param}"

        # Check if WHERE already exists
        where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
        if where_match:
            # Insert after WHERE keyword (before the existing conditions)
            insert_pos = where_match.end()
            return sql[:insert_pos] + f" {condition} AND" + sql[insert_pos:]

        # No WHERE — insert before GROUP BY / ORDER BY / LIMIT / HAVING or at end
        clause_match = re.search(
            r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b",
            sql,
            re.IGNORECASE,
        )
        if clause_match:
            insert_pos = clause_match.start()
            return sql[:insert_pos] + f"WHERE {condition} " + sql[insert_pos:]

        # No trailing clauses — append before semicolon or at end
        stripped = sql.rstrip()
        if stripped.endswith(";"):
            return stripped[:-1] + f" WHERE {condition};"
        return stripped + f" WHERE {condition}"
