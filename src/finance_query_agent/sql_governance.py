"""SQL governance: enforce SELECT-only queries before execution."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp


def validate_select_only(sql: str) -> None:
    """Parse sql and raise ValueError if it is not a read-only SELECT statement.

    Accepts plain SELECT and UNION ALL / UNION / INTERSECT / EXCEPT of SELECTs.
    Rejects:
    - Non-SELECT root statements (INSERT, UPDATE, DELETE, DDL, etc.)
    - Write statements embedded in CTEs
    - CROSS JOINs
    - set_config() calls (can bypass RLS by rewriting session GUCs)
    - Missing LIMIT when the query returns a variable number of rows
    """
    try:
        statement = sqlglot.parse_one(sql, dialect="postgres")
    except sqlglot.errors.ParseError as exc:
        raise ValueError(f"Invalid SQL: {exc}") from exc

    if not isinstance(statement, (exp.Select, exp.Union)):
        raise ValueError(f"Only SELECT statements are allowed; got {type(statement).__name__}")

    _walk_and_check(statement)
    _check_limit(statement)


_FORBIDDEN = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.TruncateTable,
    exp.Alter,
    exp.Command,
)


def _walk_and_check(statement: exp.Expression) -> None:
    for node in statement.walk():
        if isinstance(node, _FORBIDDEN):
            raise ValueError(f"SQL contains a forbidden statement type: {type(node).__name__}")
        if isinstance(node, exp.Join) and node.args.get("kind", "").upper() == "CROSS":
            raise ValueError("CROSS JOIN is not allowed")
        if isinstance(node, exp.Anonymous) and node.this.lower() == "set_config":
            raise ValueError("set_config() is not allowed in queries")


def _check_limit(statement: exp.Expression) -> None:
    """Require LIMIT when the query can return a variable number of rows."""
    if isinstance(statement, exp.Union):
        # UNION combines multiple selects — LIMIT applies to the combined result
        if statement.args.get("limit") is None:
            raise ValueError("Query must include a LIMIT clause")
        return
    assert isinstance(statement, exp.Select)
    if _is_naturally_bounded(statement):
        return
    if statement.args.get("limit") is None:
        raise ValueError("Query must include a LIMIT clause")


def _is_naturally_bounded(select: exp.Select) -> bool:
    """True if the SELECT always returns at most one row without a LIMIT."""
    # No FROM clause → scalar: SELECT 1, SELECT NOW(), etc.
    # sqlglot uses "from_" (trailing underscore) because "from" is a Python keyword.
    if not select.args.get("from_"):
        return True
    # Aggregate without GROUP BY → always returns exactly one row
    if not select.args.get("group") and any(isinstance(n, exp.AggFunc) for n in select.walk()):
        return True
    return False
