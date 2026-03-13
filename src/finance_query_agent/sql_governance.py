"""SQL governance: enforce SELECT-only queries before execution."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

# All read-only set-operation root types
_SELECT_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except)


def validate_select_only(sql: str) -> None:
    """Parse sql and raise ValueError if it is not a read-only SELECT statement.

    Accepts plain SELECT and UNION / INTERSECT / EXCEPT of SELECTs.
    Rejects:
    - Non-SELECT root statements (INSERT, UPDATE, DELETE, DDL, etc.)
    - Write statements embedded in CTEs
    - SELECT INTO (writes results to a new table)
    - CROSS JOINs
    - set_config() calls (can bypass RLS by rewriting session GUCs)
    """
    try:
        statement = sqlglot.parse_one(sql, dialect="postgres")
    except sqlglot.errors.ParseError as exc:
        raise ValueError(f"Invalid SQL: {exc}") from exc

    if not isinstance(statement, _SELECT_ROOTS):
        raise ValueError(f"Only SELECT statements are allowed; got {type(statement).__name__}")

    _walk_and_check(statement)


_FORBIDDEN = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.TruncateTable,
    exp.Alter,
    exp.Command,
    exp.Into,  # SELECT ... INTO new_table
)


def _walk_and_check(statement: exp.Expression) -> None:
    for node in statement.walk():
        if isinstance(node, _FORBIDDEN):
            raise ValueError(f"SQL contains a forbidden statement type: {type(node).__name__}")
        if isinstance(node, exp.Join):
            kind = node.args.get("kind", "")
            # Explicit CROSS JOIN or implicit comma join (FROM a, b — no ON/USING clause)
            if kind and kind.upper() == "CROSS":
                raise ValueError("CROSS JOIN is not allowed")
            if not kind and not node.args.get("on") and not node.args.get("using"):
                raise ValueError("Implicit comma join (FROM a, b) is not allowed; use explicit JOIN with ON")
        if isinstance(node, exp.Anonymous) and node.this.lower() == "set_config":
            raise ValueError("set_config() is not allowed in queries")


_MAX_ROWS = 200


def cap_limit(sql: str, max_rows: int = _MAX_ROWS) -> str:
    """Rewrite sql to ensure LIMIT <= max_rows.

    - No LIMIT → appends LIMIT max_rows
    - LIMIT > max_rows (or non-literal LIMIT) → replaces with max_rows
    - Naturally bounded queries (scalar SELECT, ungrouped aggregate) → unchanged
    """
    statement = sqlglot.parse_one(sql, dialect="postgres")
    if isinstance(statement, exp.Select) and not _is_naturally_bounded(statement):
        _apply_cap(statement, max_rows)
    elif isinstance(statement, (exp.Union, exp.Intersect, exp.Except)):
        _apply_cap(statement, max_rows)
    return statement.sql(dialect="postgres")


def _apply_cap(statement: exp.Expression, max_rows: int) -> None:
    limit_expr = statement.args.get("limit")
    if limit_expr is not None:
        # Fetch nodes (FETCH FIRST n ROWS ONLY) store the count in .args['count'],
        # not in .expression like standard LIMIT nodes do.
        if isinstance(limit_expr, exp.Fetch):
            literal = limit_expr.args.get("count")
        else:
            literal = limit_expr.expression
        if isinstance(literal, exp.Literal) and not literal.is_string:
            if int(literal.this) <= max_rows:
                return  # already within bounds
    statement.set("limit", exp.Limit(expression=exp.Literal.number(max_rows)))


def _is_naturally_bounded(select: exp.Select) -> bool:
    """True if the SELECT always returns at most one row without a LIMIT."""
    # No FROM clause → scalar: SELECT 1, SELECT NOW(), etc.
    # sqlglot uses "from_" (trailing underscore) because "from" is a Python keyword.
    if not select.args.get("from_"):
        return True
    # Aggregate without GROUP BY → always returns exactly one row.
    # Walk only the outer SELECT list, not the full AST — a subquery in WHERE/HAVING
    # (e.g. WHERE x > (SELECT AVG(x) FROM t)) would otherwise trigger a false positive.
    if not select.args.get("group") and any(
        isinstance(n, exp.AggFunc) for expr in select.expressions for n in expr.walk()
    ):
        return True
    return False
