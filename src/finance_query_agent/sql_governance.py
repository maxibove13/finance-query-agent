"""SQL governance: enforce SELECT-only queries before execution."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp


def validate_select_only(sql: str) -> None:
    """Parse sql and raise ValueError if it is not a plain SELECT statement.

    Rejects any statement whose top-level node is not a SELECT, and also
    rejects SELECTs that embed write statements via CTEs (WITH ... INSERT ...).
    """
    try:
        statement = sqlglot.parse_one(sql, dialect="postgres")
    except sqlglot.errors.ParseError as exc:
        raise ValueError(f"Invalid SQL: {exc}") from exc

    forbidden = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Create,
        exp.TruncateTable,
        exp.Alter,
        exp.Command,
    )

    if not isinstance(statement, exp.Select):
        raise ValueError(f"Only SELECT statements are allowed; got {type(statement).__name__}")

    for node in statement.walk():
        if isinstance(node, forbidden):
            raise ValueError(f"SQL contains a forbidden statement type: {type(node).__name__}")
        if isinstance(node, exp.Join) and node.args.get("kind", "").upper() == "CROSS":
            raise ValueError("CROSS JOIN is not allowed")

    if statement.args.get("limit") is None:
        raise ValueError("Query must include a LIMIT clause")
