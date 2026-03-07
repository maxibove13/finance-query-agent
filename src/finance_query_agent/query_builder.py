"""SQL generation from SchemaMapping + tool parameters."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    SchemaMapping,
    TableMapping,
)


@dataclass
class GeneratedQuery:
    sql: str
    params: list[Any]


class QueryBuilder:
    """Generates parameterized SQL from a SchemaMapping.

    Stateless after construction. One method per tool query,
    each returns a GeneratedQuery(sql, params).
    """

    def __init__(self, schema: SchemaMapping) -> None:
        self._schema = schema

    # -- Helpers: column resolution ------------------------------------------

    def _resolve_col(self, table_mapping: TableMapping, key: str) -> str:
        """Resolve a logical column key to a fully-qualified 'table.column' reference."""
        col = table_mapping.columns[key]
        if isinstance(col, ColumnRef):
            return f"{col.table}.{col.column}"
        return f"{table_mapping.table}.{col}"

    def _joins_sql(self, table_mapping: TableMapping) -> str:
        """Build JOIN clauses from a TableMapping's join definitions."""
        parts = []
        for j in table_mapping.joins:
            jtype = "LEFT JOIN" if j.type == "left" else "INNER JOIN"
            parts.append(f"{jtype} {j.table} ON {j.on}")
        return " ".join(parts)

    # -- Helpers: AmountConvention -------------------------------------------

    def _expense_filter(self, conv: AmountConvention, table: TableMapping) -> str:
        """WHERE clause fragment for filtering expenses only."""
        if conv.direction_column is not None:
            col = f"{table.table}.{conv.direction_column}"
            return f"{col} = ${{}}"  # placeholder index filled by caller
        if conv.sign_means_expense == "negative":
            return f"{self._resolve_col(table, 'amount')} < 0"
        # sign_means_expense == "positive"
        return f"{self._resolve_col(table, 'amount')} > 0"

    def _income_filter(self, conv: AmountConvention, table: TableMapping) -> str:
        """WHERE clause fragment for filtering income only."""
        if conv.direction_column is not None:
            col = f"{table.table}.{conv.direction_column}"
            return f"{col} = ${{}}"
        if conv.sign_means_expense == "negative":
            return f"{self._resolve_col(table, 'amount')} > 0"
        return f"{self._resolve_col(table, 'amount')} < 0"

    def _sum_amount_expr(self, conv: AmountConvention, table: TableMapping) -> str:
        """SUM expression that always yields a positive total for spending tools."""
        amt = self._resolve_col(table, "amount")
        if conv.sign_means_expense == "negative":
            return f"SUM(ABS({amt}))"
        return f"SUM({amt})"

    # -- Helpers: building blocks --------------------------------------------

    def _transaction_tables(self) -> list[TableMapping]:
        """Return the list of transaction TableMappings (primary + optional secondary)."""
        tables = [self._schema.transactions]
        if self._schema.secondary_transactions is not None:
            tables.append(self._schema.secondary_transactions)
        return tables

    def _add_expense_filter(
        self,
        table: TableMapping,
        where: list[str],
        params: list[Any],
    ) -> None:
        """Append expense filter to where list and params."""
        conv = table.amount_convention
        assert conv is not None
        if conv.direction_column is not None:
            params.append(conv.expense_value)
            idx = len(params)
            col = f"{table.table}.{conv.direction_column}"
            where.append(f"{col}::text = ${idx}")
        else:
            where.append(self._expense_filter(conv, table))

    def _add_income_filter(
        self,
        table: TableMapping,
        where: list[str],
        params: list[Any],
    ) -> None:
        """Append income filter to where list and params."""
        conv = table.amount_convention
        assert conv is not None
        if conv.direction_column is not None:
            params.append(conv.income_value)
            idx = len(params)
            col = f"{table.table}.{conv.direction_column}"
            where.append(f"{col}::text = ${idx}")
        else:
            where.append(self._income_filter(conv, table))

    def _add_direction_filter(
        self,
        direction: str | None,
        table: TableMapping,
        where: list[str],
        params: list[Any],
    ) -> None:
        """Add expense/income filter based on direction string."""
        if direction == "expense":
            self._add_expense_filter(table, where, params)
        elif direction == "income":
            self._add_income_filter(table, where, params)

    def _add_user_filter(
        self,
        table: TableMapping,
        where: list[str],
        params: list[Any],
        user_id: Any,
    ) -> None:
        """Add user_id = $N to where and params."""
        params.append(user_id)
        idx = len(params)
        user_col = self._resolve_col(table, "user_id")
        where.append(f"{user_col} = ${idx}")

    def _add_date_range(
        self,
        table: TableMapping,
        where: list[str],
        params: list[Any],
        period_start: date,
        period_end: date,
    ) -> None:
        """Add date range filters to where and params."""
        date_col = self._resolve_col(table, "date")
        params.append(period_start)
        where.append(f"{date_col} >= ${len(params)}")
        params.append(period_end)
        where.append(f"{date_col} <= ${len(params)}")

    def _add_account_filter(
        self,
        table: TableMapping,
        where: list[str],
        params: list[Any],
        account_id: str | None,
    ) -> None:
        """Add account_id filter if provided."""
        if account_id is not None:
            acct_col = self._resolve_col(table, "account_id")
            params.append(account_id)
            where.append(f"{acct_col} = ${len(params)}")

    # -- Helpers: UNION ALL across transaction tables ------------------------

    def _build_union_query(
        self,
        *,
        user_id: Any,
        build_one: Callable[..., tuple[str, list[Any]]],
    ) -> GeneratedQuery:
        """Build a UNION ALL query across all transaction tables.

        build_one is called for each table and must return (select_sql, params).
        Parameters are renumbered for the second table.
        """
        tables = self._transaction_tables()
        if len(tables) == 1:
            sql, params = build_one(tables[0], param_offset=0)
            return GeneratedQuery(sql=sql, params=params)

        # Build both sides; second side has param_offset = len(first_params)
        sql1, params1 = build_one(tables[0], param_offset=0)
        sql2, params2 = build_one(tables[1], param_offset=len(params1))

        combined_sql = f"({sql1}) UNION ALL ({sql2})"
        combined_params = params1 + params2
        return GeneratedQuery(sql=combined_sql, params=combined_params)

    # -- Tool queries --------------------------------------------------------

    def build_search_transactions(
        self,
        user_id: Any,
        query: str | None = None,
        period_start: date | None = None,
        period_end: date | None = None,
        min_amount: float | None = None,
        max_amount: float | None = None,
        category: str | None = None,
        direction: Literal["expense", "income"] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[GeneratedQuery, GeneratedQuery]:
        """Search transactions. Returns (data_query, count_query)."""

        def build_one_data(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []

            self._add_user_filter(table, where, params, user_id)

            if period_start is not None:
                date_col = self._resolve_col(table, "date")
                params.append(period_start)
                where.append(f"{date_col} >= ${len(params)}")
            if period_end is not None:
                date_col = self._resolve_col(table, "date")
                params.append(period_end)
                where.append(f"{date_col} <= ${len(params)}")

            if query is not None:
                desc_col = self._resolve_col(table, "description")
                params.append(f"%{query}%")
                where.append(f"{desc_col} ILIKE ${len(params)}")

            if min_amount is not None:
                amt_col = self._resolve_col(table, "amount")
                params.append(min_amount)
                where.append(f"ABS({amt_col}) >= ${len(params)}")
            if max_amount is not None:
                amt_col = self._resolve_col(table, "amount")
                params.append(max_amount)
                where.append(f"ABS({amt_col}) <= ${len(params)}")

            if category is not None:
                cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
                params.append(category)
                where.append(f"{cat_name} = ${len(params)}")

            self._add_direction_filter(direction, table, where, params)

            date_col = self._resolve_col(table, "date")
            amt_col = self._resolve_col(table, "amount")
            desc_col = self._resolve_col(table, "description")
            currency = self._resolve_col(table, "currency")
            cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"

            select = (
                f"{date_col} AS date, "
                f"{amt_col} AS amount, "
                f"{desc_col} AS description, "
                f"{currency} AS currency, "
                f"{cat_name} AS category"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = f"SELECT {select} FROM {table.table} {joins} WHERE {where_sql} ORDER BY {date_col} DESC"

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        def build_one_count(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []

            self._add_user_filter(table, where, params, user_id)

            if period_start is not None:
                date_col = self._resolve_col(table, "date")
                params.append(period_start)
                where.append(f"{date_col} >= ${len(params)}")
            if period_end is not None:
                date_col = self._resolve_col(table, "date")
                params.append(period_end)
                where.append(f"{date_col} <= ${len(params)}")

            if query is not None:
                desc_col = self._resolve_col(table, "description")
                params.append(f"%{query}%")
                where.append(f"{desc_col} ILIKE ${len(params)}")

            if min_amount is not None:
                amt_col = self._resolve_col(table, "amount")
                params.append(min_amount)
                where.append(f"ABS({amt_col}) >= ${len(params)}")
            if max_amount is not None:
                amt_col = self._resolve_col(table, "amount")
                params.append(max_amount)
                where.append(f"ABS({amt_col}) <= ${len(params)}")

            if category is not None:
                cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
                params.append(category)
                where.append(f"{cat_name} = ${len(params)}")

            self._add_direction_filter(direction, table, where, params)

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = f"SELECT COUNT(*) AS total_count FROM {table.table} {joins} WHERE {where_sql}"

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        data_q = self._build_union_query(user_id=user_id, build_one=build_one_data)
        count_q = self._build_union_query(user_id=user_id, build_one=build_one_count)

        tables = self._transaction_tables()
        if len(tables) == 1:
            data_q = GeneratedQuery(
                sql=f"{data_q.sql} LIMIT {limit} OFFSET {offset}",
                params=data_q.params,
            )
        else:
            data_q = GeneratedQuery(
                sql=(f"SELECT * FROM ({data_q.sql}) AS combined ORDER BY date DESC LIMIT {limit} OFFSET {offset}"),
                params=data_q.params,
            )
            count_q = GeneratedQuery(
                sql=f"SELECT SUM(total_count)::int AS total_count FROM ({count_q.sql}) AS combined",
                params=count_q.params,
            )

        return data_q, count_q

    def build_recurring_expenses(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        min_occurrences: int = 3,
    ) -> GeneratedQuery:
        """Find recurring expenses. SQL returns raw groups; Python post-processes."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)

            desc_col = self._resolve_col(table, "description")
            date_col = self._resolve_col(table, "date")
            amt_col = self._resolve_col(table, "amount")
            currency = self._resolve_col(table, "currency")

            abs_amt = f"ABS({amt_col})" if conv.sign_means_expense == "negative" else amt_col

            params.append(min_occurrences)
            min_occ_idx = len(params)

            select = (
                f"LOWER(TRIM({desc_col})) AS merchant_name, "
                f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {abs_amt}) AS estimated_amount, "
                f"COUNT(*) AS occurrences, "
                f"SUM({abs_amt}) AS total_amount, "
                f"ARRAY_AGG({date_col} ORDER BY {date_col}) AS dates, "
                f"{currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = (
                f"SELECT {select} FROM {table.table} {joins} "
                f"WHERE {where_sql} "
                f"GROUP BY LOWER(TRIM({desc_col})), {currency} "
                f"HAVING COUNT(*) >= ${min_occ_idx} "
                f"ORDER BY total_amount DESC"
            )

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        return self._build_union_query(user_id=user_id, build_one=build_one)


def _renumber_params(sql: str, offset: int) -> str:
    """Renumber $1, $2, ... to $(1+offset), $(2+offset), ... for UNION ALL."""

    def replace(match: re.Match[str]) -> str:
        num = int(match.group(1))
        return f"${num + offset}"

    return re.sub(r"\$(\d+)", replace, sql)
