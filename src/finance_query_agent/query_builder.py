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

    # ── Helpers: column resolution ──────────────────────────────────

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

    # ── Helpers: AmountConvention ────────────────────────────────────

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

    # ── Helpers: building blocks ────────────────────────────────────

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

    # ── Helpers: UNION ALL across transaction tables ────────────────

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

    # ── Tool queries ────────────────────────────────────────────────

    def build_spending_by_category(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        categories: list[str] | None = None,
        account_id: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.1: Total spending per category within a time period."""
        currency_col_key = "currency"

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)
            self._add_account_filter(table, where, params, account_id)

            cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"

            if categories is not None:
                params.append(categories)
                where.append(f"{cat_name} = ANY(${len(params)})")

            currency = self._resolve_col(table, currency_col_key)
            sum_expr = self._sum_amount_expr(conv, table)

            select = (
                f"COALESCE({cat_name}, 'Uncategorized') AS category, "
                f"{sum_expr} AS total_amount, "
                f"COUNT(*) AS transaction_count, "
                f"{currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = (
                f"SELECT {select} FROM {table.table} {joins} "
                f"WHERE {where_sql} "
                f"GROUP BY category, {currency} "
                f"ORDER BY total_amount DESC"
            )

            # Renumber params for UNION ALL offset
            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        result = self._build_union_query(user_id=user_id, build_one=build_one)

        # For UNION ALL, re-aggregate across both tables
        if len(self._transaction_tables()) > 1:
            result = GeneratedQuery(
                sql=(
                    f"SELECT category, SUM(total_amount) AS total_amount, "
                    f"SUM(transaction_count)::int AS transaction_count, currency "
                    f"FROM ({result.sql}) AS combined "
                    f"GROUP BY category, currency "
                    f"ORDER BY total_amount DESC"
                ),
                params=result.params,
            )

        return result

    def build_monthly_totals(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        account_id: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.2: Aggregated expense totals per month."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)
            self._add_account_filter(table, where, params, account_id)

            date_col = self._resolve_col(table, "date")
            currency = self._resolve_col(table, "currency")
            sum_expr = self._sum_amount_expr(conv, table)

            select = (
                f"EXTRACT(YEAR FROM {date_col})::int AS year, "
                f"EXTRACT(MONTH FROM {date_col})::int AS month, "
                f"{sum_expr} AS total_amount, "
                f"COUNT(*) AS transaction_count, "
                f"{currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = (
                f"SELECT {select} FROM {table.table} {joins} "
                f"WHERE {where_sql} "
                f"GROUP BY year, month, {currency} "
                f"ORDER BY year, month"
            )

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        result = self._build_union_query(user_id=user_id, build_one=build_one)

        # For UNION ALL, re-aggregate across both tables
        if len(self._transaction_tables()) > 1:
            result = GeneratedQuery(
                sql=(
                    f"SELECT year, month, SUM(total_amount) AS total_amount, "
                    f"SUM(transaction_count)::int AS transaction_count, currency "
                    f"FROM ({result.sql}) AS combined "
                    f"GROUP BY year, month, currency "
                    f"ORDER BY year, month"
                ),
                params=result.params,
            )

        return result

    def build_top_merchants(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        limit: int = 10,
        category: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.3: Top merchants by spending."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)

            if category is not None:
                cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
                params.append(category)
                where.append(f"{cat_name} = ${len(params)}")

            desc_col = self._resolve_col(table, "description")
            currency = self._resolve_col(table, "currency")
            sum_expr = self._sum_amount_expr(conv, table)

            select = (
                f"{desc_col} AS merchant_name, "
                f"{sum_expr} AS total_amount, "
                f"COUNT(*) AS transaction_count, "
                f"{currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = (
                f"SELECT {select} FROM {table.table} {joins} "
                f"WHERE {where_sql} "
                f"GROUP BY {desc_col}, {currency} "
                f"ORDER BY total_amount DESC"
            )

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        result = self._build_union_query(user_id=user_id, build_one=build_one)

        if len(self._transaction_tables()) == 1:
            result = GeneratedQuery(sql=f"{result.sql} LIMIT {limit}", params=result.params)
        else:
            result = GeneratedQuery(
                sql=(
                    f"SELECT merchant_name, SUM(total_amount) AS total_amount, "
                    f"SUM(transaction_count)::int AS transaction_count, currency "
                    f"FROM ({result.sql}) AS combined "
                    f"GROUP BY merchant_name, currency "
                    f"ORDER BY total_amount DESC LIMIT {limit}"
                ),
                params=result.params,
            )

        return result

    def build_compare_periods(
        self,
        user_id: Any,
        period_a_start: date,
        period_a_end: date,
        period_b_start: date,
        period_b_end: date,
        group_by: Literal["category", "merchant", "total"] = "total",
    ) -> GeneratedQuery:
        """Spec 7.4: Compare spending between two time periods."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_expense_filter(table, where, params)

            date_col = self._resolve_col(table, "date")
            currency = self._resolve_col(table, "currency")

            # Filter to rows in either period
            params.append(period_a_start)
            a_start_idx = len(params)
            params.append(period_a_end)
            a_end_idx = len(params)
            params.append(period_b_start)
            b_start_idx = len(params)
            params.append(period_b_end)
            b_end_idx = len(params)

            where.append(
                f"(({date_col} >= ${a_start_idx} AND {date_col} <= ${a_end_idx}) "
                f"OR ({date_col} >= ${b_start_idx} AND {date_col} <= ${b_end_idx}))"
            )

            # Build the group_label and GROUP BY
            amt_col = self._resolve_col(table, "amount")
            abs_amt = f"ABS({amt_col})" if conv.sign_means_expense == "negative" else amt_col

            if group_by == "category":
                cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
                group_label = f"COALESCE({cat_name}, 'Uncategorized')"
                group_clause = f"{group_label}, {currency}"
            elif group_by == "merchant":
                desc_col = self._resolve_col(table, "description")
                group_label = desc_col
                group_clause = f"{desc_col}, {currency}"
            else:  # total
                group_label = "'Total'"
                group_clause = currency

            select = (
                f"{group_label} AS group_label, "
                f"{currency} AS currency, "
                f"SUM(CASE WHEN {date_col} >= ${a_start_idx} AND {date_col} <= ${a_end_idx} "
                f"THEN {abs_amt} ELSE 0 END) AS period_a_total, "
                f"SUM(CASE WHEN {date_col} >= ${b_start_idx} AND {date_col} <= ${b_end_idx} "
                f"THEN {abs_amt} ELSE 0 END) AS period_b_total"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = f"SELECT {select} FROM {table.table} {joins} WHERE {where_sql} GROUP BY {group_clause}"

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        result = self._build_union_query(user_id=user_id, build_one=build_one)

        # For UNION ALL, re-aggregate across both tables
        if len(self._transaction_tables()) > 1:
            result = GeneratedQuery(
                sql=(
                    f"SELECT group_label, currency, "
                    f"SUM(period_a_total) AS period_a_total, "
                    f"SUM(period_b_total) AS period_b_total "
                    f"FROM ({result.sql}) AS combined "
                    f"GROUP BY group_label, currency"
                ),
                params=result.params,
            )

        return result

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
        """Spec 7.5: Search transactions. Returns (data_query, count_query)."""

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

    def build_category_breakdown(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        account_id: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.6: Percentage breakdown of spending by category."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)
            self._add_account_filter(table, where, params, account_id)

            cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
            currency = self._resolve_col(table, "currency")
            sum_expr = self._sum_amount_expr(conv, table)

            select = (
                f"COALESCE({cat_name}, 'Uncategorized') AS category, {sum_expr} AS total_amount, {currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = f"SELECT {select} FROM {table.table} {joins} WHERE {where_sql} GROUP BY category, {currency}"

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        # Wrap in outer query to compute percentages
        inner = self._build_union_query(user_id=user_id, build_one=build_one)
        sql = (
            f"SELECT category, total_amount, "
            f"ROUND(total_amount * 100.0 / SUM(total_amount) OVER (PARTITION BY currency), 2) AS percentage, "
            f"currency "
            f"FROM ({inner.sql}) AS breakdown "
            f"ORDER BY total_amount DESC"
        )
        return GeneratedQuery(sql=sql, params=inner.params)

    def build_spending_trend(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        granularity: Literal["week", "month"] = "month",
        category: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.7: Spending over time bucketed by week or month."""

        def build_one(table: TableMapping, param_offset: int) -> tuple[str, list[Any]]:
            params: list[Any] = []
            where: list[str] = []
            conv = table.amount_convention
            assert conv is not None

            self._add_user_filter(table, where, params, user_id)
            self._add_date_range(table, where, params, period_start, period_end)
            self._add_expense_filter(table, where, params)

            if category is not None:
                cat_name = f"{self._schema.categories.table}.{self._schema.categories.columns['name']}"
                params.append(category)
                where.append(f"{cat_name} = ${len(params)}")

            date_col = self._resolve_col(table, "date")
            currency = self._resolve_col(table, "currency")
            sum_expr = self._sum_amount_expr(conv, table)

            if granularity == "week":
                trunc = f"DATE_TRUNC('week', {date_col})::date"
                label = f"TO_CHAR(DATE_TRUNC('week', {date_col}), 'IYYY-\"W\"IW')"
            else:
                trunc = f"DATE_TRUNC('month', {date_col})::date"
                label = f"TO_CHAR(DATE_TRUNC('month', {date_col}), 'YYYY-MM')"

            select = (
                f"{label} AS period_label, "
                f"{sum_expr} AS total_amount, "
                f"COUNT(*) AS transaction_count, "
                f"{currency} AS currency"
            )

            joins = self._joins_sql(table)
            where_sql = " AND ".join(where)
            sql = (
                f"SELECT {select} FROM {table.table} {joins} "
                f"WHERE {where_sql} "
                f"GROUP BY period_label, {trunc}, {currency} "
                f"ORDER BY {trunc}"
            )

            if param_offset > 0:
                sql = _renumber_params(sql, param_offset)

            return sql, params

        result = self._build_union_query(user_id=user_id, build_one=build_one)

        # For UNION ALL, re-aggregate across both tables
        if len(self._transaction_tables()) > 1:
            result = GeneratedQuery(
                sql=(
                    f"SELECT period_label, SUM(total_amount) AS total_amount, "
                    f"SUM(transaction_count)::int AS transaction_count, currency "
                    f"FROM ({result.sql}) AS combined "
                    f"GROUP BY period_label, currency "
                    f"ORDER BY period_label"
                ),
                params=result.params,
            )

        return result

    def build_balance_summary(
        self,
        user_id: Any,
        account_id: str | None = None,
    ) -> GeneratedQuery:
        """Spec 7.8: Most recent balance per account. Only works if 'balance' is mapped."""
        table = self._schema.transactions
        params: list[Any] = []
        where: list[str] = []

        self._add_user_filter(table, where, params, user_id)
        self._add_account_filter(table, where, params, account_id)

        date_col = self._resolve_col(table, "date")
        acct_col = self._resolve_col(table, "account_id")
        balance_col = self._resolve_col(table, "balance")
        currency = self._resolve_col(table, "currency")

        # Account name from accounts table
        acct_mapping = self._schema.accounts
        acct_name = f"{acct_mapping.table}.{acct_mapping.columns.get('name', 'id')}"

        joins = self._joins_sql(table)
        where_sql = " AND ".join(where)

        sql = (
            f"SELECT DISTINCT ON ({acct_col}) "
            f"{acct_name} AS account_name, "
            f"{balance_col} AS latest_balance, "
            f"{date_col} AS last_transaction_date, "
            f"{currency} AS currency "
            f"FROM {table.table} {joins} "
            f"WHERE {where_sql} "
            f"ORDER BY {acct_col}, {date_col} DESC"
        )

        return GeneratedQuery(sql=sql, params=params)

    def build_recurring_expenses(
        self,
        user_id: Any,
        period_start: date,
        period_end: date,
        min_occurrences: int = 3,
    ) -> GeneratedQuery:
        """Spec 7.9: Find recurring expenses. SQL returns raw groups; Python post-processes."""

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
