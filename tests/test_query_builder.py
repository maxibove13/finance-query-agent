"""Pure unit tests for QueryBuilder — no database needed."""

from __future__ import annotations

from datetime import date

import pytest

from finance_query_agent.query_builder import GeneratedQuery, QueryBuilder, _renumber_params
from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)

# ── Fixtures ──────────────────────────────────────────────────────


def _direction_schema(*, with_secondary: bool = False) -> SchemaMapping:
    """Schema using direction_column AmountConvention."""
    kwargs: dict = {}
    if with_secondary:
        kwargs["secondary_transactions"] = TableMapping(
            table="credit_card_movements",
            columns={
                "date": "issued_at",
                "amount": "amount",
                "description": "description",
                "user_id": ColumnRef(table="credit_cards", column="user_id"),
                "currency": "currency",
                "account_id": "credit_card_id",
            },
            joins=[
                JoinDef(
                    table="credit_cards",
                    on="credit_card_movements.credit_card_id = credit_cards.id",
                    type="inner",
                ),
                JoinDef(table="tags", on="credit_card_movements.category_id = tags.id", type="left"),
            ],
            amount_convention=AmountConvention(
                direction_column="movement_direction",
                expense_value="debit",
                income_value="credit",
            ),
        )

    return SchemaMapping(
        transactions=TableMapping(
            table="account_movements",
            columns={
                "date": "issued_at",
                "amount": "amount",
                "description": "description",
                "user_id": ColumnRef(table="accounts", column="user_id"),
                "currency": ColumnRef(table="accounts", column="currency"),
                "account_id": "account_id",
                "balance": "balance",
            },
            joins=[
                JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
                JoinDef(table="tags", on="account_movements.category_id = tags.id", type="left"),
            ],
            amount_convention=AmountConvention(
                direction_column="movement_direction",
                expense_value="debit",
                income_value="credit",
            ),
        ),
        categories=TableMapping(
            table="tags",
            columns={"id": "id", "name": "name"},
            user_scoped=False,
        ),
        accounts=TableMapping(
            table="accounts",
            columns={"id": "id", "name": "alias", "user_id": "user_id"},
        ),
        **kwargs,
    )


def _sign_schema() -> SchemaMapping:
    """Schema using sign-based AmountConvention (negative = expense)."""
    return SchemaMapping(
        transactions=TableMapping(
            table="txns",
            columns={
                "date": "tx_date",
                "amount": "amount",
                "description": "memo",
                "user_id": "user_id",
                "currency": "currency",
                "account_id": "acct_id",
            },
            joins=[
                JoinDef(table="categories", on="txns.cat_id = categories.id", type="left"),
            ],
            amount_convention=AmountConvention(sign_means_expense="negative"),
        ),
        categories=TableMapping(
            table="categories",
            columns={"id": "id", "name": "name"},
            user_scoped=False,
        ),
        accounts=TableMapping(
            table="accounts",
            columns={"id": "id", "user_id": "user_id"},
        ),
    )


@pytest.fixture
def qb() -> QueryBuilder:
    return QueryBuilder(_direction_schema())


@pytest.fixture
def qb_sign() -> QueryBuilder:
    return QueryBuilder(_sign_schema())


@pytest.fixture
def qb_union() -> QueryBuilder:
    return QueryBuilder(_direction_schema(with_secondary=True))


# ── Utility tests ─────────────────────────────────────────────────


class TestRenumberParams:
    def test_basic(self):
        assert _renumber_params("$1, $2, $3", 3) == "$4, $5, $6"

    def test_no_params(self):
        assert _renumber_params("SELECT 1", 5) == "SELECT 1"

    def test_double_digit(self):
        assert _renumber_params("$10 AND $11", 2) == "$12 AND $13"


# ── Spending by category ──────────────────────────────────────────


class TestBuildSpendingByCategory:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_spending_by_category("user-1", date(2024, 1, 1), date(2024, 1, 31))
        assert isinstance(q, GeneratedQuery)
        assert "user-1" in q.params
        assert date(2024, 1, 1) in q.params
        assert date(2024, 1, 31) in q.params
        # Expense filter with direction column
        assert "movement_direction" in q.sql
        assert "debit" in q.params
        # Must group by currency
        assert "GROUP BY" in q.sql
        assert "currency" in q.sql.lower()
        # Must have category in select
        assert "category" in q.sql.lower()

    def test_with_categories_filter(self, qb: QueryBuilder):
        q = qb.build_spending_by_category(
            "user-1", date(2024, 1, 1), date(2024, 1, 31), categories=["Food", "Transport"]
        )
        assert ["Food", "Transport"] in q.params
        assert "ANY" in q.sql

    def test_with_account_id(self, qb: QueryBuilder):
        q = qb.build_spending_by_category("user-1", date(2024, 1, 1), date(2024, 1, 31), account_id="acct-99")
        assert "acct-99" in q.params
        assert "account_id" in q.sql

    def test_sign_convention(self, qb_sign: QueryBuilder):
        q = qb_sign.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 12, 31))
        # Sign-based: expense filter is amount < 0
        assert "amount < 0" in q.sql or "amount> 0" in q.sql or "txns.amount < 0" in q.sql
        # SUM should use ABS for negative convention
        assert "ABS" in q.sql

    def test_union_all(self, qb_union: QueryBuilder):
        q = qb_union.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "UNION ALL" in q.sql
        # Both tables referenced
        assert "account_movements" in q.sql
        assert "credit_card_movements" in q.sql


# ── Monthly totals ────────────────────────────────────────────────


class TestBuildMonthlyTotals:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_monthly_totals("user-1", date(2024, 1, 1), date(2024, 6, 30))
        assert "EXTRACT(YEAR" in q.sql
        assert "EXTRACT(MONTH" in q.sql
        assert "GROUP BY" in q.sql
        assert "user-1" in q.params
        assert "ORDER BY year, month" in q.sql

    def test_with_account_id(self, qb: QueryBuilder):
        q = qb.build_monthly_totals("u1", date(2024, 1, 1), date(2024, 6, 30), account_id="a1")
        assert "a1" in q.params

    def test_has_expense_filter(self, qb: QueryBuilder):
        q = qb.build_monthly_totals("u1", date(2024, 1, 1), date(2024, 6, 30))
        assert "debit" in q.params

    def test_direction_value_passed_verbatim(self):
        """Expense/income values from config are passed as-is -- config must match the DB."""
        schema = SchemaMapping(
            transactions=TableMapping(
                table="movements",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": "user_id",
                    "currency": "currency",
                    "account_id": "account_id",
                },
                joins=[JoinDef(table="tags", on="movements.category_id = tags.id", type="left")],
                amount_convention=AmountConvention(
                    direction_column="movement_direction",
                    expense_value="DEBIT",
                    income_value="CREDIT",
                ),
            ),
            categories=TableMapping(table="tags", columns={"id": "id", "name": "name"}, user_scoped=False),
            accounts=TableMapping(table="accounts", columns={"id": "id", "user_id": "user_id"}),
        )
        qb = QueryBuilder(schema)
        q = qb.build_monthly_totals("u1", date(2024, 1, 1), date(2024, 6, 30))
        assert "DEBIT" in q.params
        assert "movements.movement_direction::text" in q.sql


# ── Top merchants ─────────────────────────────────────────────────


class TestBuildTopMerchants:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_top_merchants("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "merchant_name" in q.sql.lower() or "description" in q.sql.lower()
        assert "LIMIT 10" in q.sql
        assert "ORDER BY total_amount DESC" in q.sql

    def test_custom_limit(self, qb: QueryBuilder):
        q = qb.build_top_merchants("u1", date(2024, 1, 1), date(2024, 12, 31), limit=25)
        assert "LIMIT 25" in q.sql

    def test_with_category(self, qb: QueryBuilder):
        q = qb.build_top_merchants("u1", date(2024, 1, 1), date(2024, 12, 31), category="Food")
        assert "Food" in q.params


# ── Compare periods ───────────────────────────────────────────────


class TestBuildComparePeriods:
    def test_total(self, qb: QueryBuilder):
        q = qb.build_compare_periods(
            "u1",
            date(2024, 1, 1),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 28),
        )
        assert "period_a_total" in q.sql
        assert "period_b_total" in q.sql
        assert "CASE WHEN" in q.sql
        # All four dates in params
        assert date(2024, 1, 1) in q.params
        assert date(2024, 2, 28) in q.params

    def test_group_by_category(self, qb: QueryBuilder):
        q = qb.build_compare_periods(
            "u1",
            date(2024, 1, 1),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 28),
            group_by="category",
        )
        assert "Uncategorized" in q.sql
        assert "group_label" in q.sql.lower()

    def test_group_by_merchant(self, qb: QueryBuilder):
        q = qb.build_compare_periods(
            "u1",
            date(2024, 1, 1),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 28),
            group_by="merchant",
        )
        assert "description" in q.sql.lower()


# ── Search transactions ───────────────────────────────────────────


class TestBuildSearchTransactions:
    def test_basic(self, qb: QueryBuilder):
        data_q, count_q = qb.build_search_transactions("u1")
        assert "LIMIT 20" in data_q.sql
        assert "OFFSET 0" in data_q.sql
        assert "COUNT(*)" in count_q.sql
        assert "u1" in data_q.params
        assert "u1" in count_q.params

    def test_text_search(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", query="Netflix")
        assert "ILIKE" in data_q.sql
        assert "%Netflix%" in data_q.params

    def test_amount_range(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", min_amount=10.0, max_amount=100.0)
        assert 10.0 in data_q.params
        assert 100.0 in data_q.params
        assert "ABS(" in data_q.sql

    def test_direction_filter_expense(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", direction="expense")
        assert "debit" in data_q.params

    def test_direction_filter_income(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", direction="income")
        assert "credit" in data_q.params

    def test_no_direction_filter(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1")
        assert "debit" not in data_q.params
        assert "credit" not in data_q.params

    def test_pagination(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", limit=50, offset=100)
        assert "LIMIT 50" in data_q.sql
        assert "OFFSET 100" in data_q.sql

    def test_category_filter(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", category="Food")
        assert "Food" in data_q.params

    def test_union_all_wraps(self, qb_union: QueryBuilder):
        data_q, count_q = qb_union.build_search_transactions("u1")
        assert "UNION ALL" in data_q.sql
        # Outer wrapper for ordering
        assert "combined" in data_q.sql
        assert "SUM(total_count)" in count_q.sql


# ── Category breakdown ────────────────────────────────────────────


class TestBuildCategoryBreakdown:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_category_breakdown("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "percentage" in q.sql.lower()
        assert "OVER (PARTITION BY currency)" in q.sql
        assert "Uncategorized" in q.sql
        assert "breakdown" in q.sql

    def test_with_account_id(self, qb: QueryBuilder):
        q = qb.build_category_breakdown("u1", date(2024, 1, 1), date(2024, 12, 31), account_id="a1")
        assert "a1" in q.params


# ── Spending trend ────────────────────────────────────────────────


class TestBuildSpendingTrend:
    def test_monthly(self, qb: QueryBuilder):
        q = qb.build_spending_trend("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "DATE_TRUNC('month'" in q.sql
        assert "period_label" in q.sql
        assert "YYYY-MM" in q.sql

    def test_weekly(self, qb: QueryBuilder):
        q = qb.build_spending_trend("u1", date(2024, 1, 1), date(2024, 3, 31), granularity="week")
        assert "DATE_TRUNC('week'" in q.sql
        assert "IW" in q.sql  # ISO week format

    def test_with_category(self, qb: QueryBuilder):
        q = qb.build_spending_trend("u1", date(2024, 1, 1), date(2024, 12, 31), category="Food")
        assert "Food" in q.params


# ── Balance summary ───────────────────────────────────────────────


class TestBuildBalanceSummary:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_balance_summary("u1")
        assert "DISTINCT ON" in q.sql
        assert "balance" in q.sql.lower()
        assert "u1" in q.params
        # Only primary transactions (no UNION ALL)
        assert "UNION ALL" not in q.sql

    def test_with_account_id(self, qb: QueryBuilder):
        q = qb.build_balance_summary("u1", account_id="a1")
        assert "a1" in q.params

    def test_account_name(self, qb: QueryBuilder):
        q = qb.build_balance_summary("u1")
        assert "account_name" in q.sql.lower() or "alias" in q.sql.lower()


# ── Recurring expenses ────────────────────────────────────────────


class TestBuildRecurringExpenses:
    def test_basic(self, qb: QueryBuilder):
        q = qb.build_recurring_expenses("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "LOWER(TRIM(" in q.sql
        assert "PERCENTILE_CONT(0.5)" in q.sql
        assert "ARRAY_AGG(" in q.sql
        assert "HAVING COUNT(*)" in q.sql
        assert 3 in q.params  # default min_occurrences

    def test_custom_min_occurrences(self, qb: QueryBuilder):
        q = qb.build_recurring_expenses("u1", date(2024, 1, 1), date(2024, 12, 31), min_occurrences=5)
        assert 5 in q.params

    def test_expense_filter(self, qb: QueryBuilder):
        q = qb.build_recurring_expenses("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "debit" in q.params

    def test_sign_convention_abs(self, qb_sign: QueryBuilder):
        q = qb_sign.build_recurring_expenses("u1", date(2024, 1, 1), date(2024, 12, 31))
        assert "ABS(" in q.sql


# ── Cross-cutting: user_id always present ─────────────────────────


class TestUserIdAlwaysInjected:
    """Every query method must include user_id in params."""

    def test_spending_by_category(self, qb: QueryBuilder):
        q = qb.build_spending_by_category("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params

    def test_monthly_totals(self, qb: QueryBuilder):
        q = qb.build_monthly_totals("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params

    def test_top_merchants(self, qb: QueryBuilder):
        q = qb.build_top_merchants("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params

    def test_compare_periods(self, qb: QueryBuilder):
        q = qb.build_compare_periods("uid", date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 28))
        assert "uid" in q.params

    def test_search_transactions(self, qb: QueryBuilder):
        dq, cq = qb.build_search_transactions("uid")
        assert "uid" in dq.params
        assert "uid" in cq.params

    def test_category_breakdown(self, qb: QueryBuilder):
        q = qb.build_category_breakdown("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params

    def test_spending_trend(self, qb: QueryBuilder):
        q = qb.build_spending_trend("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params

    def test_balance_summary(self, qb: QueryBuilder):
        q = qb.build_balance_summary("uid")
        assert "uid" in q.params

    def test_recurring_expenses(self, qb: QueryBuilder):
        q = qb.build_recurring_expenses("uid", date(2024, 1, 1), date(2024, 1, 31))
        assert "uid" in q.params


# ── Param numbering correctness ───────────────────────────────────


class TestParamNumbering:
    """Verify $N placeholders match actual param indices."""

    def _extract_param_indices(self, sql: str) -> list[int]:
        import re

        return sorted(set(int(m) for m in re.findall(r"\$(\d+)", sql)))

    def test_spending_params_sequential(self, qb: QueryBuilder):
        q = qb.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 1, 31))
        indices = self._extract_param_indices(q.sql)
        assert indices == list(range(1, len(q.params) + 1))

    def test_search_params_sequential(self, qb: QueryBuilder):
        dq, cq = qb.build_search_transactions(
            "u1",
            query="test",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 12, 31),
            min_amount=5.0,
            direction="expense",
        )
        d_indices = self._extract_param_indices(dq.sql)
        assert d_indices == list(range(1, len(dq.params) + 1))
        c_indices = self._extract_param_indices(cq.sql)
        assert c_indices == list(range(1, len(cq.params) + 1))

    def test_union_params_sequential(self, qb_union: QueryBuilder):
        q = qb_union.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 12, 31))
        indices = self._extract_param_indices(q.sql)
        assert indices == list(range(1, len(q.params) + 1))

    def test_compare_periods_params(self, qb: QueryBuilder):
        q = qb.build_compare_periods(
            "u1",
            date(2024, 1, 1),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 28),
        )
        indices = self._extract_param_indices(q.sql)
        assert indices == list(range(1, len(q.params) + 1))

    def test_recurring_params(self, qb: QueryBuilder):
        q = qb.build_recurring_expenses("u1", date(2024, 1, 1), date(2024, 12, 31))
        indices = self._extract_param_indices(q.sql)
        assert indices == list(range(1, len(q.params) + 1))


# ── ColumnRef resolution ──────────────────────────────────────────


class TestColumnRefResolution:
    def test_user_id_via_columnref(self, qb: QueryBuilder):
        """user_id is on accounts table via ColumnRef — query should reference accounts.user_id."""
        q = qb.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 1, 31))
        assert "accounts.user_id" in q.sql

    def test_currency_via_columnref(self, qb: QueryBuilder):
        """currency is on accounts table via ColumnRef."""
        q = qb.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 1, 31))
        assert "accounts.currency" in q.sql

    def test_direct_column(self, qb_sign: QueryBuilder):
        """Direct columns resolve to table.column."""
        q = qb_sign.build_spending_by_category("u1", date(2024, 1, 1), date(2024, 1, 31))
        assert "txns.user_id" in q.sql
        assert "txns.currency" in q.sql


# ── Double-limiting prevention ───────────────────────────────────


class TestDoubleLimitingPrevention:
    """Verify LIMIT appears exactly once in generated SQL, even with UNION ALL."""

    def test_top_merchants_single_table_has_one_limit(self, qb: QueryBuilder):
        q = qb.build_top_merchants("u1", date(2024, 1, 1), date(2024, 12, 31), limit=10)
        assert q.sql.upper().count("LIMIT") == 1
        assert "LIMIT 10" in q.sql

    def test_top_merchants_union_has_one_limit(self, qb_union: QueryBuilder):
        q = qb_union.build_top_merchants("u1", date(2024, 1, 1), date(2024, 12, 31), limit=10)
        assert "UNION ALL" in q.sql
        assert q.sql.upper().count("LIMIT") == 1
        assert "LIMIT 10" in q.sql

    def test_search_transactions_single_table_has_one_limit(self, qb: QueryBuilder):
        data_q, _ = qb.build_search_transactions("u1", limit=20, offset=0)
        assert data_q.sql.upper().count("LIMIT") == 1
        assert "LIMIT 20" in data_q.sql

    def test_search_transactions_union_has_one_limit(self, qb_union: QueryBuilder):
        data_q, _ = qb_union.build_search_transactions("u1", limit=20, offset=0)
        assert "UNION ALL" in data_q.sql
        assert data_q.sql.upper().count("LIMIT") == 1
        assert "LIMIT 20" in data_q.sql
