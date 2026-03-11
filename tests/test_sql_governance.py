"""Unit tests for sql_governance.validate_select_only (no DB needed)."""

from __future__ import annotations

import pytest

from finance_query_agent.sql_governance import validate_select_only


class TestValidSelectOnly:
    def test_simple_select_passes(self) -> None:
        # No FROM → scalar, naturally bounded, no LIMIT needed
        validate_select_only("SELECT 1")

    def test_select_with_where_passes(self) -> None:
        validate_select_only("SELECT id, amount FROM account_movements WHERE movement_direction = 'debit' LIMIT 100")

    def test_select_with_join_passes(self) -> None:
        validate_select_only(
            "SELECT am.amount, a.currency FROM account_movements am JOIN accounts a ON am.account_id = a.id LIMIT 100"
        )

    def test_select_with_cte_passes(self) -> None:
        validate_select_only(
            "WITH monthly AS (SELECT DATE_TRUNC('month', issued_at) AS m, SUM(amount) AS total "
            "FROM account_movements GROUP BY 1) SELECT * FROM monthly LIMIT 100"
        )

    def test_select_with_window_function_passes(self) -> None:
        validate_select_only(
            "SELECT description, amount, "
            "ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY issued_at DESC) AS rn "
            "FROM account_movements LIMIT 100"
        )

    def test_select_with_having_passes(self) -> None:
        validate_select_only(
            "SELECT account_id, SUM(amount) AS total "
            "FROM account_movements "
            "GROUP BY account_id "
            "HAVING SUM(amount) > 100 "
            "LIMIT 100"
        )

    def test_aggregate_without_group_by_passes_without_limit(self) -> None:
        # SUM() without GROUP BY always returns exactly one row — LIMIT not required
        validate_select_only("SELECT SUM(amount) FROM account_movements")

    def test_aggregate_with_group_by_requires_limit(self) -> None:
        # GROUP BY can return many rows — LIMIT required
        with pytest.raises(ValueError, match="(?i)limit"):
            validate_select_only("SELECT account_id, SUM(amount) FROM account_movements GROUP BY account_id")

    def test_union_all_passes(self) -> None:
        validate_select_only(
            "SELECT amount, 'bank' AS source FROM account_movements WHERE movement_direction = 'debit' "
            "UNION ALL "
            "SELECT amount, 'card' AS source FROM credit_card_movements "
            "LIMIT 100"
        )

    def test_union_all_without_limit_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)limit"):
            validate_select_only(
                "SELECT amount FROM account_movements UNION ALL SELECT amount FROM credit_card_movements"
            )

    def test_set_config_raises(self) -> None:
        with pytest.raises(ValueError, match="set_config"):
            validate_select_only(
                "WITH s AS (SELECT set_config('app.user_id', '2', true)) SELECT * FROM accounts LIMIT 10"
            )

    def test_insert_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)insert"):
            validate_select_only("INSERT INTO accounts (user_id, currency) VALUES (1, 'USD')")

    def test_update_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)update"):
            validate_select_only("UPDATE accounts SET currency = 'EUR' WHERE id = 1")

    def test_delete_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)delete"):
            validate_select_only("DELETE FROM account_movements WHERE id = 1")

    def test_drop_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)drop"):
            validate_select_only("DROP TABLE accounts")

    def test_create_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)create"):
            validate_select_only("CREATE TABLE foo (id SERIAL)")

    def test_invalid_sql_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_select_only("THIS IS NOT SQL %%%")

    def test_cross_join_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)cross join"):
            validate_select_only("SELECT * FROM accounts CROSS JOIN account_movements LIMIT 100")

    def test_comma_join_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)comma join"):
            validate_select_only("SELECT * FROM accounts, account_movements LIMIT 100")

    def test_select_without_limit_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)limit"):
            validate_select_only("SELECT * FROM account_movements")

    def test_select_with_limit_passes(self) -> None:
        validate_select_only("SELECT * FROM account_movements LIMIT 100")
