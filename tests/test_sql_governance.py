"""Unit tests for sql_governance (no DB needed)."""

from __future__ import annotations

import pytest

from finance_query_agent.sql_governance import cap_limit, validate_allowed_tables, validate_select_only


class TestValidSelectOnly:
    def test_simple_select_passes(self) -> None:
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
        validate_select_only("SELECT SUM(amount) FROM account_movements")

    def test_union_all_passes(self) -> None:
        validate_select_only(
            "SELECT amount, 'bank' AS source FROM account_movements WHERE movement_direction = 'debit' "
            "UNION ALL "
            "SELECT amount, 'card' AS source FROM credit_card_movements "
            "LIMIT 100"
        )

    def test_select_without_limit_passes(self) -> None:
        # LIMIT is enforced by cap_limit, not validate_select_only
        validate_select_only("SELECT * FROM account_movements")

    def test_set_config_raises(self) -> None:
        with pytest.raises(ValueError, match="set_config"):
            validate_select_only(
                "WITH s AS (SELECT set_config('app.user_id', '2', true)) SELECT * FROM accounts LIMIT 10"
            )

    def test_set_config_schema_qualified_raises(self) -> None:
        with pytest.raises(ValueError, match="set_config"):
            validate_select_only("SELECT pg_catalog.set_config('app.user_id', '99', true), id FROM accounts LIMIT 10")

    def test_set_config_inline_where_raises(self) -> None:
        with pytest.raises(ValueError, match="set_config"):
            validate_select_only(
                "SELECT * FROM accounts WHERE set_config('app.user_id', '99', true) IS NOT NULL LIMIT 10"
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

    def test_intersect_passes(self) -> None:
        validate_select_only("SELECT id FROM accounts INTERSECT SELECT id FROM accounts LIMIT 10")

    def test_except_passes(self) -> None:
        validate_select_only("SELECT id FROM accounts EXCEPT SELECT id FROM accounts LIMIT 10")

    def test_select_into_raises(self) -> None:
        with pytest.raises(ValueError, match="(?i)into"):
            validate_select_only("SELECT * INTO new_table FROM accounts")


class TestValidateAllowedTables:
    def test_allowed_tables_passes_valid_query(self) -> None:
        validate_allowed_tables(
            "SELECT id, amount FROM accounts WHERE id = 1",
            frozenset({"accounts"}),
        )

    def test_allowed_tables_rejects_pg_catalog(self) -> None:
        with pytest.raises(ValueError, match="not in the schema"):
            validate_allowed_tables(
                "SELECT * FROM pg_catalog.pg_tables",
                frozenset({"accounts"}),
            )

    def test_allowed_tables_rejects_information_schema(self) -> None:
        with pytest.raises(ValueError, match="not in the schema"):
            validate_allowed_tables(
                "SELECT * FROM information_schema.columns",
                frozenset({"accounts"}),
            )

    def test_allowed_tables_cte_alias_not_rejected(self) -> None:
        validate_allowed_tables(
            "WITH monthly AS (SELECT id FROM accounts) SELECT * FROM monthly",
            frozenset({"accounts"}),
        )

    def test_allowed_tables_rejects_unknown_table(self) -> None:
        with pytest.raises(ValueError, match="not in the schema.*secret_data"):
            validate_allowed_tables(
                "SELECT * FROM secret_data",
                frozenset({"accounts"}),
            )

    def test_allowed_tables_join(self) -> None:
        validate_allowed_tables(
            "SELECT a.id FROM accounts a JOIN movements m ON a.id = m.account_id",
            frozenset({"accounts", "movements"}),
        )

    def test_allowed_tables_rejects_schema_qualified_non_public(self) -> None:
        with pytest.raises(ValueError, match="not in the schema"):
            validate_allowed_tables(
                "SELECT * FROM analytics.accounts",
                frozenset({"accounts"}),
            )

    def test_allowed_tables_allows_public_schema(self) -> None:
        validate_allowed_tables(
            "SELECT * FROM public.accounts",
            frozenset({"accounts"}),
        )

    def test_allowed_tables_rejects_cte_shadowing_disallowed_table(self) -> None:
        """CTE alias matching a real table must not hide the underlying table."""
        with pytest.raises(ValueError, match="not in the schema"):
            validate_allowed_tables(
                "WITH secret AS (SELECT * FROM secret) SELECT * FROM secret",
                frozenset({"accounts"}),
            )

    def test_allowed_tables_rejects_cte_shadowing_pg_catalog(self) -> None:
        with pytest.raises(ValueError, match="not in the schema"):
            validate_allowed_tables(
                "WITH pg_tables AS (SELECT * FROM pg_catalog.pg_tables) SELECT * FROM pg_tables",
                frozenset({"accounts"}),
            )


class TestCapLimit:
    def test_adds_limit_when_missing(self) -> None:
        result = cap_limit("SELECT * FROM account_movements")
        assert "LIMIT 200" in result.upper()

    def test_keeps_limit_within_bounds(self) -> None:
        result = cap_limit("SELECT * FROM account_movements LIMIT 50")
        assert "LIMIT 50" in result.upper()
        assert "200" not in result

    def test_caps_limit_over_max(self) -> None:
        result = cap_limit("SELECT * FROM account_movements LIMIT 300")
        assert "LIMIT 200" in result.upper()

    def test_caps_limit_all(self) -> None:
        result = cap_limit("SELECT * FROM account_movements LIMIT ALL")
        assert "LIMIT 200" in result.upper()

    def test_caps_expression_limit(self) -> None:
        result = cap_limit("SELECT * FROM account_movements LIMIT 200 + 1")
        assert "LIMIT 200" in result.upper()

    def test_caps_cast_limit(self) -> None:
        result = cap_limit("SELECT * FROM account_movements LIMIT CAST(10 AS INT)")
        assert "LIMIT 200" in result.upper()

    def test_no_limit_added_to_scalar(self) -> None:
        result = cap_limit("SELECT 1")
        assert "LIMIT" not in result.upper()

    def test_no_limit_added_to_ungrouped_aggregate(self) -> None:
        result = cap_limit("SELECT SUM(amount) FROM account_movements")
        assert "LIMIT" not in result.upper()

    def test_aggregate_in_where_subquery_gets_capped(self) -> None:
        # Aggregate is inside a WHERE subquery — outer query is not naturally bounded.
        result = cap_limit(
            "SELECT description FROM account_movements WHERE amount > (SELECT AVG(amount) FROM account_movements)"
        )
        assert "LIMIT 200" in result.upper()

    def test_adds_limit_to_union(self) -> None:
        result = cap_limit("SELECT amount FROM account_movements UNION ALL SELECT amount FROM credit_card_movements")
        assert "LIMIT 200" in result.upper()

    def test_caps_union_over_max(self) -> None:
        result = cap_limit(
            "SELECT amount FROM account_movements UNION ALL SELECT amount FROM credit_card_movements LIMIT 500"
        )
        assert "LIMIT 200" in result.upper()

    def test_keeps_union_limit_within_bounds(self) -> None:
        result = cap_limit(
            "SELECT amount FROM account_movements UNION ALL SELECT amount FROM credit_card_movements LIMIT 100"
        )
        assert "LIMIT 100" in result.upper()
        assert "200" not in result

    def test_adds_limit_to_intersect(self) -> None:
        result = cap_limit("SELECT id FROM accounts INTERSECT SELECT id FROM accounts")
        assert "LIMIT 200" in result.upper()

    def test_adds_limit_to_except(self) -> None:
        result = cap_limit("SELECT id FROM accounts EXCEPT SELECT id FROM accounts")
        assert "LIMIT 200" in result.upper()

    def test_fetch_first_within_bounds_preserved(self) -> None:
        result = cap_limit("SELECT * FROM account_movements FETCH FIRST 10 ROWS ONLY")
        assert "200" not in result
        assert "10" in result

    def test_fetch_first_over_max_capped(self) -> None:
        result = cap_limit("SELECT * FROM account_movements FETCH FIRST 500 ROWS ONLY")
        assert "LIMIT 200" in result.upper()
