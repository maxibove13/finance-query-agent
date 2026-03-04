"""Tests for SQL validator — keyword rejection, allowlist, LIMIT injection, user filter."""

from __future__ import annotations

import pytest

from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)
from finance_query_agent.validation.sql_validator import SqlValidator


@pytest.fixture()
def schema() -> SchemaMapping:
    """Minimal SchemaMapping for testing."""
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
    )


@pytest.fixture()
def validator(schema: SchemaMapping) -> SqlValidator:
    return SqlValidator(schema)


# --- Allowlist derivation ---


class TestAllowlistDerivation:
    def test_tables_include_mapped_and_joined(self, validator: SqlValidator) -> None:
        assert "account_movements" in validator.allowed_tables
        assert "accounts" in validator.allowed_tables
        assert "tags" in validator.allowed_tables

    def test_columns_include_direct_and_ref(self, validator: SqlValidator) -> None:
        # Direct columns
        assert "issued_at" in validator.allowed_columns
        assert "amount" in validator.allowed_columns
        assert "description" in validator.allowed_columns
        assert "account_id" in validator.allowed_columns
        # ColumnRef columns
        assert "user_id" in validator.allowed_columns
        assert "currency" in validator.allowed_columns
        # Category columns
        assert "id" in validator.allowed_columns
        assert "name" in validator.allowed_columns
        # Account columns
        assert "alias" in validator.allowed_columns

    def test_unmapped_table_not_in_allowlist(self, validator: SqlValidator) -> None:
        assert "users" not in validator.allowed_tables
        assert "secret_table" not in validator.allowed_tables


# --- Keyword rejection ---


class TestKeywordRejection:
    @pytest.mark.parametrize(
        "keyword",
        [
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
            "EXECUTE",
            "CALL",
            "SET",
            "RESET",
            "LISTEN",
            "NOTIFY",
            "LOAD",
            "VACUUM",
            "REINDEX",
        ],
    )
    def test_rejects_forbidden_keyword(self, validator: SqlValidator, keyword: str) -> None:
        sql = f"{keyword} INTO account_movements VALUES (1)"
        errors = validator.validate(sql)
        assert any(keyword.upper() in e for e in errors)

    def test_rejects_case_insensitive(self, validator: SqlValidator) -> None:
        errors = validator.validate("delete from account_movements")
        assert any("DELETE" in e for e in errors)

    def test_allows_keyword_inside_string_literal_still_flagged(self, validator: SqlValidator) -> None:
        # Defense-in-depth: we flag even if inside a string. Simple regex approach.
        sql = "SELECT description FROM account_movements WHERE description = 'DELETE ME'"
        errors = validator.validate(sql)
        assert any("DELETE" in e for e in errors)

    def test_allows_valid_select(self, validator: SqlValidator) -> None:
        sql = "SELECT amount, description FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []


# --- Single SELECT enforcement ---


class TestSelectEnforcement:
    def test_rejects_non_select(self, validator: SqlValidator) -> None:
        errors = validator.validate("EXPLAIN SELECT 1")
        assert any("must start with SELECT" in e for e in errors)

    def test_accepts_select_with_whitespace(self, validator: SqlValidator) -> None:
        sql = "  SELECT amount FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []


# --- CTE rejection ---


class TestCTERejection:
    def test_rejects_cte(self, validator: SqlValidator) -> None:
        sql = "WITH totals AS (SELECT 1) SELECT * FROM totals"
        errors = validator.validate(sql)
        assert any("CTE" in e for e in errors)

    def test_rejects_with_case_insensitive(self, validator: SqlValidator) -> None:
        sql = "with t as (select 1) select * from t"
        errors = validator.validate(sql)
        assert any("CTE" in e for e in errors)


# --- Subquery rejection ---


class TestSubqueryRejection:
    def test_rejects_subquery_in_where(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements WHERE amount > (SELECT AVG(amount) FROM account_movements)"
        errors = validator.validate(sql)
        assert any("Subquer" in e for e in errors)

    def test_rejects_subquery_in_from(self, validator: SqlValidator) -> None:
        sql = "SELECT * FROM (SELECT amount FROM account_movements) sub"
        errors = validator.validate(sql)
        assert any("Subquer" in e for e in errors)


# --- Multiple statement rejection ---


class TestMultipleStatements:
    def test_rejects_multiple_statements(self, validator: SqlValidator) -> None:
        sql = "SELECT 1; SELECT 2"
        errors = validator.validate(sql)
        assert any("Multiple statements" in e for e in errors)

    def test_allows_trailing_semicolon(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements;"
        errors = validator.validate(sql)
        assert errors == []


# --- Table allowlist ---


class TestTableAllowlist:
    def test_rejects_unmapped_table(self, validator: SqlValidator) -> None:
        sql = "SELECT * FROM users"
        errors = validator.validate(sql)
        assert any("Table not in allowlist: users" in e for e in errors)

    def test_accepts_mapped_table(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []

    def test_rejects_unmapped_join(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements JOIN secret_table ON 1=1"
        errors = validator.validate(sql)
        assert any("secret_table" in e for e in errors)

    def test_accepts_mapped_join(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements JOIN accounts ON account_movements.account_id = accounts.id"
        errors = validator.validate(sql)
        assert errors == []


# --- Column allowlist ---


class TestColumnAllowlist:
    def test_rejects_unmapped_qualified_column(self, validator: SqlValidator) -> None:
        sql = "SELECT accounts.secret_col FROM accounts"
        errors = validator.validate(sql)
        assert any("Column not in allowlist" in e for e in errors)

    def test_accepts_mapped_qualified_column(self, validator: SqlValidator) -> None:
        sql = "SELECT accounts.user_id FROM accounts"
        errors = validator.validate(sql)
        assert errors == []

    def test_ignores_unknown_table_prefix(self, validator: SqlValidator) -> None:
        # If table prefix is not in allowlist, we don't flag the column
        # (the table itself will be caught by table check if in FROM/JOIN)
        sql = "SELECT unknown.col FROM account_movements"
        errors = validator.validate(sql)
        assert not any("Column not in allowlist" in e for e in errors)


# --- LIMIT injection ---


class TestLimitInjection:
    def test_adds_limit_when_missing(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements"
        result = validator.inject_limit(sql, max_limit=200)
        assert result == "SELECT amount FROM account_movements LIMIT 200"

    def test_adds_limit_before_semicolon(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements;"
        result = validator.inject_limit(sql, max_limit=200)
        assert result == "SELECT amount FROM account_movements LIMIT 200;"

    def test_caps_excessive_limit(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements LIMIT 9999"
        result = validator.inject_limit(sql, max_limit=200)
        assert "LIMIT 200" in result
        assert "9999" not in result

    def test_preserves_valid_limit(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements LIMIT 50"
        result = validator.inject_limit(sql, max_limit=200)
        assert "LIMIT 50" in result

    def test_caps_with_custom_max(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements LIMIT 150"
        result = validator.inject_limit(sql, max_limit=100)
        assert "LIMIT 100" in result


# --- User filter injection ---


class TestUserFilterInjection:
    def test_injects_where_when_none_exists(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements"
        result = validator.inject_user_filter(sql, "$1")
        assert "WHERE accounts.user_id = $1" in result

    def test_injects_and_when_where_exists(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements WHERE amount > 100"
        result = validator.inject_user_filter(sql, "$1")
        assert "accounts.user_id = $1 AND" in result
        assert "amount > 100" in result

    def test_injects_before_group_by(self, validator: SqlValidator) -> None:
        sql = "SELECT description, SUM(amount) FROM account_movements GROUP BY description"
        result = validator.inject_user_filter(sql, "$1")
        assert "WHERE accounts.user_id = $1" in result
        assert result.index("WHERE") < result.index("GROUP BY")

    def test_injects_before_order_by(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements ORDER BY amount"
        result = validator.inject_user_filter(sql, "$1")
        assert "WHERE accounts.user_id = $1" in result
        assert result.index("WHERE") < result.index("ORDER BY")

    def test_injects_before_limit(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements LIMIT 10"
        result = validator.inject_user_filter(sql, "$1")
        assert "WHERE accounts.user_id = $1" in result
        assert result.index("WHERE") < result.index("LIMIT")

    def test_uses_direct_column_when_not_columnref(self) -> None:
        """When user_id is a direct column (not ColumnRef), use table.column."""
        schema = SchemaMapping(
            transactions=TableMapping(
                table="txns",
                columns={
                    "date": "txn_date",
                    "amount": "amt",
                    "description": "desc",
                    "user_id": "uid",
                    "currency": "curr",
                    "account_id": "acct_id",
                },
                amount_convention=AmountConvention(sign_means_expense="negative"),
            ),
            categories=TableMapping(
                table="cats",
                columns={"id": "id", "name": "name"},
                user_scoped=False,
            ),
            accounts=TableMapping(
                table="accts",
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        v = SqlValidator(schema)
        result = v.inject_user_filter("SELECT amt FROM txns", "$1")
        assert "txns.uid = $1" in result

    def test_handles_trailing_semicolon(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements;"
        result = validator.inject_user_filter(sql, "$1")
        assert "WHERE accounts.user_id = $1" in result
        assert result.endswith(";")


# --- Combined validation ---


class TestCombinedValidation:
    def test_valid_query_passes_all_checks(self, validator: SqlValidator) -> None:
        sql = (
            "SELECT amount, description FROM account_movements"
            " JOIN accounts ON account_movements.account_id = accounts.id"
            " WHERE amount > 100 ORDER BY amount LIMIT 50"
        )
        errors = validator.validate(sql)
        assert errors == []

    def test_multiple_errors_collected(self, validator: SqlValidator) -> None:
        sql = "DELETE FROM secret_table; DROP TABLE accounts"
        errors = validator.validate(sql)
        # Should catch: not SELECT, forbidden keyword (DELETE, DROP), multiple statements, unmapped table
        assert len(errors) >= 3


# --- Bypass prevention ---


class TestBypassPrevention:
    """Verify the SQL validator catches known bypass vectors."""

    def test_rejects_quoted_table_name(self, validator: SqlValidator) -> None:
        sql = 'SELECT * FROM "secret_table"'
        errors = validator.validate(sql)
        assert any("Double-quoted" in e for e in errors)

    def test_rejects_quoted_column_name(self, validator: SqlValidator) -> None:
        sql = 'SELECT "secret_col" FROM account_movements'
        errors = validator.validate(sql)
        assert any("Double-quoted" in e for e in errors)

    def test_rejects_schema_qualified_unknown_table(self, validator: SqlValidator) -> None:
        sql = "SELECT * FROM public.secret_table"
        errors = validator.validate(sql)
        assert any("Table not in allowlist: secret_table" in e for e in errors)

    def test_allows_schema_qualified_known_table(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM public.account_movements"
        errors = validator.validate(sql)
        assert errors == []

    def test_alias_column_rejected(self, validator: SqlValidator) -> None:
        sql = "SELECT a.secret_col FROM account_movements a"
        errors = validator.validate(sql)
        assert any("Column not in allowlist" in e for e in errors)

    def test_alias_valid_column_passes(self, validator: SqlValidator) -> None:
        sql = "SELECT a.amount FROM account_movements a"
        errors = validator.validate(sql)
        assert errors == []

    def test_as_alias_column_rejected(self, validator: SqlValidator) -> None:
        sql = "SELECT am.secret_col FROM account_movements AS am"
        errors = validator.validate(sql)
        assert any("Column not in allowlist" in e for e in errors)

    def test_quoted_plus_schema(self, validator: SqlValidator) -> None:
        sql = 'SELECT * FROM public."secret_table"'
        errors = validator.validate(sql)
        assert any("Double-quoted" in e for e in errors)

    def test_rejects_unqualified_unknown_column(self, validator: SqlValidator) -> None:
        sql = "SELECT secret FROM account_movements"
        errors = validator.validate(sql)
        assert any("Column not in allowlist: secret" in e for e in errors)

    def test_allows_unqualified_known_column(self, validator: SqlValidator) -> None:
        sql = "SELECT amount, description FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []

    def test_rejects_unqualified_in_where(self, validator: SqlValidator) -> None:
        sql = "SELECT amount FROM account_movements WHERE secret > 5"
        errors = validator.validate(sql)
        assert any("Column not in allowlist: secret" in e for e in errors)

    def test_allows_select_alias_in_group_by(self, validator: SqlValidator) -> None:
        sql = "SELECT description AS merchant, SUM(amount) AS total FROM account_movements GROUP BY merchant"
        errors = validator.validate(sql)
        assert errors == []

    def test_allows_aggregate_functions(self, validator: SqlValidator) -> None:
        sql = "SELECT COUNT(*), SUM(amount), AVG(amount) FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []

    def test_allows_type_cast(self, validator: SqlValidator) -> None:
        sql = "SELECT SUM(amount)::int AS total FROM account_movements"
        errors = validator.validate(sql)
        assert errors == []
