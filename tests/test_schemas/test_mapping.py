"""Tests for schema mapping validation."""

from __future__ import annotations

import pytest

from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)


def _make_transactions(**overrides):
    defaults = {
        "table": "account_movements",
        "columns": {
            "date": "issued_at",
            "amount": "amount",
            "description": "description",
            "user_id": ColumnRef(table="accounts", column="user_id"),
            "currency": ColumnRef(table="accounts", column="currency"),
            "account_id": "account_id",
        },
        "joins": [
            JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
            JoinDef(table="tags", on="account_movements.category_id = tags.id", type="left"),
        ],
        "amount_convention": AmountConvention(
            direction_column="movement_direction",
            expense_value="debit",
            income_value="credit",
        ),
    }
    defaults.update(overrides)
    return TableMapping(**defaults)


def _make_categories():
    return TableMapping(table="tags", columns={"id": "id", "name": "name"}, user_scoped=False)


def _make_accounts():
    return TableMapping(table="accounts", columns={"id": "id", "user_id": "user_id", "name": "alias"})


def _make_schema(**overrides):
    defaults = {
        "transactions": _make_transactions(),
        "categories": _make_categories(),
        "accounts": _make_accounts(),
    }
    defaults.update(overrides)
    return SchemaMapping(**defaults)


class TestAmountConvention:
    def test_direction_column_valid(self):
        ac = AmountConvention(direction_column="dir", expense_value="debit", income_value="credit")
        assert ac.direction_column == "dir"

    def test_sign_based_valid(self):
        ac = AmountConvention(sign_means_expense="positive")
        assert ac.sign_means_expense == "positive"

    def test_both_set_rejected(self):
        with pytest.raises(ValueError, match="not both"):
            AmountConvention(direction_column="dir", expense_value="d", income_value="c", sign_means_expense="positive")

    def test_neither_set_rejected(self):
        with pytest.raises(ValueError, match="Must set either"):
            AmountConvention()

    def test_direction_without_values_rejected(self):
        with pytest.raises(ValueError, match="expense_value and income_value"):
            AmountConvention(direction_column="dir")


class TestSchemaMapping:
    def test_valid_schema(self):
        schema = _make_schema()
        assert schema.transactions.table == "account_movements"

    def test_missing_required_column(self):
        cols = {
            "date": "issued_at",
            "amount": "amount",
            # missing description, user_id, currency, account_id
        }
        with pytest.raises(ValueError, match="missing required column mappings"):
            _make_schema(
                transactions=TableMapping(
                    table="t",
                    columns=cols,
                    amount_convention=AmountConvention(sign_means_expense="positive"),
                )
            )

    def test_column_ref_without_join_rejected(self):
        with pytest.raises(ValueError, match="no JoinDef exists"):
            _make_schema(
                transactions=_make_transactions(
                    columns={
                        "date": "issued_at",
                        "amount": "amount",
                        "description": "description",
                        "user_id": ColumnRef(table="no_such_table", column="user_id"),
                        "currency": "currency",
                        "account_id": "account_id",
                    },
                )
            )

    def test_transactions_require_amount_convention(self):
        with pytest.raises(ValueError, match="requires amount_convention"):
            _make_schema(transactions=_make_transactions(amount_convention=None))

    def test_categories_require_id_and_name(self):
        with pytest.raises(ValueError, match="'id' and 'name'"):
            _make_schema(categories=TableMapping(table="cats", columns={"id": "id"}, user_scoped=False))

    def test_accounts_require_id_and_user_id(self):
        with pytest.raises(ValueError, match="'id' and 'user_id'"):
            _make_schema(accounts=TableMapping(table="accts", columns={"id": "id"}))

    def test_secondary_transactions_validated(self):
        secondary = _make_transactions(table="credit_card_movements")
        schema = _make_schema(secondary_transactions=secondary)
        assert schema.secondary_transactions is not None

    def test_with_balance_column(self):
        tx = _make_transactions(
            columns={
                **_make_transactions().columns,
                "balance": "balance",
            }
        )
        schema = _make_schema(transactions=tx)
        assert "balance" in schema.transactions.columns
