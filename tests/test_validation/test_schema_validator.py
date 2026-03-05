"""Tests for schema_validator.py — validates SchemaMapping against live DB."""

from __future__ import annotations

import pytest
from testcontainers.postgres import PostgresContainer

from finance_query_agent.connection import Connection
from finance_query_agent.exceptions import SchemaValidationError
from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)
from finance_query_agent.validation.schema_validator import introspect_schema, validate_schema
from tests.conftest import skip_without_docker

# SQL to create the test schema matching the spec's sample
_CREATE_TABLES = """
CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    alias TEXT,
    currency TEXT NOT NULL
);

CREATE TABLE tags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL
);

CREATE TABLE account_movements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id),
    category_id UUID REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC NOT NULL,
    description TEXT NOT NULL,
    movement_direction TEXT NOT NULL,
    balance NUMERIC
);

CREATE TABLE credit_cards (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL
);

CREATE TABLE credit_card_movements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    credit_card_id UUID NOT NULL REFERENCES credit_cards(id),
    category_id UUID REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC NOT NULL,
    description TEXT NOT NULL,
    currency TEXT NOT NULL,
    movement_direction TEXT NOT NULL
);
"""


def _valid_schema() -> SchemaMapping:
    """The full sample schema from the spec."""
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
        secondary_transactions=TableMapping(
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
        ),
    )


@pytest.fixture(scope="module")
def postgres_url():
    """Start a real Postgres container for the module and create tables once."""
    skip_without_docker()
    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url().replace("+psycopg2", "")

        import asyncio

        async def _setup():
            c = Connection(url)
            await c.connect()
            await c.execute(_CREATE_TABLES)
            await c.close()

        asyncio.new_event_loop().run_until_complete(_setup())
        yield url


@pytest.fixture
async def conn(postgres_url):
    """Create a Connection to the already-initialized test DB."""
    c = Connection(postgres_url)
    await c.connect()
    try:
        yield c
    finally:
        await c.close()


class TestValidateSchema:
    async def test_valid_schema_passes(self, conn):
        await validate_schema(_valid_schema(), conn)

    async def test_missing_table_raises(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="nonexistent_table",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": "user_id",
                    "currency": "currency",
                    "account_id": "account_id",
                },
                joins=[],
                amount_convention=AmountConvention(sign_means_expense="negative"),
            ),
            categories=TableMapping(
                table="tags",
                columns={"id": "id", "name": "name"},
                user_scoped=False,
            ),
            accounts=TableMapping(
                table="accounts",
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_table.*does not exist"):
            await validate_schema(schema, conn)

    async def test_missing_column_raises(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="account_movements",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": "nonexistent_col",
                    "currency": "currency",
                    "account_id": "account_id",
                },
                joins=[],
                amount_convention=AmountConvention(sign_means_expense="negative"),
            ),
            categories=TableMapping(
                table="tags",
                columns={"id": "id", "name": "name"},
                user_scoped=False,
            ),
            accounts=TableMapping(
                table="accounts",
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_col.*does not exist"):
            await validate_schema(schema, conn)

    async def test_column_ref_to_missing_table_raises(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="account_movements",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": ColumnRef(table="nonexistent_table", column="user_id"),
                    "currency": "currency",
                    "account_id": "account_id",
                },
                joins=[
                    JoinDef(
                        table="nonexistent_table",
                        on="account_movements.account_id = nonexistent_table.id",
                    ),
                ],
                amount_convention=AmountConvention(sign_means_expense="negative"),
            ),
            categories=TableMapping(
                table="tags",
                columns={"id": "id", "name": "name"},
                user_scoped=False,
            ),
            accounts=TableMapping(
                table="accounts",
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_table.*does not exist"):
            await validate_schema(schema, conn)

    async def test_column_ref_to_missing_column_raises(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="account_movements",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": ColumnRef(table="accounts", column="nonexistent_col"),
                    "currency": ColumnRef(table="accounts", column="currency"),
                    "account_id": "account_id",
                },
                joins=[
                    JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
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
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_col.*does not exist"):
            await validate_schema(schema, conn)

    async def test_invalid_direction_column_raises(self, conn):
        schema = SchemaMapping(
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
                ],
                amount_convention=AmountConvention(
                    direction_column="nonexistent_direction_col",
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
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_direction_col.*does not exist"):
            await validate_schema(schema, conn)

    async def test_missing_join_table_raises(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="account_movements",
                columns={
                    "date": "issued_at",
                    "amount": "amount",
                    "description": "description",
                    "user_id": ColumnRef(table="ghost_table", column="user_id"),
                    "currency": "amount",
                    "account_id": "account_id",
                },
                joins=[
                    JoinDef(table="ghost_table", on="account_movements.account_id = ghost_table.id"),
                ],
                amount_convention=AmountConvention(sign_means_expense="negative"),
            ),
            categories=TableMapping(
                table="tags",
                columns={"id": "id", "name": "name"},
                user_scoped=False,
            ),
            accounts=TableMapping(
                table="accounts",
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError, match="ghost_table.*does not exist"):
            await validate_schema(schema, conn)

    async def test_collects_multiple_errors(self, conn):
        schema = SchemaMapping(
            transactions=TableMapping(
                table="account_movements",
                columns={
                    "date": "bad_date_col",
                    "amount": "bad_amount_col",
                    "description": "description",
                    "user_id": ColumnRef(table="accounts", column="user_id"),
                    "currency": ColumnRef(table="accounts", column="currency"),
                    "account_id": "account_id",
                },
                joins=[
                    JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
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
                columns={"id": "id", "user_id": "user_id"},
            ),
        )
        with pytest.raises(SchemaValidationError) as exc_info:
            await validate_schema(schema, conn)
        msg = str(exc_info.value)
        assert "bad_date_col" in msg
        assert "bad_amount_col" in msg


class TestIntrospectSchema:
    async def test_returns_table_descriptions(self, conn):
        result = await introspect_schema(conn, ["accounts", "tags"])
        assert "TABLE accounts" in result
        assert "TABLE tags" in result
        assert "user_id" in result
        assert "name" in result

    async def test_empty_tables_returns_empty_string(self, conn):
        result = await introspect_schema(conn, ["nonexistent_table"])
        assert result == ""

    async def test_includes_data_types(self, conn):
        result = await introspect_schema(conn, ["account_movements"])
        assert "numeric" in result
        assert "date" in result or "timestamp" in result

    async def test_includes_nullability(self, conn):
        result = await introspect_schema(conn, ["accounts"])
        assert "NOT NULL" in result
        # alias is nullable
        assert "NULL" in result
