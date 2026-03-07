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
    ViewMapping,
)
from finance_query_agent.validation.schema_validator import ColumnTypeInfo, introspect_schema, validate_schema
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

CREATE TABLE historical_expenses_mv (
    user_id UUID NOT NULL,
    filter_at DATE NOT NULL,
    usd_amount NUMERIC NOT NULL,
    local_amount NUMERIC NOT NULL,
    category TEXT,
    description TEXT
);

CREATE TABLE historical_incomes_mv (
    user_id UUID NOT NULL,
    month TEXT NOT NULL,
    month_as_date DATE,
    usd_amount NUMERIC NOT NULL,
    local_amount NUMERIC NOT NULL
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

        import asyncpg as _asyncpg

        async def _setup():
            raw = await _asyncpg.connect(url)
            try:
                await raw.execute(_CREATE_TABLES)
            finally:
                await raw.close()

        asyncio.new_event_loop().run_until_complete(_setup())
        yield url


@pytest.fixture
async def conn(postgres_url):
    """Create a Connection to the already-initialized test DB."""
    import finance_query_agent.connection as conn_module

    conn_module._pool = None
    c = Connection(postgres_url)
    await c.connect()
    try:
        yield c
    finally:
        if conn_module._pool is not None:
            await conn_module._pool.close()
            conn_module._pool = None


class TestValidateSchema:
    async def test_valid_schema_passes(self, conn):
        result = await validate_schema(_valid_schema(), conn)
        assert isinstance(result, ColumnTypeInfo)
        # user_id is on accounts table, declared as UUID in this test schema
        assert result.user_id_type == "uuid"
        # movement_direction is TEXT in this test schema, not an enum
        assert result.direction_is_enum is False

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


class TestValidateViewMapping:
    async def test_valid_view_mapping_passes(self, conn):
        schema = _valid_schema()
        schema = schema.model_copy(
            update={
                "unified_expenses": ViewMapping(
                    table="historical_expenses_mv",
                    columns={
                        "user_id": "user_id",
                        "date": "filter_at",
                        "usd_amount": "usd_amount",
                        "local_amount": "local_amount",
                        "category": "category",
                        "merchant": "description",
                    },
                )
            }
        )
        result = await validate_schema(schema, conn)
        assert isinstance(result, ColumnTypeInfo)

    async def test_view_missing_table_raises(self, conn):
        schema = _valid_schema()
        schema = schema.model_copy(
            update={
                "unified_expenses": ViewMapping(
                    table="nonexistent_mv",
                    columns={
                        "user_id": "user_id",
                        "date": "filter_at",
                        "usd_amount": "usd_amount",
                        "local_amount": "local_amount",
                        "category": "category",
                        "merchant": "description",
                    },
                )
            }
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_mv.*does not exist"):
            await validate_schema(schema, conn)

    async def test_view_missing_column_raises(self, conn):
        schema = _valid_schema()
        schema = schema.model_copy(
            update={
                "unified_expenses": ViewMapping(
                    table="historical_expenses_mv",
                    columns={
                        "user_id": "user_id",
                        "date": "filter_at",
                        "usd_amount": "nonexistent_col",
                        "local_amount": "local_amount",
                        "category": "category",
                        "merchant": "description",
                    },
                )
            }
        )
        with pytest.raises(SchemaValidationError, match="nonexistent_col.*does not exist"):
            await validate_schema(schema, conn)

    async def test_income_text_month_passes(self, conn):
        schema = _valid_schema()
        schema = schema.model_copy(
            update={
                "unified_income": ViewMapping(
                    table="historical_incomes_mv",
                    columns={
                        "user_id": "user_id",
                        "month": "month",
                        "usd_amount": "usd_amount",
                        "local_amount": "local_amount",
                    },
                )
            }
        )
        result = await validate_schema(schema, conn)
        assert isinstance(result, ColumnTypeInfo)

    async def test_income_date_month_raises(self, conn):
        """month column mapped to a DATE column should fail — lexicographic comparison requires text."""
        schema = _valid_schema()
        schema = schema.model_copy(
            update={
                "unified_income": ViewMapping(
                    table="historical_incomes_mv",
                    columns={
                        "user_id": "user_id",
                        "month": "month_as_date",
                        "usd_amount": "usd_amount",
                        "local_amount": "local_amount",
                    },
                )
            }
        )
        with pytest.raises(SchemaValidationError, match="month.*must be text"):
            await validate_schema(schema, conn)


_CREATE_ENUM_TABLES = """
CREATE TYPE movementdirection AS ENUM ('DEBIT', 'CREDIT');

CREATE TABLE enum_movements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id),
    category_id UUID REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC NOT NULL,
    description TEXT NOT NULL,
    movement_direction movementdirection NOT NULL,
    balance NUMERIC
);
"""


@pytest.fixture(scope="module")
def postgres_url_with_enum():
    """Postgres container with enum-typed direction column."""
    skip_without_docker()
    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url().replace("+psycopg2", "")

        import asyncio

        import asyncpg as _asyncpg

        async def _setup():
            raw = await _asyncpg.connect(url)
            try:
                await raw.execute(_CREATE_TABLES)
                await raw.execute(_CREATE_ENUM_TABLES)
            finally:
                await raw.close()

        asyncio.new_event_loop().run_until_complete(_setup())
        yield url


@pytest.fixture
async def conn_with_enum(postgres_url_with_enum):
    """Connection to a DB that has enum-typed direction column."""
    import finance_query_agent.connection as conn_module

    conn_module._pool = None
    c = Connection(postgres_url_with_enum)
    await c.connect()
    try:
        yield c
    finally:
        if conn_module._pool is not None:
            await conn_module._pool.close()
            conn_module._pool = None


def _enum_schema(expense: str = "DEBIT", income: str = "CREDIT") -> SchemaMapping:
    """Schema using enum_movements table (direction is a real enum)."""
    return SchemaMapping(
        transactions=TableMapping(
            table="enum_movements",
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
                JoinDef(table="accounts", on="enum_movements.account_id = accounts.id", type="inner"),
                JoinDef(table="tags", on="enum_movements.category_id = tags.id", type="left"),
            ],
            amount_convention=AmountConvention(
                direction_column="movement_direction",
                expense_value=expense,
                income_value=income,
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


class TestEnumLabelValidation:
    async def test_matching_enum_labels_pass(self, conn_with_enum):
        result = await validate_schema(_enum_schema("DEBIT", "CREDIT"), conn_with_enum)
        assert result.direction_is_enum is True

    async def test_lowercase_mismatch_raises(self, conn_with_enum):
        with pytest.raises(SchemaValidationError, match="'debit' not found in enum"):
            await validate_schema(_enum_schema("debit", "credit"), conn_with_enum)

    async def test_partial_mismatch_reports_specific_field(self, conn_with_enum):
        with pytest.raises(SchemaValidationError, match="expense_value.*'debit'") as exc_info:
            await validate_schema(_enum_schema("debit", "CREDIT"), conn_with_enum)
        # income_value should NOT appear in errors since it matches
        assert "income_value" not in str(exc_info.value)


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
