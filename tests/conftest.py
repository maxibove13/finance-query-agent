"""Shared test fixtures: testcontainers Postgres, moto DynamoDB, sample schema."""

from __future__ import annotations

import asyncio

import asyncpg
import boto3
import docker
import pytest
from moto import mock_aws
from testcontainers.postgres import PostgresContainer

from finance_query_agent.connection import Connection
from finance_query_agent.encryption import FieldEncryptor
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


def skip_without_docker() -> None:
    """Call at the top of any fixture that needs Docker."""
    if not _docker_available():
        pytest.skip("Docker not available")


# ── Schema Mapping Fixture ──────────────────────────────────────────────────

SEED_USER_1 = 1
SEED_USER_2 = 2


@pytest.fixture(scope="session")
def sample_schema_mapping() -> SchemaMapping:
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
        categories=TableMapping(table="tags", columns={"id": "id", "name": "name"}, user_scoped=False),
        accounts=TableMapping(table="accounts", columns={"id": "id", "user_id": "user_id", "name": "alias"}),
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
                    table="credit_cards", on="credit_card_movements.credit_card_id = credit_cards.id", type="inner"
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


# ── Postgres Fixtures ────────────────────────────────────────────────────────

SEED_SQL = """
CREATE TYPE movementdirection AS ENUM ('credit', 'debit');

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    alias VARCHAR(255),
    currency VARCHAR(3) NOT NULL
);

CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE account_movements (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    category_id INTEGER REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    description VARCHAR(255) NOT NULL,
    movement_direction movementdirection NOT NULL,
    balance NUMERIC(15,2)
);

CREATE TABLE credit_cards (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    currency VARCHAR(3) NOT NULL
);

CREATE TABLE credit_card_movements (
    id SERIAL PRIMARY KEY,
    credit_card_id INTEGER NOT NULL REFERENCES credit_cards(id),
    category_id INTEGER REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    description VARCHAR(255) NOT NULL,
    movement_direction movementdirection NOT NULL,
    currency VARCHAR(3) NOT NULL
);

-- Accounts (id auto-incremented: 1, 2, 3)
INSERT INTO accounts (user_id, alias, currency) VALUES (1, 'Checking', 'USD');
INSERT INTO accounts (user_id, alias, currency) VALUES (1, 'Savings UYU', 'UYU');
INSERT INTO accounts (user_id, alias, currency) VALUES (2, 'Other User', 'USD');

-- Tags (categories, id auto-incremented: 1..5)
INSERT INTO tags (name) VALUES ('groceries');
INSERT INTO tags (name) VALUES ('transport');
INSERT INTO tags (name) VALUES ('entertainment');
INSERT INTO tags (name) VALUES ('utilities');
INSERT INTO tags (name) VALUES ('restaurants');

-- Credit cards (id auto-incremented: 1)
INSERT INTO credit_cards (user_id, currency) VALUES (1, 'USD');

-- Account movements for user 1 (USD, account_id=1)
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2025-10-05', 150.00, 'Whole Foods', 'debit', 4850.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2025-10-15', 85.50, 'Trader Joes', 'debit', 4764.50);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 2, '2025-10-10', 45.00, 'Uber', 'debit', 4805.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 3, '2025-10-20', 12.99, 'Netflix', 'debit', 4792.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, NULL, '2025-10-25', 3000.00, 'Salary Oct', 'credit', 7792.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2025-11-05', 120.00, 'Whole Foods', 'debit', 7672.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2025-11-15', 95.00, 'Trader Joes', 'debit', 7577.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 2, '2025-11-08', 30.00, 'Uber', 'debit', 7642.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 3, '2025-11-20', 12.99, 'Netflix', 'debit', 7564.02);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 4, '2025-11-01', 89.00, 'Electric Company', 'debit', 7703.01);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, NULL, '2025-11-25', 3000.00, 'Salary Nov', 'credit', 10564.02);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2025-12-05', 140.00, 'Whole Foods', 'debit', 10424.02);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 3, '2025-12-20', 12.99, 'Netflix', 'debit', 10411.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 5, '2025-12-15', 65.00, 'Sushi Place', 'debit', 10359.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, NULL, '2025-12-25', 3000.00, 'Salary Dec', 'credit', 13359.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2026-01-05', 160.00, 'Whole Foods', 'debit', 13199.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 3, '2026-01-20', 12.99, 'Netflix', 'debit', 13186.04);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 2, '2026-01-10', 55.00, 'Uber', 'debit', 13144.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 4, '2026-01-01', 92.00, 'Electric Company', 'debit', 13107.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, NULL, '2026-01-25', 3000.00, 'Salary Jan', 'credit', 16107.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 1, '2026-02-05', 135.00, 'Whole Foods', 'debit', 15972.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 3, '2026-02-20', 12.99, 'Netflix', 'debit', 15959.04);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, 5, '2026-02-14', 80.00, 'Valentines Dinner', 'debit', 15892.03);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (1, NULL, '2026-02-25', 3000.00, 'Salary Feb', 'credit', 18892.03);

-- Account movements for user 1 (UYU, account_id=2)
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (2, 1, '2025-11-10', 2500.00, 'Supermercado', 'debit', 47500.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (2, 2, '2025-11-15', 800.00, 'Bus Pass', 'debit', 46700.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (2, NULL, '2025-11-25', 50000.00, 'Sueldo Nov', 'credit', 96700.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (2, 1, '2025-12-10', 2800.00, 'Supermercado', 'debit', 93900.00);
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (2, NULL, '2025-12-25', 50000.00, 'Sueldo Dec', 'credit', 143900.00);

-- Account movements for user 2 (isolation test, account_id=3)
INSERT INTO account_movements (account_id, category_id, issued_at, amount, description, movement_direction, balance) VALUES (3, 1, '2025-11-05', 200.00, 'Other User Groceries', 'debit', 4800.00);

-- Credit card movements for user 1 (credit_card_id=1)
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 3, '2025-11-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 5, '2025-11-18', 42.00, 'Restaurant XYZ', 'debit', 'USD');
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 3, '2025-12-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 5, '2025-12-20', 55.00, 'Steakhouse', 'debit', 'USD');
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 3, '2026-01-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements (credit_card_id, category_id, issued_at, amount, description, movement_direction, currency) VALUES (1, 3, '2026-02-10', 15.99, 'Spotify', 'debit', 'USD');
"""


@pytest.fixture(scope="session")
def postgres_url():
    """Start a Postgres container and seed it with test data."""
    skip_without_docker()
    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

        async def _seed():
            conn = await asyncpg.connect(url)
            try:
                await conn.execute(SEED_SQL)
            finally:
                await conn.close()

        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_seed())
        yield url


@pytest.fixture
async def db_connection(postgres_url: str):
    """Provide a connected Connection instance."""
    import finance_query_agent.connection as conn_module

    conn_module._pool = None
    conn = Connection(postgres_url)
    await conn.connect()
    try:
        yield conn
    finally:
        if conn_module._pool is not None:
            await conn_module._pool.close()
            conn_module._pool = None


@pytest.fixture
def query_builder(sample_schema_mapping: SchemaMapping) -> QueryBuilder:
    return QueryBuilder(sample_schema_mapping)


# ── DynamoDB Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def dynamodb_table():
    """Create a moto-mocked DynamoDB table."""
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="test-conversations",
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "updated_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "user_id-index",
                    "KeySchema": [
                        {"AttributeName": "user_id", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield "test-conversations"


@pytest.fixture
def field_encryptor():
    """Dev-mode encryptor (no-op passthrough)."""
    return FieldEncryptor(key=None)
