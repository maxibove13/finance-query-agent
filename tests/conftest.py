"""Shared test fixtures: testcontainers Postgres, moto DynamoDB, sample schema."""

from __future__ import annotations

import asyncio

import asyncpg
import boto3
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

# ── Schema Mapping Fixture ──────────────────────────────────────────────────

SEED_USER_1 = "test-user-1"
SEED_USER_2 = "test-user-2"


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
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    alias TEXT,
    currency TEXT NOT NULL
);

CREATE TABLE tags (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE account_movements (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    category_id TEXT REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    description TEXT NOT NULL,
    movement_direction TEXT NOT NULL,
    balance NUMERIC(12,2)
);

CREATE TABLE credit_cards (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    currency TEXT NOT NULL
);

CREATE TABLE credit_card_movements (
    id TEXT PRIMARY KEY,
    credit_card_id TEXT NOT NULL REFERENCES credit_cards(id),
    category_id TEXT REFERENCES tags(id),
    issued_at DATE NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    description TEXT NOT NULL,
    movement_direction TEXT NOT NULL,
    currency TEXT NOT NULL
);

-- Accounts
INSERT INTO accounts VALUES ('acc-1', 'test-user-1', 'Checking', 'USD');
INSERT INTO accounts VALUES ('acc-2', 'test-user-1', 'Savings UYU', 'UYU');
INSERT INTO accounts VALUES ('acc-3', 'test-user-2', 'Other User', 'USD');

-- Tags (categories)
INSERT INTO tags VALUES ('cat-1', 'groceries');
INSERT INTO tags VALUES ('cat-2', 'transport');
INSERT INTO tags VALUES ('cat-3', 'entertainment');
INSERT INTO tags VALUES ('cat-4', 'utilities');
INSERT INTO tags VALUES ('cat-5', 'restaurants');

-- Credit cards
INSERT INTO credit_cards VALUES ('cc-1', 'test-user-1', 'USD');

-- Account movements for test-user-1 (USD)
INSERT INTO account_movements VALUES ('am-01', 'acc-1', 'cat-1', '2025-10-05', 150.00, 'Whole Foods', 'debit', 4850.00);
INSERT INTO account_movements VALUES ('am-02', 'acc-1', 'cat-1', '2025-10-15', 85.50, 'Trader Joes', 'debit', 4764.50);
INSERT INTO account_movements VALUES ('am-03', 'acc-1', 'cat-2', '2025-10-10', 45.00, 'Uber', 'debit', 4805.00);
INSERT INTO account_movements VALUES ('am-04', 'acc-1', 'cat-3', '2025-10-20', 12.99, 'Netflix', 'debit', 4792.01);
INSERT INTO account_movements VALUES ('am-05', 'acc-1', NULL, '2025-10-25', 3000.00, 'Salary Oct', 'credit', 7792.01);
INSERT INTO account_movements VALUES ('am-06', 'acc-1', 'cat-1', '2025-11-05', 120.00, 'Whole Foods', 'debit', 7672.01);
INSERT INTO account_movements VALUES ('am-07', 'acc-1', 'cat-1', '2025-11-15', 95.00, 'Trader Joes', 'debit', 7577.01);
INSERT INTO account_movements VALUES ('am-08', 'acc-1', 'cat-2', '2025-11-08', 30.00, 'Uber', 'debit', 7642.01);
INSERT INTO account_movements VALUES ('am-09', 'acc-1', 'cat-3', '2025-11-20', 12.99, 'Netflix', 'debit', 7564.02);
INSERT INTO account_movements VALUES ('am-10', 'acc-1', 'cat-4', '2025-11-01', 89.00, 'Electric Company', 'debit', 7703.01);
INSERT INTO account_movements VALUES ('am-11', 'acc-1', NULL, '2025-11-25', 3000.00, 'Salary Nov', 'credit', 10564.02);
INSERT INTO account_movements VALUES ('am-12', 'acc-1', 'cat-1', '2025-12-05', 140.00, 'Whole Foods', 'debit', 10424.02);
INSERT INTO account_movements VALUES ('am-13', 'acc-1', 'cat-3', '2025-12-20', 12.99, 'Netflix', 'debit', 10411.03);
INSERT INTO account_movements VALUES ('am-14', 'acc-1', 'cat-5', '2025-12-15', 65.00, 'Sushi Place', 'debit', 10359.03);
INSERT INTO account_movements VALUES ('am-15', 'acc-1', NULL, '2025-12-25', 3000.00, 'Salary Dec', 'credit', 13359.03);
INSERT INTO account_movements VALUES ('am-16', 'acc-1', 'cat-1', '2026-01-05', 160.00, 'Whole Foods', 'debit', 13199.03);
INSERT INTO account_movements VALUES ('am-17', 'acc-1', 'cat-3', '2026-01-20', 12.99, 'Netflix', 'debit', 13186.04);
INSERT INTO account_movements VALUES ('am-18', 'acc-1', 'cat-2', '2026-01-10', 55.00, 'Uber', 'debit', 13144.03);
INSERT INTO account_movements VALUES ('am-19', 'acc-1', 'cat-4', '2026-01-01', 92.00, 'Electric Company', 'debit', 13107.03);
INSERT INTO account_movements VALUES ('am-20', 'acc-1', NULL, '2026-01-25', 3000.00, 'Salary Jan', 'credit', 16107.03);
INSERT INTO account_movements VALUES ('am-21', 'acc-1', 'cat-1', '2026-02-05', 135.00, 'Whole Foods', 'debit', 15972.03);
INSERT INTO account_movements VALUES ('am-22', 'acc-1', 'cat-3', '2026-02-20', 12.99, 'Netflix', 'debit', 15959.04);
INSERT INTO account_movements VALUES ('am-23', 'acc-1', 'cat-5', '2026-02-14', 80.00, 'Valentines Dinner', 'debit', 15892.03);
INSERT INTO account_movements VALUES ('am-24', 'acc-1', NULL, '2026-02-25', 3000.00, 'Salary Feb', 'credit', 18892.03);

-- Account movements for test-user-1 (UYU account)
INSERT INTO account_movements VALUES ('am-25', 'acc-2', 'cat-1', '2025-11-10', 2500.00, 'Supermercado', 'debit', 47500.00);
INSERT INTO account_movements VALUES ('am-26', 'acc-2', 'cat-2', '2025-11-15', 800.00, 'Bus Pass', 'debit', 46700.00);
INSERT INTO account_movements VALUES ('am-27', 'acc-2', NULL, '2025-11-25', 50000.00, 'Sueldo Nov', 'credit', 96700.00);
INSERT INTO account_movements VALUES ('am-28', 'acc-2', 'cat-1', '2025-12-10', 2800.00, 'Supermercado', 'debit', 93900.00);
INSERT INTO account_movements VALUES ('am-29', 'acc-2', NULL, '2025-12-25', 50000.00, 'Sueldo Dec', 'credit', 143900.00);

-- Account movements for test-user-2 (isolation test)
INSERT INTO account_movements VALUES ('am-30', 'acc-3', 'cat-1', '2025-11-05', 200.00, 'Other User Groceries', 'debit', 4800.00);

-- Credit card movements for test-user-1
INSERT INTO credit_card_movements VALUES ('ccm-01', 'cc-1', 'cat-3', '2025-11-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements VALUES ('ccm-02', 'cc-1', 'cat-5', '2025-11-18', 42.00, 'Restaurant XYZ', 'debit', 'USD');
INSERT INTO credit_card_movements VALUES ('ccm-03', 'cc-1', 'cat-3', '2025-12-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements VALUES ('ccm-04', 'cc-1', 'cat-5', '2025-12-20', 55.00, 'Steakhouse', 'debit', 'USD');
INSERT INTO credit_card_movements VALUES ('ccm-05', 'cc-1', 'cat-3', '2026-01-10', 15.99, 'Spotify', 'debit', 'USD');
INSERT INTO credit_card_movements VALUES ('ccm-06', 'cc-1', 'cat-3', '2026-02-10', 15.99, 'Spotify', 'debit', 'USD');
"""


@pytest.fixture(scope="session")
def postgres_url():
    """Start a Postgres container and seed it with test data."""
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
    conn = Connection(postgres_url)
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()


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
