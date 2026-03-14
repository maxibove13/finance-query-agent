"""Shared test fixtures: testcontainers Postgres, moto DynamoDB."""

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

# Semantic model used by sql_governance's table allowlist validation
_TEST_SEMANTIC = {
    "description": "Test DB",
    "tables": [
        {"name": "accounts"},
        {"name": "account_movements"},
        {"name": "tags"},
        {"name": "credit_cards"},
        {"name": "credit_card_movements"},
    ],
    "relationships": [],
}


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


# ── Postgres Fixtures ────────────────────────────────────────────────────────

SEED_USER_1 = 1
SEED_USER_2 = 2

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

-- RLS: user-owned tables only. tags is shared reference data (global taxonomy) — no RLS by design.
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
CREATE POLICY user_isolation ON accounts FOR SELECT
  USING (user_id = NULLIF(current_setting('app.user_id', true), '')::integer);

ALTER TABLE credit_cards ENABLE ROW LEVEL SECURITY;
CREATE POLICY user_isolation ON credit_cards FOR SELECT
  USING (user_id = NULLIF(current_setting('app.user_id', true), '')::integer);

-- Movements don't have user_id directly; isolation via correlated subquery
ALTER TABLE account_movements ENABLE ROW LEVEL SECURITY;
CREATE POLICY user_isolation ON account_movements FOR SELECT
  USING (account_id IN (
    SELECT id FROM accounts
    WHERE user_id = NULLIF(current_setting('app.user_id', true), '')::integer
  ));

ALTER TABLE credit_card_movements ENABLE ROW LEVEL SECURITY;
CREATE POLICY user_isolation ON credit_card_movements FOR SELECT
  USING (credit_card_id IN (
    SELECT id FROM credit_cards
    WHERE user_id = NULLIF(current_setting('app.user_id', true), '')::integer
  ));

-- Non-superuser app role so RLS is enforced during tests
CREATE ROLE app_user WITH LOGIN PASSWORD 'app_test_pw';
GRANT USAGE ON SCHEMA public TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_user;
"""


@pytest.fixture(autouse=True)
def _patch_semantic(monkeypatch):
    """Ensure schema_builder uses test semantic model instead of fetching from SSM/S3."""
    import finance_query_agent.schema_builder as sb

    monkeypatch.setattr(sb, "_SEMANTIC", _TEST_SEMANTIC)


@pytest.fixture(scope="session")
def postgres_url():
    """Start a Postgres container, seed with test data, yield app_user URL (non-superuser for RLS)."""
    skip_without_docker()
    with PostgresContainer("postgres:16-alpine") as pg:
        admin_url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

        async def _seed():
            conn = await asyncpg.connect(admin_url)
            try:
                await conn.execute(SEED_SQL)
            finally:
                await conn.close()

        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_seed())

        # Yield the non-superuser URL so RLS policies are enforced
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        yield f"postgresql://app_user:app_test_pw@{host}:{port}/test"


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
