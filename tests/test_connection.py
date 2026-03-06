"""Tests for connection.py — asyncpg pool with retry."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, PropertyMock, patch

import asyncpg
import pytest

import finance_query_agent.connection as conn_module
from finance_query_agent.connection import Connection
from finance_query_agent.exceptions import DatabaseConnectionError, QueryTimeoutError


@pytest.fixture(autouse=True)
def _reset_pool():
    """Reset module-level pool between tests."""
    conn_module._pool = None
    yield
    conn_module._pool = None


@pytest.fixture
def conn():
    return Connection("postgresql://test:test@localhost/test")


def _make_mock_pool():
    pool = AsyncMock()
    type(pool)._closing = PropertyMock(return_value=False)
    type(pool)._closed = PropertyMock(return_value=False)
    return pool


class TestConnect:
    async def test_connect_success(self, conn):
        mock_pool = _make_mock_pool()
        mock_create = AsyncMock(return_value=mock_pool)
        with patch("finance_query_agent.connection.asyncpg.create_pool", mock_create):
            await conn.connect()
            mock_create.assert_called_once_with(
                "postgresql://test:test@localhost/test",
                min_size=1,
                max_size=5,
                command_timeout=30,
                server_settings={"statement_timeout": "30000"},
            )
        assert conn._pool is mock_pool
        assert conn_module._pool is mock_pool

    async def test_connect_reuses_cached_pool(self, conn):
        mock_pool = _make_mock_pool()
        conn_module._pool = mock_pool
        with patch("finance_query_agent.connection.asyncpg.create_pool") as mock_create:
            await conn.connect()
            mock_create.assert_not_called()
        assert conn._pool is mock_pool

    async def test_connect_recreates_closed_pool(self, conn):
        old_pool = _make_mock_pool()
        type(old_pool)._closed = PropertyMock(return_value=True)
        conn_module._pool = old_pool

        new_pool = _make_mock_pool()
        with patch("finance_query_agent.connection.asyncpg.create_pool", AsyncMock(return_value=new_pool)):
            await conn.connect()
        assert conn._pool is new_pool
        assert conn_module._pool is new_pool

    async def test_connect_retries_on_os_error(self, conn):
        mock_pool = _make_mock_pool()
        with patch(
            "finance_query_agent.connection.asyncpg.create_pool",
            AsyncMock(side_effect=[OSError("conn refused"), OSError("conn refused"), mock_pool]),
        ):
            await conn.connect()
        assert conn._pool is mock_pool

    async def test_connect_retries_on_connection_failure(self, conn):
        mock_pool = _make_mock_pool()
        with patch(
            "finance_query_agent.connection.asyncpg.create_pool",
            AsyncMock(side_effect=[asyncpg.ConnectionFailureError("fail"), mock_pool]),
        ):
            await conn.connect()
        assert conn._pool is mock_pool

    async def test_connect_gives_up_after_3_attempts(self, conn):
        with patch(
            "finance_query_agent.connection.asyncpg.create_pool",
            AsyncMock(side_effect=OSError("conn refused")),
        ):
            with pytest.raises(OSError, match="conn refused"):
                await conn.connect()

    async def test_connect_wraps_postgres_error(self, conn):
        with patch(
            "finance_query_agent.connection.asyncpg.create_pool",
            AsyncMock(side_effect=asyncpg.InvalidPasswordError("bad password")),
        ):
            with pytest.raises(DatabaseConnectionError, match="bad password"):
                await conn.connect()

    async def test_connect_logs_postgres_error(self, conn, caplog):
        with patch(
            "finance_query_agent.connection.asyncpg.create_pool",
            AsyncMock(side_effect=asyncpg.InvalidPasswordError("bad password")),
        ):
            with caplog.at_level(logging.ERROR, logger="finance_query_agent.connection"):
                with pytest.raises(DatabaseConnectionError):
                    await conn.connect()
        assert "Database connection failed" in caplog.text


class TestClose:
    async def test_close_is_noop(self, conn):
        mock_pool = _make_mock_pool()
        conn._pool = mock_pool
        conn_module._pool = mock_pool
        await conn.close()
        # Pool should still be there
        assert conn_module._pool is mock_pool


class TestExecute:
    async def test_execute_success(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.execute.return_value = "INSERT 0 1"
        conn._pool = mock_pool
        result = await conn.execute("SELECT 1")
        assert result == "INSERT 0 1"
        mock_pool.execute.assert_called_once_with("SELECT 1")

    async def test_execute_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.execute("SELECT 1")

    async def test_execute_wraps_timeout(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.execute.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._pool = mock_pool
        with pytest.raises(QueryTimeoutError):
            await conn.execute("SELECT pg_sleep(999)")

    async def test_execute_wraps_postgres_error(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.execute.side_effect = asyncpg.UndefinedTableError("no such table")
        conn._pool = mock_pool
        with pytest.raises(DatabaseConnectionError):
            await conn.execute("SELECT * FROM nope")


class TestFetch:
    async def test_fetch_success(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetch.return_value = [{"id": 1}]
        conn._pool = mock_pool
        result = await conn.fetch("SELECT * FROM t")
        assert result == [{"id": 1}]

    async def test_fetch_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.fetch("SELECT 1")

    async def test_fetch_wraps_timeout(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetch.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._pool = mock_pool
        with pytest.raises(QueryTimeoutError):
            await conn.fetch("SELECT pg_sleep(999)")

    async def test_fetch_wraps_postgres_error(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetch.side_effect = asyncpg.UndefinedTableError("no such table")
        conn._pool = mock_pool
        with pytest.raises(DatabaseConnectionError):
            await conn.fetch("SELECT * FROM nope")


class TestFetchrow:
    async def test_fetchrow_success(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetchrow.return_value = {"id": 1}
        conn._pool = mock_pool
        result = await conn.fetchrow("SELECT * FROM t LIMIT 1")
        assert result == {"id": 1}

    async def test_fetchrow_none(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetchrow.return_value = None
        conn._pool = mock_pool
        result = await conn.fetchrow("SELECT * FROM t WHERE 1=0")
        assert result is None

    async def test_fetchrow_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.fetchrow("SELECT 1")

    async def test_fetchrow_wraps_timeout(self, conn):
        mock_pool = _make_mock_pool()
        mock_pool.fetchrow.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._pool = mock_pool
        with pytest.raises(QueryTimeoutError):
            await conn.fetchrow("SELECT pg_sleep(999)")
