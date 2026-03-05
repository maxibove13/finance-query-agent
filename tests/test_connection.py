"""Tests for connection.py — asyncpg connection with retry."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from finance_query_agent.connection import Connection
from finance_query_agent.exceptions import DatabaseConnectionError, QueryTimeoutError


@pytest.fixture
def conn():
    return Connection("postgresql://test:test@localhost/test")


class TestConnect:
    async def test_connect_success(self, conn):
        mock_connection = AsyncMock()
        with patch("finance_query_agent.connection.asyncpg.connect", return_value=mock_connection) as mock_connect:
            await conn.connect()
            mock_connect.assert_called_once_with(
                "postgresql://test:test@localhost/test",
                timeout=10,
                server_settings={"statement_timeout": "30000"},
            )
        assert conn._conn is mock_connection

    async def test_connect_retries_on_os_error(self, conn):
        mock_connection = AsyncMock()
        with patch(
            "finance_query_agent.connection.asyncpg.connect",
            side_effect=[OSError("conn refused"), OSError("conn refused"), mock_connection],
        ):
            await conn.connect()
        assert conn._conn is mock_connection

    async def test_connect_retries_on_connection_failure(self, conn):
        mock_connection = AsyncMock()
        with patch(
            "finance_query_agent.connection.asyncpg.connect",
            side_effect=[asyncpg.ConnectionFailureError("fail"), mock_connection],
        ):
            await conn.connect()
        assert conn._conn is mock_connection

    async def test_connect_gives_up_after_3_attempts(self, conn):
        with patch(
            "finance_query_agent.connection.asyncpg.connect",
            side_effect=OSError("conn refused"),
        ):
            with pytest.raises(OSError, match="conn refused"):
                await conn.connect()

    async def test_connect_wraps_postgres_error(self, conn):
        with patch(
            "finance_query_agent.connection.asyncpg.connect",
            side_effect=asyncpg.InvalidPasswordError("bad password"),
        ):
            with pytest.raises(DatabaseConnectionError, match="bad password"):
                await conn.connect()

    async def test_connect_logs_postgres_error(self, conn, caplog):
        with patch(
            "finance_query_agent.connection.asyncpg.connect",
            side_effect=asyncpg.InvalidPasswordError("bad password"),
        ):
            with caplog.at_level(logging.ERROR, logger="finance_query_agent.connection"):
                with pytest.raises(DatabaseConnectionError):
                    await conn.connect()
        assert "Database connection failed" in caplog.text


class TestClose:
    async def test_close_when_connected(self, conn):
        mock_connection = AsyncMock()
        conn._conn = mock_connection
        await conn.close()
        mock_connection.close.assert_called_once()
        assert conn._conn is None

    async def test_close_when_not_connected(self, conn):
        await conn.close()  # should not raise

    async def test_close_idempotent(self, conn):
        mock_connection = AsyncMock()
        conn._conn = mock_connection
        await conn.close()
        await conn.close()
        mock_connection.close.assert_called_once()

    async def test_close_swallows_and_logs_error(self, conn, caplog):
        mock_connection = AsyncMock()
        mock_connection.close.side_effect = OSError("connection reset")
        conn._conn = mock_connection
        with caplog.at_level(logging.ERROR, logger="finance_query_agent.connection"):
            await conn.close()  # should not raise
        assert conn._conn is None
        assert "Failed to close database connection" in caplog.text


class TestExecute:
    async def test_execute_success(self, conn):
        mock_connection = AsyncMock()
        mock_connection.execute.return_value = "INSERT 0 1"
        conn._conn = mock_connection
        result = await conn.execute("SELECT 1")
        assert result == "INSERT 0 1"
        mock_connection.execute.assert_called_once_with("SELECT 1")

    async def test_execute_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.execute("SELECT 1")

    async def test_execute_wraps_timeout(self, conn):
        mock_connection = AsyncMock()
        mock_connection.execute.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._conn = mock_connection
        with pytest.raises(QueryTimeoutError):
            await conn.execute("SELECT pg_sleep(999)")

    async def test_execute_wraps_postgres_error(self, conn):
        mock_connection = AsyncMock()
        mock_connection.execute.side_effect = asyncpg.UndefinedTableError("no such table")
        conn._conn = mock_connection
        with pytest.raises(DatabaseConnectionError):
            await conn.execute("SELECT * FROM nope")


class TestFetch:
    async def test_fetch_success(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetch.return_value = [{"id": 1}]
        conn._conn = mock_connection
        result = await conn.fetch("SELECT * FROM t")
        assert result == [{"id": 1}]

    async def test_fetch_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.fetch("SELECT 1")

    async def test_fetch_wraps_timeout(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetch.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._conn = mock_connection
        with pytest.raises(QueryTimeoutError):
            await conn.fetch("SELECT pg_sleep(999)")

    async def test_fetch_wraps_postgres_error(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetch.side_effect = asyncpg.UndefinedTableError("no such table")
        conn._conn = mock_connection
        with pytest.raises(DatabaseConnectionError):
            await conn.fetch("SELECT * FROM nope")


class TestFetchrow:
    async def test_fetchrow_success(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetchrow.return_value = {"id": 1}
        conn._conn = mock_connection
        result = await conn.fetchrow("SELECT * FROM t LIMIT 1")
        assert result == {"id": 1}

    async def test_fetchrow_none(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetchrow.return_value = None
        conn._conn = mock_connection
        result = await conn.fetchrow("SELECT * FROM t WHERE 1=0")
        assert result is None

    async def test_fetchrow_not_connected(self, conn):
        with pytest.raises(DatabaseConnectionError, match="Not connected"):
            await conn.fetchrow("SELECT 1")

    async def test_fetchrow_wraps_timeout(self, conn):
        mock_connection = AsyncMock()
        mock_connection.fetchrow.side_effect = asyncpg.QueryCanceledError("timeout")
        conn._conn = mock_connection
        with pytest.raises(QueryTimeoutError):
            await conn.fetchrow("SELECT pg_sleep(999)")
