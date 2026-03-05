"""Asyncpg single connection with tenacity retry."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg  # type: ignore[import-untyped]
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from finance_query_agent.exceptions import DatabaseConnectionError, QueryTimeoutError

logger = logging.getLogger(__name__)


class Connection:
    """Single asyncpg connection wrapper (Lambda model — one connection per invocation)."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._conn: asyncpg.Connection | None = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((OSError, asyncpg.ConnectionFailureError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def connect(self) -> None:
        """Open the asyncpg connection with retry."""
        try:
            self._conn = await asyncpg.connect(
                self._db_url,
                timeout=10,
                server_settings={"statement_timeout": "30000"},
            )
        except (OSError, asyncpg.ConnectionFailureError):
            raise
        except asyncpg.PostgresError as exc:
            logger.error("Database connection failed: %s", exc)
            raise DatabaseConnectionError(str(exc)) from exc

    async def close(self) -> None:
        """Close the connection. Safe to call multiple times."""
        if self._conn is not None:
            conn = self._conn
            self._conn = None
            try:
                await conn.close()
            except Exception:
                logger.error("Failed to close database connection", exc_info=True)

    def _get_conn(self) -> Any:
        if self._conn is None:
            raise DatabaseConnectionError("Not connected — call connect() first")
        return self._conn

    async def execute(self, query: str, *args: object) -> str:
        """Execute a query and return the status string."""
        try:
            return await self._get_conn().execute(query, *args)  # type: ignore[no-any-return]
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Execute a query and return all rows."""
        try:
            return await self._get_conn().fetch(query, *args)  # type: ignore[no-any-return]
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Execute a query and return a single row."""
        try:
            return await self._get_conn().fetchrow(query, *args)
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc
