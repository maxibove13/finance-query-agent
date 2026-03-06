"""Asyncpg connection pool with tenacity retry, cached across warm Lambda invocations."""

from __future__ import annotations

import asyncio
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

_pool: asyncpg.Pool | None = None


def _pool_is_usable(pool: asyncpg.Pool) -> bool:
    """Check if a cached pool is open and belongs to the current event loop."""
    if pool._closing or pool._closed:
        return False
    return pool._loop is asyncio.get_running_loop()


class Connection:
    """Asyncpg pool wrapper. The pool is cached at module level across warm Lambda invocations."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._pool: asyncpg.Pool | None = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((OSError, asyncpg.ConnectionFailureError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def connect(self) -> None:
        """Create or reuse the module-level connection pool."""
        global _pool  # noqa: PLW0603
        if _pool is not None and _pool_is_usable(_pool):
            self._pool = _pool
            return
        # Stale pool from a previous event loop — terminate its connections.
        if _pool is not None:
            logger.info("Terminating stale connection pool (loop mismatch)")
            _pool.terminate()
            _pool = None
        try:
            _pool = await asyncpg.create_pool(
                self._db_url,
                min_size=1,
                max_size=5,
                command_timeout=30,
                server_settings={"statement_timeout": "30000"},
            )
            self._pool = _pool
        except (OSError, asyncpg.ConnectionFailureError):
            raise
        except asyncpg.PostgresError as exc:
            logger.error("Database connection failed: %s", exc)
            raise DatabaseConnectionError(str(exc)) from exc

    async def close(self) -> None:
        """No-op. Pool persists across warm invocations."""

    def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise DatabaseConnectionError("Not connected — call connect() first")
        return self._pool

    async def execute(self, query: str, *args: object) -> str:
        """Execute a query and return the status string."""
        try:
            return await self._get_pool().execute(query, *args)  # type: ignore[no-any-return]
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Execute a query and return all rows."""
        try:
            return await self._get_pool().fetch(query, *args)  # type: ignore[no-any-return]
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Execute a query and return a single row."""
        try:
            return await self._get_pool().fetchrow(query, *args)
        except asyncpg.QueryCanceledError as exc:
            raise QueryTimeoutError(str(exc)) from exc
        except asyncpg.PostgresError as exc:
            raise DatabaseConnectionError(str(exc)) from exc
