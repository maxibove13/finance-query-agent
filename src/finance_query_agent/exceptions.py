"""Exception hierarchy for finance-query-agent."""


class FinanceQueryError(Exception):
    """Base exception for all finance query agent errors."""


class SchemaValidationError(FinanceQueryError):
    """Schema mapping does not match the live database."""


class DatabaseConnectionError(FinanceQueryError):
    """Database connection error (creation, health, closure)."""


class QueryTimeoutError(FinanceQueryError):
    """A query exceeded the configured timeout."""


class LLMError(FinanceQueryError):
    """LLM API call failed (rate limit, auth, network, unexpected response)."""
