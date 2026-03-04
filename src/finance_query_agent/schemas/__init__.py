"""Schema models for finance-query-agent."""

from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)
from finance_query_agent.schemas.responses import AgentResponse, TokenUsage, ToolCallRecord
from finance_query_agent.schemas.tool_results import (
    AccountSummary,
    CategoryBreakdown,
    CategorySpending,
    MerchantSpending,
    MonthlyTotal,
    PeriodComparison,
    RecurringExpense,
    Transaction,
    TransactionSearchResult,
    TrendPoint,
)

__all__ = [
    "AccountSummary",
    "AgentResponse",
    "AmountConvention",
    "CategoryBreakdown",
    "CategorySpending",
    "ColumnRef",
    "JoinDef",
    "MerchantSpending",
    "MonthlyTotal",
    "PeriodComparison",
    "RecurringExpense",
    "SchemaMapping",
    "TableMapping",
    "TokenUsage",
    "ToolCallRecord",
    "Transaction",
    "TransactionSearchResult",
    "TrendPoint",
]
