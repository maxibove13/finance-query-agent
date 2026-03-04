"""Pydantic models for tool return types."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class CategorySpending(BaseModel):
    category: str
    total_amount: Decimal
    transaction_count: int
    currency: str


class MonthlyTotal(BaseModel):
    year: int
    month: int
    total_amount: Decimal
    transaction_count: int
    currency: str


class MerchantSpending(BaseModel):
    merchant_name: str
    total_amount: Decimal
    transaction_count: int
    currency: str


class PeriodComparison(BaseModel):
    group_label: str
    currency: str
    period_a_total: Decimal
    period_b_total: Decimal
    absolute_change: Decimal
    percentage_change: float | None  # None when period_a_total is 0


class Transaction(BaseModel):
    date: date
    amount: Decimal
    description: str
    currency: str
    category: str | None


class TransactionSearchResult(BaseModel):
    transactions: list[Transaction]
    total_count: int
    has_more: bool


class CategoryBreakdown(BaseModel):
    category: str
    total_amount: Decimal
    percentage: float
    currency: str


class TrendPoint(BaseModel):
    period_label: str
    total_amount: Decimal
    transaction_count: int
    currency: str


class AccountSummary(BaseModel):
    account_name: str | None
    latest_balance: Decimal
    last_transaction_date: date
    currency: str


class RecurringExpense(BaseModel):
    merchant_name: str
    estimated_amount: Decimal
    frequency: str
    occurrences: int
    total_amount: Decimal
    currency: str
