"""Pydantic models for tool return types."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


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


class RecurringExpense(BaseModel):
    merchant_name: str
    estimated_amount: Decimal
    frequency: str
    occurrences: int
    total_amount: Decimal
    currency: str
