"""Result models for unified view tools."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class ExpenseGroup(BaseModel):
    label: str
    total_amount: Decimal
    transaction_count: int
    currency: str


class IncomeMonth(BaseModel):
    month_label: str  # 'YYYY/MM'
    total_amount: Decimal
    currency: str


class BalanceSnapshot(BaseModel):
    date: date
    total_balance: Decimal | None = None
    currency_balances: dict[str, Decimal] | None = None
