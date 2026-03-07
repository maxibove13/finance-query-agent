"""Schema mapping models — declarative config for how financial data is stored."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, model_validator


class ColumnRef(BaseModel):
    """Reference to a column on a joined table."""

    table: str
    column: str


class JoinDef(BaseModel):
    """Table join definition."""

    table: str
    on: str
    type: Literal["inner", "left"] = "left"


class AmountConvention(BaseModel):
    """How the DB distinguishes expenses from income.

    Exactly one convention must be set:
    - Option A: direction_column + expense_value + income_value
    - Option B: sign_means_expense
    """

    # Option A: separate direction column
    direction_column: str | None = None
    expense_value: str | None = None
    income_value: str | None = None

    # Option B: sign-based
    sign_means_expense: Literal["positive", "negative"] | None = None

    @model_validator(mode="after")
    def _validate_exactly_one_convention(self) -> AmountConvention:
        has_direction = self.direction_column is not None
        has_sign = self.sign_means_expense is not None

        if has_direction and has_sign:
            raise ValueError("Set either direction_column OR sign_means_expense, not both")
        if not has_direction and not has_sign:
            raise ValueError("Must set either direction_column or sign_means_expense")

        if has_direction:
            if not self.expense_value or not self.income_value:
                raise ValueError("direction_column requires both expense_value and income_value")

        return self


class TableMapping(BaseModel):
    """Mapping of a database table to its logical role."""

    table: str
    columns: dict[str, str | ColumnRef]
    joins: list[JoinDef] = []
    amount_convention: AmountConvention | None = None
    user_scoped: bool = True


class ViewMapping(BaseModel):
    """Mapping for a pre-computed database view (e.g. materialized view with pre-joined exchange rates)."""

    table: str
    columns: dict[str, str]  # logical key -> actual column name


class SchemaMapping(BaseModel):
    """Top-level schema configuration provided by the client."""

    transactions: TableMapping
    categories: TableMapping
    accounts: TableMapping
    secondary_transactions: TableMapping | None = None
    unified_expenses: ViewMapping | None = None
    unified_income: ViewMapping | None = None
    unified_balances: ViewMapping | None = None

    @model_validator(mode="after")
    def _validate_view_mappings(self) -> SchemaMapping:
        """Validate that view mappings have required logical keys."""
        view_required_keys: dict[str, set[str]] = {
            "unified_expenses": {"user_id", "date", "usd_amount", "local_amount", "category", "merchant"},
            "unified_income": {"user_id", "month", "usd_amount", "local_amount"},
            "unified_balances": {"user_id", "date", "usd_total", "local_total"},
        }
        for field_name, required_keys in view_required_keys.items():
            view: ViewMapping | None = getattr(self, field_name)
            if view is None:
                continue
            missing = required_keys - set(view.columns.keys())
            if missing:
                raise ValueError(f"{field_name} missing required column mappings: {missing}")
        return self

    @model_validator(mode="after")
    def _validate_transaction_tables(self) -> SchemaMapping:
        for name in ("transactions", "secondary_transactions"):
            table = getattr(self, name)
            if table is None:
                continue
            if table.amount_convention is None:
                raise ValueError(f"{name} requires amount_convention")
            required_keys = {"date", "amount", "description", "user_id", "currency", "account_id"}
            missing = required_keys - set(table.columns.keys())
            if missing:
                raise ValueError(f"{name} missing required column mappings: {missing}")
            # Validate ColumnRef entries have matching JoinDef
            join_tables = {j.table for j in table.joins}
            for key, col in table.columns.items():
                if isinstance(col, ColumnRef) and col.table not in join_tables:
                    raise ValueError(
                        f"{name}.columns.{key} references table '{col.table}' but no JoinDef exists for it"
                    )

        # Categories must have id and name
        cat_keys = set(self.categories.columns.keys())
        if not {"id", "name"} <= cat_keys:
            raise ValueError("categories table must have 'id' and 'name' column mappings")

        # Accounts must have id and user_id
        acc_keys = set(self.accounts.columns.keys())
        if not {"id", "user_id"} <= acc_keys:
            raise ValueError("accounts table must have 'id' and 'user_id' column mappings")

        return self
