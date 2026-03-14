"""Tests for schema_builder — _render logic and semantic model rendering."""

from __future__ import annotations

import pytest

import finance_query_agent.schema_builder as sb
from finance_query_agent.schema_builder import _render, get_allowed_tables

_MINIMAL_SEMANTIC = {
    "description": "Test DB",
    "tables": [
        {
            "name": "account_movements",
            "dimensions": [
                {
                    "name": "description",
                    "data_type": "text",
                    "synonyms": ["merchant"],
                    "description": "Merchant name",
                }
            ],
        }
    ],
    "relationships": [],
    "verified_queries": [{"question": "How much?", "sql": "SELECT 1"}],
}


@pytest.fixture(autouse=True)
def patch_semantic(monkeypatch):
    monkeypatch.setattr(sb, "_SEMANTIC", _MINIMAL_SEMANTIC)


def test_render_includes_columns():
    result = _render(_MINIMAL_SEMANTIC)
    assert "description" in result
    assert "text" in result


def test_render_includes_semantic_synonyms():
    result = _render(_MINIMAL_SEMANTIC)
    assert "merchant" in result


def test_render_includes_verified_queries():
    result = _render(_MINIMAL_SEMANTIC)
    assert "verified_queries" in result


def test_render_includes_relationships_section():
    result = _render(_MINIMAL_SEMANTIC)
    assert "relationships:" in result


def test_render_hides_private_columns():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "accounts",
                "dimensions": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"},
                    {"name": "user_id", "data_type": "integer", "description": "Owner", "access": "private"},
                    {"name": "alias", "data_type": "varchar(255)", "description": "Account name"},
                ],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "user_id" not in result
    assert "alias" in result
    assert "id" in result


def test_render_non_additive_metric():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "account_movements",
                "dimensions": [],
                "metrics": [
                    {
                        "name": "avg_balance",
                        "expr": "AVG(balance)",
                        "description": "Average balance",
                        "non_additive": True,
                    }
                ],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "[non-additive]" in result
    assert "avg_balance" in result


def test_get_allowed_tables_returns_table_names():
    result = get_allowed_tables()
    assert result == frozenset({"account_movements"})


def test_render_includes_filters():
    semantic = {
        "description": "Test DB",
        "tables": [],
        "relationships": [],
        "filters": [
            {
                "name": "subscriptions",
                "synonyms": ["recurring", "monthly subscriptions"],
                "expr": "description ILIKE '%netflix%'",
            },
            {
                "name": "dining_out",
                "description": "Restaurant expenses",
                "expr": "tags.name = 'restaurants'",
                "requires_join": "tags",
            },
        ],
    }
    result = _render(semantic)
    assert "filters:" in result
    assert "subscriptions: description ILIKE '%netflix%'" in result
    assert "synonyms: recurring, monthly subscriptions" in result
    assert "dining_out: tags.name = 'restaurants'" in result
    assert "Restaurant expenses" in result
    assert "requires JOIN to tags" in result
