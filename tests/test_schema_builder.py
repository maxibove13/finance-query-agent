"""Tests for schema_builder — _render logic and semantic model rendering."""

from __future__ import annotations

from unittest.mock import MagicMock

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


def test_render_nullable_column():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "t",
                "dimensions": [{"name": "col", "data_type": "text", "nullable": True}],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "nullable" in result


def test_render_enum_column_with_sample_values():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "t",
                "dimensions": [
                    {
                        "name": "direction",
                        "data_type": "text",
                        "is_enum": True,
                        "sample_values": ["debit", "credit"],
                    }
                ],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "values: debit, credit" in result


def test_render_metric_with_filter():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "t",
                "dimensions": [],
                "metrics": [
                    {
                        "name": "total_expenses",
                        "expr": "SUM(amount)",
                        "description": "Total expenses",
                        "filter": {"expr": "movement_direction = 'debit'"},
                    }
                ],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "WHERE movement_direction = 'debit'" in result
    assert "total_expenses" in result


def test_render_relationships_with_join_columns():
    semantic = {
        "description": "Test DB",
        "tables": [],
        "relationships": [
            {
                "left_table": "account_movements",
                "right_table": "accounts",
                "join_type": "left",
                "relationship_columns": [{"left_column": "account_id", "right_column": "id"}],
            }
        ],
    }
    result = _render(semantic)
    assert "account_movements LEFT JOIN accounts ON account_movements.account_id = accounts.id" in result


def test_render_custom_instructions():
    semantic = {
        "description": "Test DB",
        "tables": [],
        "relationships": [],
        "custom_instructions": "Always use UTC.\nNever filter by user_id.",
    }
    result = _render(semantic)
    assert "business_rules:" in result
    assert "Always use UTC." in result
    assert "Never filter by user_id." in result


def test_render_table_description():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "accounts",
                "description": "User bank accounts",
                "dimensions": [],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "User bank accounts" in result


def test_render_time_dimensions_and_facts():
    semantic = {
        "description": "Test DB",
        "tables": [
            {
                "name": "t",
                "dimensions": [],
                "time_dimensions": [{"name": "issued_at", "data_type": "timestamp"}],
                "facts": [{"name": "amount", "data_type": "numeric"}],
            }
        ],
        "relationships": [],
    }
    result = _render(semantic)
    assert "issued_at" in result
    assert "amount" in result


def test_get_schema_context_caches(monkeypatch):
    """get_schema_context returns cached result on second call."""
    monkeypatch.setattr(sb, "_schema_context", None)
    monkeypatch.setattr(sb, "_SEMANTIC", _MINIMAL_SEMANTIC)
    result1 = sb.get_schema_context()
    result2 = sb.get_schema_context()
    assert result1 is result2
    # Cleanup
    monkeypatch.setattr(sb, "_schema_context", None)


def test_fetch_semantic_local_path(monkeypatch, tmp_path):
    """_fetch_semantic reads from local path when configured."""
    yaml_content = "description: Local\ntables: []\nrelationships: []\n"
    yaml_file = tmp_path / "model.yaml"
    yaml_file.write_text(yaml_content)

    mock_settings = MagicMock()
    mock_settings.semantic_model_local_path = str(yaml_file)
    monkeypatch.setattr("finance_query_agent.schema_builder.get_settings", lambda: mock_settings)

    result = sb._fetch_semantic()
    assert result["description"] == "Local"
