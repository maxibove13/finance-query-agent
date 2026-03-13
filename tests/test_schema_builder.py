"""Tests for schema_builder — _render logic and semantic patching."""

from __future__ import annotations

import pytest

import finance_query_agent.schema_builder as sb
from finance_query_agent.schema_builder import _render

_MINIMAL_SEMANTIC = {
    "description": "Test DB",
    "tables": [
        {
            "name": "account_movements",
            "dimensions": [{"name": "description", "synonyms": ["merchant"], "description": "Merchant name"}],
        }
    ],
    "relationships": [],
    "verified_queries": [{"question": "How much?", "sql": "SELECT 1"}],
}


@pytest.fixture(autouse=True)
def patch_semantic(monkeypatch):
    monkeypatch.setattr(sb, "_SEMANTIC", _MINIMAL_SEMANTIC)


def test_render_includes_introspected_columns():
    fake = {"accounts": [{"name": "id", "data_type": "integer", "nullable": False}]}
    result = _render(_MINIMAL_SEMANTIC, fake)
    assert "id" in result
    assert "integer" in result


def test_render_includes_semantic_synonyms():
    fake = {"account_movements": [{"name": "description", "data_type": "text", "nullable": True}]}
    result = _render(_MINIMAL_SEMANTIC, fake)
    assert "merchant" in result


def test_render_includes_verified_queries():
    result = _render(_MINIMAL_SEMANTIC, {})
    assert "verified_queries" in result


def test_render_includes_relationships_section():
    result = _render(_MINIMAL_SEMANTIC, {})
    assert "relationships:" in result
