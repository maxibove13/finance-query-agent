"""Tests for audit.py — DynamoDB SQL audit logging."""

from __future__ import annotations

from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from finance_query_agent.audit import SqlAudit
from finance_query_agent.schemas.responses import TokenUsage, ToolCallRecord


@pytest.fixture
def audit_table():
    with mock_aws():
        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamo.create_table(
            TableName="test_audit",
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "user_id-created_at-index",
                    "KeySchema": [
                        {"AttributeName": "user_id", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


def _tool_calls(n: int = 1, row_count: int = 3) -> list[ToolCallRecord]:
    return [
        ToolCallRecord(
            tool_name="execute_sql",
            parameters={"sql": f"SELECT * FROM accounts -- {i}"},
            execution_time_ms=50 + i * 10,
            row_count=row_count,
        )
        for i in range(n)
    ]


def _token_usage() -> TokenUsage:
    return TokenUsage(input_tokens=100, output_tokens=50)


class TestSqlAudit:
    async def test_write_invocation_creates_item(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="42",
            session_id="session-abc",
            question="How much did I spend?",
            tool_calls=_tool_calls(1),
            token_usage=_token_usage(),
            total_ms=200,
            unresolved=False,
        )
        resp = audit_table.scan()
        assert resp["Count"] == 1
        item = resp["Items"][0]
        assert item["PK"] == "USER#42"
        assert item["SK"].startswith("INVOCATION#")
        assert item["user_id"] == "42"
        assert item["session_id"] == "session-abc"
        assert item["query_count"] == 1
        assert item["unresolved"] is False
        assert "ttl" in item
        assert "created_at" in item

    async def test_pii_scrubbed_in_question(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="My account 12345678 spent $50",
            tool_calls=_tool_calls(),
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert "12345678" not in item["question"]

    async def test_retried_true_when_multiple_tool_calls(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(2),
            token_usage=_token_usage(),
            total_ms=300,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert item["retried"] is True

    async def test_retried_false_when_single_tool_call(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(1),
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert item["retried"] is False

    async def test_any_empty_result_true_when_zero_rows(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(1, row_count=0),
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=True,
        )
        item = audit_table.scan()["Items"][0]
        assert item["any_empty_result"] is True

    async def test_any_empty_result_false_when_rows_present(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(1, row_count=5),
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert item["any_empty_result"] is False

    async def test_sql_queries_schema(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(1),
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert len(item["sql_queries"]) == 1
        q = item["sql_queries"][0]
        assert "sql" in q
        assert "row_count" in q
        assert "execution_time_ms" in q
        assert "empty_result" in q

    async def test_token_usage_stored(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=_tool_calls(),
            token_usage=TokenUsage(input_tokens=200, output_tokens=75),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert item["input_tokens"] == 200
        assert item["output_tokens"] == 75

    async def test_pii_scrubbed_in_sql(self, audit_table) -> None:
        audit = SqlAudit("test_audit", "us-east-1")
        await audit.write_invocation(
            user_id="1",
            session_id="s",
            question="test",
            tool_calls=[
                ToolCallRecord(
                    tool_name="execute_sql",
                    parameters={"sql": "SELECT * FROM account_movements WHERE description ILIKE '%12345678%' LIMIT 10"},
                    execution_time_ms=10,
                    row_count=0,
                )
            ],
            token_usage=_token_usage(),
            total_ms=100,
            unresolved=False,
        )
        item = audit_table.scan()["Items"][0]
        assert "12345678" not in item["sql_queries"][0]["sql"]

    async def test_propagates_dynamo_exception(self) -> None:
        """SqlAudit propagates exceptions — the handler wraps with try/except."""
        audit = SqlAudit("test_audit", "us-east-1")
        audit._table = MagicMock()
        audit._table.put_item.side_effect = Exception("DynamoDB throttled")
        with pytest.raises(Exception, match="DynamoDB throttled"):
            await audit.write_invocation(
                user_id="1",
                session_id="s",
                question="test",
                tool_calls=_tool_calls(),
                token_usage=_token_usage(),
                total_ms=100,
                unresolved=False,
            )
