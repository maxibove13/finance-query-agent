"""Tests for memory.py — DynamoDB conversation memory with encryption."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import boto3
import pytest
from cryptography.fernet import Fernet
from moto import mock_aws
from pydantic_ai.messages import ModelMessagesTypeAdapter, ModelRequest, ModelResponse, TextPart, UserPromptPart

from finance_query_agent.encryption import FieldEncryptor
from finance_query_agent.exceptions import ConversationConflictError
from finance_query_agent.memory import ConversationMemory


@pytest.fixture
def dynamodb_table():
    """Create a moto DynamoDB table for testing."""
    with mock_aws():
        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamo.create_table(
            TableName="test_conversations",
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "updated_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "user_id-index",
                    "KeySchema": [
                        {"AttributeName": "user_id", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


@pytest.fixture
def encryptor():
    key = Fernet.generate_key().decode()
    return FieldEncryptor(key)


@pytest.fixture
def dev_encryptor():
    return FieldEncryptor(key=None)


def _sample_messages():
    """Build a minimal Pydantic AI conversation."""
    return [
        ModelRequest(parts=[UserPromptPart(content="How much did I spend?")]),
        ModelResponse(parts=[TextPart(content="You spent $100 on groceries.")]),
    ]


class TestConversationMemory:
    async def test_load_empty_returns_empty_list(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages, version = await mem.load_history("user-1", "session-1")
        assert messages == []
        assert version == 0

    async def test_save_and_load_round_trip(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages = _sample_messages()
        await mem.save_history("user-1", "session-1", messages, expected_version=0)
        loaded, version = await mem.load_history("user-1", "session-1")
        assert len(loaded) == 2
        assert version == 1
        # Verify the content round-trips correctly
        original_json = ModelMessagesTypeAdapter.dump_json(messages)
        loaded_json = ModelMessagesTypeAdapter.dump_json(loaded)
        assert json.loads(original_json) == json.loads(loaded_json)

    async def test_data_is_encrypted_at_rest(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages = _sample_messages()
        await mem.save_history("user-1", "session-1", messages, expected_version=0)
        # Read raw item from DynamoDB
        response = dynamodb_table.get_item(Key={"PK": "USER#user-1", "SK": "SESSION#session-1"})
        raw = response["Item"]["messages_json"]
        # Should NOT be readable as plain JSON (it's encrypted)
        with pytest.raises(json.JSONDecodeError):
            json.loads(raw)

    async def test_dev_passthrough_no_encryption(self, dynamodb_table, dev_encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", dev_encryptor)
        messages = _sample_messages()
        await mem.save_history("user-1", "session-1", messages, expected_version=0)
        # Read raw item — should be plain JSON in dev mode
        response = dynamodb_table.get_item(Key={"PK": "USER#user-1", "SK": "SESSION#session-1"})
        raw = response["Item"]["messages_json"]
        parsed = json.loads(raw)
        assert isinstance(parsed, list)

    async def test_separate_sessions_are_isolated(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        msg1 = [ModelRequest(parts=[UserPromptPart(content="Question 1")])]
        msg2 = [ModelRequest(parts=[UserPromptPart(content="Question 2")])]
        await mem.save_history("user-1", "session-a", msg1, expected_version=0)
        await mem.save_history("user-1", "session-b", msg2, expected_version=0)
        loaded_a, _ = await mem.load_history("user-1", "session-a")
        loaded_b, _ = await mem.load_history("user-1", "session-b")
        assert len(loaded_a) == 1
        assert len(loaded_b) == 1

    async def test_separate_users_are_isolated(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        msg1 = [ModelRequest(parts=[UserPromptPart(content="User 1 question")])]
        await mem.save_history("user-1", "session-1", msg1, expected_version=0)
        loaded, version = await mem.load_history("user-2", "session-1")
        assert loaded == []
        assert version == 0

    async def test_item_has_metadata_fields(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        await mem.save_history("user-1", "session-1", _sample_messages(), expected_version=0)
        response = dynamodb_table.get_item(Key={"PK": "USER#user-1", "SK": "SESSION#session-1"})
        item = response["Item"]
        assert item["user_id"] == "user-1"
        assert "created_at" in item
        assert "updated_at" in item

    async def test_created_at_preserved_on_update(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        await mem.save_history("user-1", "session-1", _sample_messages(), expected_version=0)
        response = dynamodb_table.get_item(Key={"PK": "USER#user-1", "SK": "SESSION#session-1"})
        original_created = response["Item"]["created_at"]

        # Save again with updated version — created_at should not change
        updated_msgs = _sample_messages() + [ModelRequest(parts=[UserPromptPart(content="Follow-up")])]
        await mem.save_history("user-1", "session-1", updated_msgs, expected_version=1)
        response = dynamodb_table.get_item(Key={"PK": "USER#user-1", "SK": "SESSION#session-1"})
        assert response["Item"]["created_at"] == original_created

    async def test_conflict_raises_on_stale_version(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages = _sample_messages()
        # First write succeeds (version 0 → 1)
        await mem.save_history("user-1", "session-1", messages, expected_version=0)
        # Second write with stale version=0 must fail
        with pytest.raises(ConversationConflictError):
            await mem.save_history("user-1", "session-1", messages, expected_version=0)

    async def test_version_increments_on_each_save(self, dynamodb_table, encryptor):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages = _sample_messages()
        await mem.save_history("user-1", "session-1", messages, expected_version=0)
        _, v1 = await mem.load_history("user-1", "session-1")
        assert v1 == 1
        await mem.save_history("user-1", "session-1", messages, expected_version=1)
        _, v2 = await mem.load_history("user-1", "session-1")
        assert v2 == 2

    async def test_legacy_item_without_version_can_be_updated(self, dynamodb_table, encryptor):
        """Legacy DynamoDB items without a version attribute should be updatable with expected_version=0."""
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        messages = _sample_messages()
        serialized = ModelMessagesTypeAdapter.dump_json(messages).decode()
        encrypted = encryptor.encrypt(serialized)
        # Insert a legacy item without a version attribute
        dynamodb_table.put_item(
            Item={
                "PK": "USER#legacy-user",
                "SK": "SESSION#legacy-session",
                "user_id": "legacy-user",
                "messages_json": encrypted,
                "created_at": "2025-01-01T00:00:00+00:00",
                "updated_at": "2025-01-01T00:00:00+00:00",
            }
        )
        # Verify the item has no version
        response = dynamodb_table.get_item(Key={"PK": "USER#legacy-user", "SK": "SESSION#legacy-session"})
        assert "version" not in response["Item"]
        # save_history with expected_version=0 should succeed
        updated = _sample_messages() + [ModelRequest(parts=[UserPromptPart(content="Follow-up")])]
        await mem.save_history("legacy-user", "legacy-session", updated, expected_version=0)
        # After save, version should be 1
        loaded, version = await mem.load_history("legacy-user", "legacy-session")
        assert version == 1
        assert len(loaded) == 3


class TestMemoryErrorLogging:
    async def test_load_history_logs_on_dynamo_error(self, encryptor, caplog):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        mem._table = MagicMock()
        mem._table.get_item.side_effect = Exception("DynamoDB unreachable")
        with caplog.at_level(logging.ERROR, logger="finance_query_agent.memory"):
            with pytest.raises(Exception, match="DynamoDB unreachable"):
                await mem.load_history("user-1", "session-1")
        assert "DynamoDB load_history failed" in caplog.text
        assert "user-1" in caplog.text
        assert "session-1" in caplog.text

    async def test_save_history_logs_on_dynamo_error(self, encryptor, caplog):
        mem = ConversationMemory("test_conversations", "us-east-1", encryptor)
        mem._table = MagicMock()
        mem._table.update_item.side_effect = Exception("DynamoDB throttled")
        with caplog.at_level(logging.ERROR, logger="finance_query_agent.memory"):
            with pytest.raises(Exception, match="DynamoDB throttled"):
                await mem.save_history("user-1", "session-1", _sample_messages(), expected_version=0)
        assert "DynamoDB save_history failed" in caplog.text
        assert "user-1" in caplog.text
        assert "session-1" in caplog.text
