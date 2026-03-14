"""DynamoDB conversation memory with Fernet encryption."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

from finance_query_agent.encryption import FieldEncryptor
from finance_query_agent.exceptions import ConversationConflictError

logger = logging.getLogger(__name__)


class ConversationMemory:
    """Loads and saves conversation history to DynamoDB with field-level encryption."""

    def __init__(self, table_name: str, region: str, encryptor: FieldEncryptor) -> None:
        self._table_name = table_name
        self._encryptor = encryptor
        self._dynamo = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamo.Table(table_name)

    async def load_history(self, user_id: str, session_id: str) -> tuple[list[ModelMessage], int]:
        """Load and decrypt conversation history. Returns (messages, version); version=0 if not found."""
        pk = f"USER#{user_id}"
        sk = f"SESSION#{session_id}"
        try:
            response = await asyncio.to_thread(
                self._table.get_item,
                Key={"PK": pk, "SK": sk},
            )
        except Exception:
            logger.error("DynamoDB load_history failed | user=%s session=%s", user_id, session_id)
            raise
        item = response.get("Item")
        if not item:
            return [], 0
        decrypted = self._encryptor.decrypt(item["messages_json"])
        version = int(item.get("version", 0))
        return list(ModelMessagesTypeAdapter.validate_json(decrypted)), version

    async def save_history(
        self, user_id: str, session_id: str, messages: list[ModelMessage], expected_version: int
    ) -> None:
        """Serialize, encrypt, and save conversation history with optimistic locking.

        Raises ConversationConflictError if the item was modified since it was loaded.
        """
        pk = f"USER#{user_id}"
        sk = f"SESSION#{session_id}"
        serialized = ModelMessagesTypeAdapter.dump_json(messages).decode()
        encrypted = self._encryptor.encrypt(serialized)
        now = datetime.now(UTC).isoformat()
        try:
            await asyncio.to_thread(
                self._table.update_item,
                Key={"PK": pk, "SK": sk},
                UpdateExpression=(
                    "SET user_id = :uid, messages_json = :msg, updated_at = :now"
                    ", created_at = if_not_exists(created_at, :now)"
                    ", version = :new_version"
                ),
                ConditionExpression="attribute_not_exists(PK) OR version = :expected OR attribute_not_exists(version)",
                ExpressionAttributeValues={
                    ":uid": user_id,
                    ":msg": encrypted,
                    ":now": now,
                    ":expected": expected_version,
                    ":new_version": expected_version + 1,
                },
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConversationConflictError(
                    f"Conversation for user {user_id} session {session_id} was modified concurrently"
                ) from e
            logger.error("DynamoDB save_history failed | user=%s session=%s", user_id, session_id)
            raise
        except Exception:
            logger.error("DynamoDB save_history failed | user=%s session=%s", user_id, session_id)
            raise
