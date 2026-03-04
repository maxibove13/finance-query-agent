"""DynamoDB conversation memory with Fernet encryption."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import boto3  # type: ignore[import-untyped]
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

from finance_query_agent.encryption import FieldEncryptor


class ConversationMemory:
    """Loads and saves conversation history to DynamoDB with field-level encryption."""

    def __init__(self, table_name: str, region: str, encryptor: FieldEncryptor) -> None:
        self._table_name = table_name
        self._encryptor = encryptor
        self._dynamo = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamo.Table(table_name)

    async def load_history(self, user_id: str, session_id: str) -> list[ModelMessage]:
        """Load and decrypt conversation history. Returns empty list if not found."""
        pk = f"USER#{user_id}"
        sk = f"SESSION#{session_id}"
        response = await asyncio.to_thread(
            self._table.get_item,
            Key={"PK": pk, "SK": sk},
        )
        item = response.get("Item")
        if not item:
            return []
        decrypted = self._encryptor.decrypt(item["messages_json"])
        return list(ModelMessagesTypeAdapter.validate_json(decrypted))

    async def save_history(self, user_id: str, session_id: str, messages: list[ModelMessage]) -> None:
        """Serialize, encrypt, and save conversation history."""
        pk = f"USER#{user_id}"
        sk = f"SESSION#{session_id}"
        serialized = ModelMessagesTypeAdapter.dump_json(messages).decode()
        encrypted = self._encryptor.encrypt(serialized)
        now = datetime.now(UTC).isoformat()
        await asyncio.to_thread(
            self._table.update_item,
            Key={"PK": pk, "SK": sk},
            UpdateExpression=(
                "SET user_id = :uid, messages_json = :msg, updated_at = :now"
                ", created_at = if_not_exists(created_at, :now)"
            ),
            ExpressionAttributeValues={
                ":uid": user_id,
                ":msg": encrypted,
                ":now": now,
            },
        )
