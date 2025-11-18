import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import uuid

logger = logging.getLogger(__name__)


class Message:
    """Класс сообщения для сериализации/десериализации"""

    def __init__(self, content: str, message_type: str = "default",
                 priority: int = 1, metadata: Optional[Dict[str, Any]] = None):
        self.message_id = str(uuid.uuid4())
        self.timestamp = datetime.now().isoformat()
        self.content = content
        self.message_type = message_type
        self.priority = priority
        self.metadata = metadata or {}
        self.version = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь"""
        return {
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "content": self.content,
            "message_type": self.message_type,
            "priority": self.priority,
            "metadata": self.metadata,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Десериализация из словаря"""
        try:
            message = cls(
                content=data.get("content", ""),
                message_type=data.get("message_type", "default"),
                priority=data.get("priority", 1),
                metadata=data.get("metadata", {})
            )
            # Перезаписываем автоматически генерируемые поля
            message.message_id = data.get("message_id", message.message_id)
            message.timestamp = data.get("timestamp", message.timestamp)
            message.version = data.get("version", message.version)
            return message
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            raise


class MessageSerializer:
    """Утилиты для сериализации и десериализации сообщений"""

    @staticmethod
    def serialize(message: Message) -> str:
        """Сериализация сообщения в JSON строку"""
        try:
            return json.dumps(message.to_dict(), ensure_ascii=False)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise

    @staticmethod
    def deserialize(json_str: str) -> Message:
        """Десериализация JSON строки в сообщение"""
        try:
            data = json.loads(json_str)
            return Message.from_dict(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            raise
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise

    @staticmethod
    def validate_message_data(data: Dict[str, Any]) -> bool:
        """Валидация данных сообщения"""
        required_fields = ["content", "message_id", "timestamp"]
        return all(field in data for field in required_fields)