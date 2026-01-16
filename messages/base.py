"""Base message class and handler registry for SQS message processing.

This module provides the foundation for type-safe SQS message handling:

- Message: Base dataclass for all SQS messages
- @handler: Decorator to register async handlers for message types
- HandlerRegistry: Central registry that maps message types to handlers

Example:
    @dataclass
    class ScrapeCity(Message):
        queue = "scrape-queue"
        city: str
        state: str
        country: str = "usa"

    @handler(ScrapeCity)
    async def handle_scrape_city(msg: ScrapeCity):
        await scrape_city(msg.city, msg.state, msg.country)
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, asdict
from typing import TypeVar, Callable, Awaitable, Type, Any, ClassVar, Optional, Dict, List, Tuple
from abc import ABC
import json

# Type variable for message classes
M = TypeVar("M", bound="Message")

# Handler function type
HandlerFunc = Callable[[M], Awaitable[Any]]


@dataclass
class Message(ABC):
    """Base class for all SQS messages.

    Subclasses must define:
    - queue: ClassVar[str] - The SQS queue name/identifier for this message type
    - Any additional fields for the message payload

    The message type is automatically serialized as '_type' in the JSON payload.
    """

    # Subclasses must override this
    queue: ClassVar[str] = ""

    def to_dict(self) -> dict:
        """Serialize message to dict for SQS.

        Includes '_type' field with the class name for deserialization.
        """
        data = asdict(self)
        data["_type"] = self.__class__.__name__
        return data

    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls: Type[M], data: dict) -> M:
        """Deserialize message from dict.

        Ignores '_type' field and any unknown fields.
        """
        # Get field names for this class
        field_names = {f.name for f in fields(cls)}

        # Filter to only known fields, excluding _type
        filtered = {k: v for k, v in data.items() if k in field_names and k != "_type"}

        return cls(**filtered)

    @classmethod
    def from_json(cls: Type[M], json_str: str) -> M:
        """Deserialize message from JSON string."""
        return cls.from_dict(json.loads(json_str))


class HandlerRegistry:
    """Registry that maps message types to their handlers.

    This is a singleton that stores all registered handlers.
    Use the @handler decorator instead of calling this directly.
    """

    _handlers: Dict[str, Tuple[Type[Message], HandlerFunc]] = {}
    _message_types: Dict[str, Type[Message]] = {}

    @classmethod
    def register(cls, message_cls: Type[M], handler_func: HandlerFunc[M]) -> None:
        """Register a handler for a message type."""
        type_name = message_cls.__name__
        cls._handlers[type_name] = (message_cls, handler_func)
        cls._message_types[type_name] = message_cls

    @classmethod
    def get_handler(cls, type_name: str) -> Optional[Tuple[Type[Message], HandlerFunc]]:
        """Get the message class and handler for a type name."""
        return cls._handlers.get(type_name)

    @classmethod
    def get_message_class(cls, type_name: str) -> Optional[Type[Message]]:
        """Get the message class for a type name."""
        return cls._message_types.get(type_name)

    @classmethod
    def parse_message(cls, data: dict) -> Message:
        """Parse a dict into the appropriate Message subclass.

        Raises:
            ValueError: If _type is missing or unknown.
        """
        type_name = data.get("_type")
        if not type_name:
            raise ValueError("Message missing '_type' field")

        message_cls = cls._message_types.get(type_name)
        if not message_cls:
            raise ValueError(f"Unknown message type: {type_name}")

        return message_cls.from_dict(data)

    @classmethod
    def list_handlers(cls) -> List[str]:
        """List all registered message types."""
        return list(cls._handlers.keys())


def handler(message_cls: Type[M]) -> Callable[[HandlerFunc[M]], HandlerFunc[M]]:
    """Decorator to register a handler for a message type.

    Example:
        @handler(ScrapeCity)
        async def handle_scrape_city(msg: ScrapeCity):
            await scrape_city(msg.city, msg.state, msg.country)
    """

    def decorator(func: HandlerFunc[M]) -> HandlerFunc[M]:
        HandlerRegistry.register(message_cls, func)
        return func

    return decorator
