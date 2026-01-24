"""
Ingestor registry - Plugin system for data source ingestors.
"""

from typing import Type, Dict, Callable, Optional, List
from services.ingestor.base import BaseIngestor


# Global registry of ingestors
_REGISTRY: Dict[str, Type[BaseIngestor]] = {}


def register(name: str) -> Callable[[Type[BaseIngestor]], Type[BaseIngestor]]:
    """
    Decorator to register an ingestor class.

    Usage:
        @register("my_source")
        class MyIngestor(BaseIngestor):
            ...
    """

    def decorator(cls: Type[BaseIngestor]) -> Type[BaseIngestor]:
        if name in _REGISTRY:
            raise ValueError(f"Ingestor '{name}' is already registered")
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_ingestor(name: str) -> Type[BaseIngestor]:
    """
    Get an ingestor class by name.

    Raises:
        ValueError: If ingestor is not registered
    """
    if name not in _REGISTRY:
        available = ", ".join(_REGISTRY.keys()) or "(none)"
        raise ValueError(f"Unknown ingestor: '{name}'. Available: {available}")
    return _REGISTRY[name]


def get_ingestor_or_none(name: str) -> Optional[Type[BaseIngestor]]:
    """Get an ingestor class by name, or None if not found."""
    return _REGISTRY.get(name)


def list_ingestors() -> List[str]:
    """List all registered ingestor names."""
    return list(_REGISTRY.keys())


def is_registered(name: str) -> bool:
    """Check if an ingestor is registered."""
    return name in _REGISTRY
