"""Tests for ingestor registry."""

import pytest
from services.ingestor.registry import (
    register,
    get_ingestor,
    get_ingestor_or_none,
    list_ingestors,
    is_registered,
    _REGISTRY,
)
from services.ingestor.base import BaseIngestor


class TestRegistry:
    """Tests for ingestor registry functions."""

    @pytest.mark.no_db
    def test_list_ingestors_includes_builtin(self):
        """Built-in ingestors are registered."""
        ingestors = list_ingestors()

        assert "dbpr" in ingestors
        assert "texas" in ingestors

    @pytest.mark.no_db
    def test_get_ingestor_returns_class(self):
        """Get ingestor returns the class."""
        cls = get_ingestor("dbpr")

        assert cls is not None
        assert issubclass(cls, BaseIngestor)

    @pytest.mark.no_db
    def test_get_ingestor_raises_for_unknown(self):
        """Get ingestor raises ValueError for unknown source."""
        with pytest.raises(ValueError, match="Unknown ingestor"):
            get_ingestor("nonexistent_source")

    @pytest.mark.no_db
    def test_get_ingestor_or_none_returns_none(self):
        """Get ingestor or none returns None for unknown source."""
        result = get_ingestor_or_none("nonexistent_source")
        assert result is None

    @pytest.mark.no_db
    def test_get_ingestor_or_none_returns_class(self):
        """Get ingestor or none returns class for known source."""
        result = get_ingestor_or_none("dbpr")
        assert result is not None

    @pytest.mark.no_db
    def test_is_registered_true(self):
        """Is registered returns True for known source."""
        assert is_registered("dbpr") is True
        assert is_registered("texas") is True

    @pytest.mark.no_db
    def test_is_registered_false(self):
        """Is registered returns False for unknown source."""
        assert is_registered("nonexistent") is False
