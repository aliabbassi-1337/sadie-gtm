"""Pytest configuration and shared fixtures."""

import pytest
import asyncio
from db.client import init_db, close_db


@pytest.fixture(scope="function", autouse=True)
async def setup_db(request):
    """Initialize database connection pool for tests that need it.

    Tests marked with @pytest.mark.no_db will skip database initialization.
    """
    # Skip DB setup for tests marked with no_db
    if "no_db" in [marker.name for marker in request.node.iter_markers()]:
        yield
        return

    await init_db()
    yield
    await close_db()


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "no_db: mark test to skip database setup")
    config.addinivalue_line("markers", "integration: mark test as integration test (hits external services)")
    config.addinivalue_line("markers", "online: mark test as online test (hits external APIs)")