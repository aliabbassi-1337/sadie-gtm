"""Pytest configuration and shared fixtures."""

import pytest
import asyncio
from db.client import init_db, close_db


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def setup_db():
    """Initialize database connection pool for all tests."""
    await init_db()
    yield
    await close_db()
