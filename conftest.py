"""Pytest configuration and shared fixtures."""

import pytest
import asyncio
from db.client import init_db, close_db


@pytest.fixture(scope="function")
async def db():
    """Initialize database connection pool for tests that need it.

    Usage: Add 'db' as a parameter to tests that need database access.
    Tests without this fixture will not connect to the database.
    """
    await init_db()
    yield
    await close_db()
