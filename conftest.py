"""Pytest configuration and shared fixtures."""

import os
import pytest
import asyncio
from db.client import init_db, close_db

# Load env vars
from dotenv import load_dotenv
load_dotenv()


# =============================================================================
# SAFETY CHECK: Prevent tests from running against production database
# =============================================================================

ALLOWED_DB_HOSTS = {"localhost", "127.0.0.1", "host.docker.internal", "db", "postgres"}


def pytest_configure(config):
    """Register custom markers and check database safety."""
    config.addinivalue_line("markers", "no_db: mark test to skip database setup")
    config.addinivalue_line("markers", "integration: mark test as integration test (hits external services)")
    config.addinivalue_line("markers", "online: mark test as online test (hits external APIs)")
    
    # Check database host
    db_host = os.getenv("SADIE_DB_HOST", "localhost")
    
    if db_host not in ALLOWED_DB_HOSTS:
        pytest.exit(
            f"\n\n"
            f"{'=' * 60}\n"
            f"SAFETY CHECK FAILED: Cannot run tests against production DB!\n"
            f"{'=' * 60}\n"
            f"\n"
            f"Current SADIE_DB_HOST: {db_host}\n"
            f"Allowed hosts: {', '.join(sorted(ALLOWED_DB_HOSTS))}\n"
            f"\n"
            f"To run tests, set SADIE_DB_HOST to 'localhost' in your .env\n"
            f"{'=' * 60}\n",
            returncode=1,
        )


# =============================================================================
# Fixtures
# =============================================================================

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
