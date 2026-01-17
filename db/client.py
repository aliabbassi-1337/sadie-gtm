import os
from pathlib import Path
from contextlib import asynccontextmanager
import asyncpg
import aiosql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load queries from SQL files
queries = aiosql.from_path(
    Path(__file__).parent / "queries",
    "asyncpg",
)

# Global connection pool
_pool = None


async def _init_connection(conn):
    """Initialize each connection with search_path (works with Supavisor)."""
    await conn.execute("SET search_path TO sadie_gtm, public")


async def init_db():
    """Initialize connection pool once at startup."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=os.getenv("SADIE_DB_HOST"),
            port=int(os.getenv("SADIE_DB_PORT", "5432")),
            database=os.getenv("SADIE_DB_NAME"),
            user=os.getenv("SADIE_DB_USER"),
            password=os.getenv("SADIE_DB_PASSWORD"),
            min_size=1,
            max_size=10,  # Transaction pooling mode allows more connections
            command_timeout=60,
            max_inactive_connection_lifetime=300,
            statement_cache_size=0,  # Required for Supavisor transaction mode (port 6543)
            init=_init_connection,  # Set search_path on each connection init
        )
    return _pool


@asynccontextmanager
async def get_conn():
    """Get connection from pool (recommended pattern from asyncpg docs)."""
    pool = await init_db()
    async with pool.acquire() as conn:
        yield conn


@asynccontextmanager
async def get_transaction():
    """Get connection with transaction context."""
    pool = await init_db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn


async def close_db():
    """Gracefully close all connections."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
