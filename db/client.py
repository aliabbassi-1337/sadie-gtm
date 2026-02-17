import os
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import urlparse
import asyncpg
import aiosql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def _parse_database_url():
    """Parse DATABASE_URL into individual components."""
    url = os.getenv("DATABASE_URL")
    if not url:
        return None
    
    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }

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
        # Try DATABASE_URL first (Fargate/production), then individual vars (local dev)
        db_config = _parse_database_url()
        if db_config:
            host = db_config["host"]
            port = db_config["port"]
            database = db_config["database"]
            user = db_config["user"]
            password = db_config["password"]
        else:
            host = os.getenv("SADIE_DB_HOST")
            port = int(os.getenv("SADIE_DB_PORT", "5432"))
            database = os.getenv("SADIE_DB_NAME")
            user = os.getenv("SADIE_DB_USER")
            password = os.getenv("SADIE_DB_PASSWORD")
        
        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
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
        await conn.execute("SET search_path TO sadie_gtm, public")
        yield conn


@asynccontextmanager
async def get_transaction():
    """Get connection with transaction context."""
    pool = await init_db()
    async with pool.acquire() as conn:
        await conn.execute("SET search_path TO sadie_gtm, public")
        async with conn.transaction():
            yield conn


async def close_db():
    """Gracefully close all connections."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
