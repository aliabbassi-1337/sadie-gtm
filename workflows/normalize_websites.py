"""Normalize hotel website URLs in the database.

Fixes common data quality issues:
- Missing scheme (www.hotel.com → https://www.hotel.com)
- Emails stored as URLs (info@hotel.com → NULL)
- Spaces in URLs (http:// www.hotel.com → https://www.hotel.com)
- Placeholder URLs (no.website.com.au → NULL)
- HTTP → HTTPS upgrade

Usage:
    uv run python -m workflows.normalize_websites --dry-run
    uv run python -m workflows.normalize_websites
"""

import asyncio
import argparse
from urllib.parse import urlparse

from loguru import logger

from db.client import init_db, close_db, get_conn


PLACEHOLDER_PATTERNS = ["no.website", "nowebsite", "no-website"]
PLACEHOLDER_DOMAINS = {"none", "na", "null", "tba", "tbd", "unknown", "test"}


def normalize_url(url: str) -> str | None:
    """Normalize a website URL. Returns None if URL is garbage."""
    if not url:
        return None

    url = url.strip()
    if not url:
        return None

    # Reject emails (e.g. "info@kmva.com.au")
    if "@" in url and "//" not in url:
        return None

    # Remove spaces (e.g. "http:// www.gulfhaven.com.au")
    url = url.replace(" ", "")

    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    # Upgrade HTTP to HTTPS
    if url.startswith("http://"):
        url = "https://" + url[7:]

    # Parse and validate
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower().rstrip(".")
        if not host or "." not in host:
            return None

        # Check placeholder patterns
        for p in PLACEHOLDER_PATTERNS:
            if p in host:
                return None

        # Check placeholder domains (first part of host without www)
        host_base = host.replace("www.", "").split(".")[0]
        if host_base in PLACEHOLDER_DOMAINS:
            return None

    except Exception:
        return None

    return url


async def run(dry_run: bool = False):
    """Normalize all hotel website URLs."""
    await init_db()

    async with get_conn() as conn:
        hotels = await conn.fetch("""
            SELECT id, website
            FROM sadie_gtm.hotels
            WHERE website IS NOT NULL AND website != ''
        """)

    logger.info(f"Checking {len(hotels)} hotels with website data")

    fixes = []
    nullified = []

    for h in hotels:
        original = h["website"]
        normalized = normalize_url(original)

        if normalized is None:
            nullified.append({"id": h["id"], "original": original})
        elif normalized != original:
            fixes.append({"id": h["id"], "original": original, "normalized": normalized})

    logger.info(f"  URLs to normalize: {len(fixes)}")
    logger.info(f"  URLs to nullify (garbage): {len(nullified)}")

    for f in fixes[:20]:
        logger.info(f"  FIX: '{f['original']}' → '{f['normalized']}'")
    for n in nullified[:20]:
        logger.info(f"  NULL: '{n['original']}'")

    if dry_run:
        logger.info("Dry run — no changes made")
        await close_db()
        return

    if fixes:
        ids = [f["id"] for f in fixes]
        urls = [f["normalized"] for f in fixes]
        async with get_conn() as conn:
            await conn.execute("""
                UPDATE sadie_gtm.hotels AS h
                SET website = m.website, updated_at = NOW()
                FROM (
                    SELECT unnest($1::int[]) AS id,
                           unnest($2::text[]) AS website
                ) AS m
                WHERE h.id = m.id
            """, ids, urls)
        logger.info(f"  Normalized {len(fixes)} URLs")

    if nullified:
        ids = [n["id"] for n in nullified]
        async with get_conn() as conn:
            await conn.execute("""
                UPDATE sadie_gtm.hotels
                SET website = NULL, updated_at = NOW()
                WHERE id = ANY($1::int[])
            """, ids)
        logger.info(f"  Nullified {len(nullified)} garbage URLs")

    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize hotel website URLs")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))
