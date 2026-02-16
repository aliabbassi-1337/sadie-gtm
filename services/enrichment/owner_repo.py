"""Repository for owner enrichment database operations.

Result tracking and caching only — work distribution is handled by SQS.
"""

from datetime import datetime
from typing import Optional
from db.client import get_conn
from lib.owner_discovery.models import DecisionMaker, DomainIntel

MAX_QUERY_LIMIT = 5000


def _parse_date(val: Optional[str]) -> Optional[datetime]:
    """Parse ISO date string to datetime for asyncpg."""
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


async def get_hotels_pending_owner_enrichment(
    limit: int = 100,
    layer: Optional[int] = None,
) -> list[dict]:
    """Get hotels that need owner enrichment.

    Criteria:
    - Has a website (needed for most enrichment layers)
    - Not already completed (status != 1)
    - Optionally: hasn't completed a specific layer yet

    Returns list of dicts with hotel_id, name, website, city, state, country.
    """
    limit = min(limit, MAX_QUERY_LIMIT)

    async with get_conn() as conn:
        if layer:
            rows = await conn.fetch("""
                SELECT h.id as hotel_id, h.name, h.website, h.city, h.state, h.country,
                       h.email, h.phone_website, h.phone_google
                FROM hotels h
                LEFT JOIN hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
                WHERE h.website IS NOT NULL
                  AND h.website != ''
                  AND (hoe.hotel_id IS NULL OR hoe.layers_completed & $1 = 0)
                ORDER BY h.id
                LIMIT $2
            """, layer, limit)
        else:
            rows = await conn.fetch("""
                SELECT h.id as hotel_id, h.name, h.website, h.city, h.state, h.country,
                       h.email, h.phone_website, h.phone_google
                FROM hotels h
                LEFT JOIN hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
                WHERE h.website IS NOT NULL
                  AND h.website != ''
                  AND (hoe.hotel_id IS NULL OR hoe.status NOT IN (1))
                ORDER BY h.id
                LIMIT $1
            """, limit)
        return [dict(r) for r in rows]


async def insert_decision_maker(hotel_id: int, dm: DecisionMaker) -> Optional[int]:
    """Insert a decision maker record. Returns ID or None on conflict."""
    async with get_conn() as conn:
        row = await conn.fetchrow("""
            INSERT INTO hotel_decision_makers
                (hotel_id, full_name, title, email, email_verified, phone, source, confidence, raw_source_url)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (hotel_id, full_name, title) DO UPDATE
            SET email = COALESCE(NULLIF(EXCLUDED.email, ''), hotel_decision_makers.email),
                email_verified = EXCLUDED.email_verified OR hotel_decision_makers.email_verified,
                phone = COALESCE(NULLIF(EXCLUDED.phone, ''), hotel_decision_makers.phone),
                confidence = GREATEST(EXCLUDED.confidence, hotel_decision_makers.confidence),
                raw_source_url = COALESCE(EXCLUDED.raw_source_url, hotel_decision_makers.raw_source_url),
                updated_at = NOW()
            RETURNING id
        """, hotel_id, dm.full_name, dm.title, dm.email, dm.email_verified,
            dm.phone, dm.source, dm.confidence, dm.raw_source_url)
        return row["id"] if row else None


async def batch_insert_decision_makers(
    hotel_id: int,
    dms: list[DecisionMaker],
) -> int:
    """Insert multiple decision makers for a hotel. Returns count inserted."""
    count = 0
    for dm in dms:
        result = await insert_decision_maker(hotel_id, dm)
        if result:
            count += 1
    return count


async def update_enrichment_status(
    hotel_id: int,
    status: int,
    layers_completed: int,
) -> None:
    """Update the enrichment status for a hotel.

    Status: 0=pending, 1=complete, 2=no_results
    layers_completed: bitmask OR'd with existing value
    """
    async with get_conn() as conn:
        await conn.execute("""
            INSERT INTO hotel_owner_enrichment (hotel_id, status, layers_completed, last_attempt)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (hotel_id) DO UPDATE
            SET status = $2,
                layers_completed = hotel_owner_enrichment.layers_completed | $3,
                last_attempt = NOW()
        """, hotel_id, status, layers_completed)


async def cache_domain_intel(intel: DomainIntel) -> None:
    """Cache WHOIS/RDAP data for a domain."""
    async with get_conn() as conn:
        await conn.execute("""
            INSERT INTO domain_whois_cache
                (domain, registrant_name, registrant_org, registrant_email,
                 registrar, registration_date, is_privacy_protected, source, queried_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (domain) DO UPDATE
            SET registrant_name = COALESCE(EXCLUDED.registrant_name, domain_whois_cache.registrant_name),
                registrant_org = COALESCE(EXCLUDED.registrant_org, domain_whois_cache.registrant_org),
                registrant_email = COALESCE(EXCLUDED.registrant_email, domain_whois_cache.registrant_email),
                is_privacy_protected = EXCLUDED.is_privacy_protected,
                queried_at = NOW()
        """, intel.domain, intel.registrant_name, intel.registrant_org,
            intel.registrant_email, intel.registrar, _parse_date(intel.registration_date),
            intel.is_privacy_protected, intel.whois_source)


async def cache_dns_intel(intel: DomainIntel) -> None:
    """Cache DNS intelligence for a domain."""
    async with get_conn() as conn:
        await conn.execute("""
            INSERT INTO domain_dns_cache
                (domain, email_provider, mx_records, soa_email,
                 spf_record, dmarc_record, is_catch_all, queried_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (domain) DO UPDATE
            SET email_provider = EXCLUDED.email_provider,
                mx_records = EXCLUDED.mx_records,
                soa_email = EXCLUDED.soa_email,
                spf_record = EXCLUDED.spf_record,
                dmarc_record = EXCLUDED.dmarc_record,
                is_catch_all = EXCLUDED.is_catch_all,
                queried_at = NOW()
        """, intel.domain, intel.email_provider, intel.mx_records,
            intel.soa_email, intel.spf_record, intel.dmarc_record,
            intel.is_catch_all)


async def get_enrichment_stats() -> dict:
    """Get owner enrichment pipeline statistics."""
    async with get_conn() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE h.website IS NOT NULL AND h.website != '') as total_with_website,
                COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 0) as pending,
                COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 1) as complete,
                COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 2) as no_results,
                COUNT(DISTINCT hdm.hotel_id) as hotels_with_contacts,
                COUNT(hdm.id) as total_contacts,
                COUNT(hdm.id) FILTER (WHERE hdm.email_verified) as verified_emails
            FROM hotels h
            LEFT JOIN hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
            LEFT JOIN hotel_decision_makers hdm ON h.id = hdm.hotel_id
        """)
        return dict(row) if row else {}


async def get_decision_makers_for_hotel(hotel_id: int) -> list[dict]:
    """Get all decision makers for a hotel, ordered by confidence."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT id, full_name, title, email, email_verified, phone,
                   source, confidence, raw_source_url, created_at
            FROM hotel_decision_makers
            WHERE hotel_id = $1
            ORDER BY confidence DESC
        """, hotel_id)
        return [dict(r) for r in rows]
