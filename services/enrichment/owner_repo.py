"""Repository for owner enrichment database operations.

Result tracking and caching only — work distribution is handled by SQS.
All SQL lives in db/queries/owner_enrichment.sql (aiosql format).
"""

from datetime import datetime
from typing import Optional
from db.client import queries, get_conn
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
    """Get hotels that need owner enrichment."""
    limit = min(limit, MAX_QUERY_LIMIT)

    async with get_conn() as conn:
        if layer:
            rows = await queries.get_hotels_pending_owner_enrichment_by_layer(
                conn, layer=layer, limit=limit,
            )
        else:
            rows = await queries.get_hotels_pending_owner_enrichment(
                conn, limit=limit,
            )
        return [dict(r) for r in rows]


async def insert_decision_maker(hotel_id: int, dm: DecisionMaker) -> Optional[int]:
    """Insert a decision maker record. Returns ID or None on conflict."""
    async with get_conn() as conn:
        row = await queries.insert_decision_maker(
            conn,
            hotel_id=hotel_id,
            full_name=dm.full_name,
            title=dm.title,
            email=dm.email,
            email_verified=dm.email_verified,
            phone=dm.phone,
            source=dm.source,
            confidence=dm.confidence,
            raw_source_url=dm.raw_source_url,
        )
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
        await queries.update_enrichment_status(
            conn,
            hotel_id=hotel_id,
            status=status,
            layers_completed=layers_completed,
        )


async def cache_domain_intel(intel: DomainIntel) -> None:
    """Cache WHOIS/RDAP data for a domain."""
    async with get_conn() as conn:
        await queries.cache_domain_intel(
            conn,
            domain=intel.domain,
            registrant_name=intel.registrant_name,
            registrant_org=intel.registrant_org,
            registrant_email=intel.registrant_email,
            registrar=intel.registrar,
            registration_date=_parse_date(intel.registration_date),
            is_privacy_protected=intel.is_privacy_protected,
            source=intel.whois_source,
        )


async def cache_dns_intel(intel: DomainIntel) -> None:
    """Cache DNS intelligence for a domain."""
    async with get_conn() as conn:
        await queries.cache_dns_intel(
            conn,
            domain=intel.domain,
            email_provider=intel.email_provider,
            mx_records=intel.mx_records,
            soa_email=intel.soa_email,
            spf_record=intel.spf_record,
            dmarc_record=intel.dmarc_record,
            is_catch_all=intel.is_catch_all,
        )


async def get_enrichment_stats() -> dict:
    """Get owner enrichment pipeline statistics."""
    async with get_conn() as conn:
        row = await queries.get_enrichment_stats(conn)
        return dict(row) if row else {}


async def get_decision_makers_for_hotel(hotel_id: int) -> list[dict]:
    """Get all decision makers for a hotel, ordered by confidence."""
    async with get_conn() as conn:
        rows = await queries.get_decision_makers_for_hotel(
            conn, hotel_id=hotel_id,
        )
        return [dict(r) for r in rows]
