"""Tests for owner enrichment repo functions (hits local DB)."""

import pytest
from services.enrichment.repo import (
    get_hotels_pending_owner_enrichment,
    insert_decision_maker,
    batch_insert_decision_makers,
    update_owner_enrichment_status,
    cache_domain_intel,
    cache_dns_intel,
    get_owner_enrichment_stats,
    get_decision_makers_for_hotel,
)
from services.enrichment.owner_models import DecisionMaker, DomainIntel
from services.leadgen.repo import insert_hotel, delete_hotel
from db.client import get_conn


# ── Helpers ──────────────────────────────────────────────────────────

async def _create_test_hotel(suffix: str = "") -> int:
    return await insert_hotel(
        name=f"Owner Test Hotel {suffix}",
        website=f"https://owner-test{suffix}.com",
        city="Miami",
        state="Florida",
        status=0,
        source="test",
    )


async def _cleanup_hotel(hotel_id: int):
    """Delete hotel and all related owner enrichment data."""
    async with get_conn() as conn:
        await conn.execute(
            "DELETE FROM sadie_gtm.hotel_decision_makers WHERE hotel_id = $1",
            hotel_id,
        )
        await conn.execute(
            "DELETE FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1",
            hotel_id,
        )
    await delete_hotel(hotel_id)


async def _cleanup_domain(domain: str):
    async with get_conn() as conn:
        await conn.execute(
            "DELETE FROM sadie_gtm.domain_whois_cache WHERE domain = $1", domain,
        )
        await conn.execute(
            "DELETE FROM sadie_gtm.domain_dns_cache WHERE domain = $1", domain,
        )


# ── Decision Maker CRUD ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_insert_decision_maker():
    hotel_id = await _create_test_hotel("dm1")
    try:
        dm = DecisionMaker(
            full_name="John Smith",
            title="Owner",
            email="john@owner-testdm1.com",
            source="test",
            confidence=0.9,
        )
        result = await insert_decision_maker(hotel_id, dm)
        assert result is not None
    finally:
        await _cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_insert_decision_maker_upsert_merges():
    """ON CONFLICT should merge: keep best email, highest confidence."""
    hotel_id = await _create_test_hotel("upsert")
    try:
        dm1 = DecisionMaker(
            full_name="Jane Doe",
            title="GM",
            email=None,
            source="rdap",
            confidence=0.5,
        )
        await insert_decision_maker(hotel_id, dm1)

        dm2 = DecisionMaker(
            full_name="Jane Doe",
            title="GM",
            email="jane@owner-testupsert.com",
            source="website_scrape",
            confidence=0.8,
        )
        await insert_decision_maker(hotel_id, dm2)

        dms = await get_decision_makers_for_hotel(hotel_id)
        assert len(dms) == 1
        assert dms[0]["email"] == "jane@owner-testupsert.com"
        assert float(dms[0]["confidence"]) == pytest.approx(0.8, abs=1e-5)
    finally:
        await _cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_batch_insert_decision_makers():
    hotel_id = await _create_test_hotel("batch")
    try:
        dms = [
            DecisionMaker(full_name="Alice", title="Owner", source="rdap", confidence=0.9),
            DecisionMaker(full_name="Bob", title="GM", source="website_scrape", confidence=0.7),
        ]
        count = await batch_insert_decision_makers(hotel_id, dms)
        assert count == 2

        rows = await get_decision_makers_for_hotel(hotel_id)
        assert len(rows) == 2
        names = {r["full_name"] for r in rows}
        assert names == {"Alice", "Bob"}
    finally:
        await _cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_decision_makers_ordered_by_confidence():
    hotel_id = await _create_test_hotel("order")
    try:
        dms = [
            DecisionMaker(full_name="Low", title="Staff", source="test", confidence=0.2),
            DecisionMaker(full_name="High", title="Owner", source="test", confidence=0.95),
            DecisionMaker(full_name="Mid", title="GM", source="test", confidence=0.6),
        ]
        await batch_insert_decision_makers(hotel_id, dms)

        rows = await get_decision_makers_for_hotel(hotel_id)
        confidences = [float(r["confidence"]) for r in rows]
        assert confidences == sorted(confidences, reverse=True)
    finally:
        await _cleanup_hotel(hotel_id)


# ── Enrichment Status ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_enrichment_status():
    hotel_id = await _create_test_hotel("status")
    try:
        await update_owner_enrichment_status(hotel_id, status=1, layers_completed=0b000101)

        async with get_conn() as conn:
            row = await conn.fetchrow(
                "SELECT status, layers_completed FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1",
                hotel_id,
            )
        assert row["status"] == 1
        assert row["layers_completed"] == 0b000101
    finally:
        await _cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_update_enrichment_status_ors_layers():
    """Layers should be OR'd, not replaced."""
    hotel_id = await _create_test_hotel("layers")
    try:
        await update_owner_enrichment_status(hotel_id, status=0, layers_completed=0b000001)
        await update_owner_enrichment_status(hotel_id, status=0, layers_completed=0b000100)

        async with get_conn() as conn:
            row = await conn.fetchrow(
                "SELECT layers_completed FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1",
                hotel_id,
            )
        assert row["layers_completed"] == 0b000101
    finally:
        await _cleanup_hotel(hotel_id)


# ── Domain Intel Caching ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cache_domain_intel():
    domain = "test-owner-whois.example"
    try:
        intel = DomainIntel(
            domain=domain,
            registrant_name="Test Owner",
            registrant_org="Test LLC",
            registrant_email="owner@test.com",
            is_privacy_protected=False,
            whois_source="test",
        )
        await cache_domain_intel(intel)

        async with get_conn() as conn:
            row = await conn.fetchrow(
                "SELECT registrant_name, registrant_org FROM sadie_gtm.domain_whois_cache WHERE domain = $1",
                domain,
            )
        assert row["registrant_name"] == "Test Owner"
        assert row["registrant_org"] == "Test LLC"
    finally:
        await _cleanup_domain(domain)


@pytest.mark.asyncio
async def test_cache_dns_intel():
    domain = "test-owner-dns.example"
    try:
        intel = DomainIntel(
            domain=domain,
            email_provider="google_workspace",
            mx_records=["aspmx.l.google.com"],
            soa_email="admin@test.com",
        )
        await cache_dns_intel(intel)

        async with get_conn() as conn:
            row = await conn.fetchrow(
                "SELECT email_provider, soa_email FROM sadie_gtm.domain_dns_cache WHERE domain = $1",
                domain,
            )
        assert row["email_provider"] == "google_workspace"
        assert row["soa_email"] == "admin@test.com"
    finally:
        await _cleanup_domain(domain)


# ── Pending Hotels Query ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_hotels_pending_owner_enrichment():
    hotel_id = await _create_test_hotel("pending")
    try:
        hotels = await get_hotels_pending_owner_enrichment(limit=5000)
        hotel_ids = [h["hotel_id"] for h in hotels]
        assert hotel_id in hotel_ids
    finally:
        await _cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_hotels_pending_excludes_complete():
    hotel_id = await _create_test_hotel("complete")
    try:
        await update_owner_enrichment_status(hotel_id, status=1, layers_completed=0xFF)

        hotels = await get_hotels_pending_owner_enrichment(limit=5000)
        hotel_ids = [h["hotel_id"] for h in hotels]
        assert hotel_id not in hotel_ids
    finally:
        await _cleanup_hotel(hotel_id)


# ── Stats ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_enrichment_stats():
    stats = await get_owner_enrichment_stats()
    assert isinstance(stats, dict)
    assert "total_with_website" in stats
    assert "total_contacts" in stats
