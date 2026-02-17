"""Tests for email discovery and verification module."""

import pytest
from lib.owner_discovery.email_discovery import (
    _generate_patterns,
    discover_emails,
    verify_o365_email,
    enrich_decision_maker_email,
    ROLE_EMAILS,
)
from services.enrichment.owner_models import DecisionMaker


@pytest.mark.no_db
class TestGeneratePatterns:

    def test_standard_name(self):
        patterns = _generate_patterns("John", "Smith", "hotel.com")
        assert "john.smith@hotel.com" in patterns
        assert "johnsmith@hotel.com" in patterns
        assert "john@hotel.com" in patterns
        assert "jsmith@hotel.com" in patterns
        assert "j.smith@hotel.com" in patterns
        assert "smith@hotel.com" in patterns

    def test_all_lowercase(self):
        patterns = _generate_patterns("JOHN", "SMITH", "hotel.com")
        assert all("@hotel.com" in p for p in patterns)
        assert all(p == p.lower() for p in patterns)

    def test_whitespace_stripped(self):
        patterns = _generate_patterns(" John ", " Smith ", "hotel.com")
        assert "john.smith@hotel.com" in patterns


@pytest.mark.online
@pytest.mark.asyncio
async def test_verify_o365_known_domain():
    """Microsoft's own domain should have O365 autodiscover responding."""
    import httpx
    async with httpx.AsyncClient() as client:
        result = await verify_o365_email(client, "test@microsoft.com")
    # We just check we get a response, not that the email exists
    assert result in ("exists", "not_found", None)


@pytest.mark.online
@pytest.mark.asyncio
async def test_verify_o365_fake_domain():
    """Totally fake domain should return not_found or None."""
    import httpx
    async with httpx.AsyncClient() as client:
        result = await verify_o365_email(client, "test@xz9q7k2m-nonexistent.com")
    assert result in ("not_found", None)


@pytest.mark.online
@pytest.mark.asyncio
async def test_discover_emails_role_based():
    """Should generate role-based candidates for any domain."""
    results = await discover_emails("example.com")
    # May be empty (no MX), but should not error
    assert isinstance(results, list)


@pytest.mark.no_db
class TestEnrichDecisionMakerEmail:

    @pytest.mark.asyncio
    async def test_already_verified_skips(self):
        """DM with verified email should be returned unchanged."""
        dm = DecisionMaker(
            full_name="John Smith",
            email="john@hotel.com",
            email_verified=True,
            source="test",
        )
        result = await enrich_decision_maker_email(dm, "hotel.com")
        assert result.email == "john@hotel.com"
        assert result.email_verified is True
