"""Tests for WHOIS history module."""

import pytest
import httpx
from lib.owner_discovery.whois_history import (
    whois_lookup,
    whois_to_decision_maker,
    _is_privacy,
    _is_junk_email,
)
from services.enrichment.owner_models import DomainIntel


@pytest.mark.no_db
class TestPrivacyDetection:

    def test_obvious_privacy(self):
        assert _is_privacy("Redacted for privacy") is True

    def test_whoisguard(self):
        assert _is_privacy("WhoisGuard Protected") is True

    def test_domains_by_proxy(self):
        assert _is_privacy("Domains By Proxy, LLC") is True

    def test_on_behalf_of(self):
        assert _is_privacy("On behalf of owner") is True

    def test_real_name(self):
        assert _is_privacy("John Smith") is False

    def test_none(self):
        assert _is_privacy(None) is True

    def test_empty(self):
        assert _is_privacy("") is True


@pytest.mark.no_db
class TestJunkEmail:

    def test_aws_trust_safety(self):
        assert _is_junk_email("trustandsafety@support.aws.com") is True

    def test_abuse_email(self):
        assert _is_junk_email("abuse@godaddy.com") is True

    def test_registrar_domain(self):
        assert _is_junk_email("contact@registrarmail.net") is True

    def test_real_email(self):
        assert _is_junk_email("owner@grandhotel.com") is False

    def test_none(self):
        assert _is_junk_email(None) is True


@pytest.mark.no_db
class TestWhoisToDecisionMaker:

    def test_valid_intel(self):
        intel = DomainIntel(
            domain="hotel.com",
            registrant_name="Carlos Diaz",
            registrant_email="carlos@hotel.com",
            is_privacy_protected=False,
            whois_source="live_whois",
        )
        dm = whois_to_decision_maker(intel)
        assert dm is not None
        assert dm.full_name == "Carlos Diaz"
        assert dm.email == "carlos@hotel.com"
        assert dm.source == "live_whois"

    def test_privacy_protected_returns_none(self):
        intel = DomainIntel(
            domain="hotel.com",
            registrant_name="Redacted for privacy",
            is_privacy_protected=True,
            whois_source="live_whois",
        )
        dm = whois_to_decision_maker(intel)
        assert dm is None

    def test_no_name_returns_none(self):
        intel = DomainIntel(
            domain="hotel.com",
            is_privacy_protected=False,
            whois_source="live_whois",
        )
        dm = whois_to_decision_maker(intel)
        assert dm is None


@pytest.mark.online
@pytest.mark.asyncio
async def test_whois_lookup_real_domain():
    """Live WHOIS for a known domain should return DomainIntel."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        intel = await whois_lookup(client, "google.com")
    assert intel is not None
    assert intel.domain == "google.com"
    assert intel.registrar is not None


@pytest.mark.online
@pytest.mark.asyncio
async def test_whois_lookup_nonexistent():
    """Nonexistent domain should return None."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        intel = await whois_lookup(client, "xz9q7k2m-nonexistent-99.com")
    assert intel is None
