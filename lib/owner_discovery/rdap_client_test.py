"""Tests for RDAP client."""

import pytest
import httpx
from lib.owner_discovery.rdap_client import rdap_to_decision_maker


@pytest.mark.online
@pytest.mark.asyncio
async def test_rdap_google_com():
    """google.com should return structured RDAP data (privacy-protected)."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        dm, intel = await rdap_to_decision_maker(client, "google.com")
    assert intel is not None
    assert intel.domain == "google.com"
    assert intel.registrar is not None


@pytest.mark.online
@pytest.mark.asyncio
async def test_rdap_nonexistent_domain():
    """Totally fake domain should return None."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        dm, intel = await rdap_to_decision_maker(client, "xz9q7k2m-nonexistent-99.com")
    # Either None or empty intel is acceptable
    assert dm is None


@pytest.mark.online
@pytest.mark.asyncio
async def test_rdap_returns_domain_intel_fields():
    """RDAP response should populate registrar and privacy status."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        dm, intel = await rdap_to_decision_maker(client, "example.com")
    assert intel is not None
    assert isinstance(intel.is_privacy_protected, bool)
    assert intel.whois_source == "rdap"
