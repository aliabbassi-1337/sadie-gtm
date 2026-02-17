"""Tests for DNS intelligence module."""

import pytest
from lib.owner_discovery.dns_intel import (
    get_email_provider,
    get_soa_email,
    get_spf_record,
    get_dmarc_record,
    analyze_domain,
    _extract_domain,
)


@pytest.mark.no_db
class TestExtractDomain:

    def test_url_with_scheme(self):
        assert _extract_domain("https://www.example.com/about") == "example.com"

    def test_url_without_scheme(self):
        assert _extract_domain("www.example.com") == "example.com"

    def test_bare_domain(self):
        assert _extract_domain("example.com") == "example.com"

    def test_empty(self):
        assert _extract_domain("") is None

    def test_none(self):
        assert _extract_domain(None) is None

    def test_subdomain_preserved(self):
        assert _extract_domain("https://booking.hotel.com") == "booking.hotel.com"

    def test_www_stripped(self):
        assert _extract_domain("https://www.hotel.com") == "hotel.com"


@pytest.mark.online
@pytest.mark.asyncio
async def test_analyze_domain_google():
    """google.com should return Google Workspace as email provider."""
    intel = await analyze_domain("google.com")
    assert intel is not None
    assert intel.domain == "google.com"
    assert intel.email_provider == "google_workspace"
    assert len(intel.mx_records) > 0


@pytest.mark.online
@pytest.mark.asyncio
async def test_analyze_domain_microsoft():
    """outlook.com should return Microsoft 365 as email provider."""
    intel = await analyze_domain("outlook.com")
    assert intel is not None
    assert intel.email_provider == "microsoft_365"


@pytest.mark.online
@pytest.mark.no_db
def test_get_email_provider_google():
    provider, mx_hosts = get_email_provider("google.com")
    assert provider == "google_workspace"
    assert len(mx_hosts) > 0


@pytest.mark.online
@pytest.mark.no_db
def test_get_spf_record_google():
    spf = get_spf_record("google.com")
    assert spf is not None
    assert spf.startswith("v=spf1")


@pytest.mark.online
@pytest.mark.no_db
def test_get_dmarc_record_google():
    dmarc = get_dmarc_record("google.com")
    assert dmarc is not None
    assert "v=DMARC1" in dmarc


@pytest.mark.online
@pytest.mark.asyncio
async def test_analyze_domain_nonexistent():
    """Nonexistent domain should return None or empty intel."""
    intel = await analyze_domain("xz9q7k2m-nonexistent-99.com")
    # May return None or DomainIntel with no data
    if intel is not None:
        assert intel.email_provider is None or "other" in str(intel.email_provider)
