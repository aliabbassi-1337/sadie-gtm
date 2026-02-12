"""Tests for Archive Slug Discovery."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from lib.archive.discovery import (
    ArchiveSlugDiscovery,
    BookingEnginePattern,
    DiscoveredSlug,
    BOOKING_ENGINE_PATTERNS,
)


@pytest.mark.no_db
class TestBookingEnginePatterns:
    """Tests for booking engine pattern definitions."""

    def test_has_rms_pattern(self):
        """Should have RMS pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "rms" in names

    def test_has_cloudbeds_pattern(self):
        """Should have Cloudbeds pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "cloudbeds" in names

    def test_has_mews_pattern(self):
        """Should have Mews pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "mews" in names

    def test_has_siteminder_pattern(self):
        """Should have SiteMinder pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "siteminder" in names

    def test_has_siteminder_directbook_pattern(self):
        """Should have SiteMinder direct-book pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "siteminder_directbook" in names

    def test_siteminder_directbook_extracts_slug(self):
        """Should extract slug from direct-book.com URL."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_directbook")
        discovery = ArchiveSlugDiscovery()
        slug = discovery._extract_slug(
            "https://direct-book.com/properties/my-hotel-resort", pattern.slug_regex
        )
        assert slug == "my-hotel-resort"

    def test_siteminder_directbook_has_domains(self):
        """siteminder_directbook should have domains configured."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_directbook")
        assert pattern.domains == ["direct-book.com"]

    def test_all_patterns_have_required_fields(self):
        """All patterns should have required fields."""
        for pattern in BOOKING_ENGINE_PATTERNS:
            assert pattern.name, "Pattern should have name"
            assert pattern.wayback_url_pattern, "Pattern should have wayback URL"
            assert pattern.slug_regex, "Pattern should have slug regex"
            assert pattern.commoncrawl_url_pattern, "Pattern should have CC URL"

    def test_all_major_patterns_have_domains(self):
        """All major engine patterns should have domains populated."""
        for pattern in BOOKING_ENGINE_PATTERNS:
            assert pattern.domains, f"{pattern.name} should have domains"

    def test_domains_field_defaults_to_empty(self):
        """domains field should default to empty list for backward compat."""
        p = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
        )
        assert p.domains == []


@pytest.mark.no_db
class TestArchiveSlugDiscoveryDeduplication:
    """Tests for slug deduplication logic."""
    
    def test_deduplicates_exact_matches(self):
        """Should remove exact duplicate slugs."""
        discovery = ArchiveSlugDiscovery()
        
        slugs = [
            DiscoveredSlug(engine="rms", slug="12345", source_url="url1", archive_source="wayback"),
            DiscoveredSlug(engine="rms", slug="12345", source_url="url2", archive_source="commoncrawl"),
            DiscoveredSlug(engine="rms", slug="67890", source_url="url3", archive_source="wayback"),
        ]
        
        unique = discovery._dedupe_slugs(slugs)
        
        assert len(unique) == 2
        assert unique[0].slug == "12345"
        assert unique[1].slug == "67890"
    
    def test_case_insensitive_deduplication(self):
        """Should deduplicate case-insensitively."""
        discovery = ArchiveSlugDiscovery()
        
        slugs = [
            DiscoveredSlug(engine="rms", slug="ABC123", source_url="url1", archive_source="wayback"),
            DiscoveredSlug(engine="rms", slug="abc123", source_url="url2", archive_source="commoncrawl"),
            DiscoveredSlug(engine="rms", slug="Abc123", source_url="url3", archive_source="wayback"),
        ]
        
        unique = discovery._dedupe_slugs(slugs)
        
        assert len(unique) == 1
        # Should keep the first occurrence
        assert unique[0].slug == "ABC123"
    
    def test_preserves_first_occurrence(self):
        """Should preserve the first occurrence when deduplicating."""
        discovery = ArchiveSlugDiscovery()
        
        slugs = [
            DiscoveredSlug(engine="rms", slug="test", source_url="first_url", archive_source="wayback", timestamp="2024"),
            DiscoveredSlug(engine="rms", slug="TEST", source_url="second_url", archive_source="commoncrawl", timestamp="2025"),
        ]
        
        unique = discovery._dedupe_slugs(slugs)
        
        assert len(unique) == 1
        assert unique[0].source_url == "first_url"
        assert unique[0].timestamp == "2024"
    
    def test_handles_empty_list(self):
        """Should handle empty slug list."""
        discovery = ArchiveSlugDiscovery()
        
        unique = discovery._dedupe_slugs([])
        
        assert unique == []


@pytest.mark.no_db
class TestArchiveSlugDiscoveryExtractSlug:
    """Tests for slug extraction from URLs."""
    
    def test_extracts_rms_numeric_slug(self):
        """Should extract numeric RMS slug."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://bookings12.rmscloud.com/Search/Index/12345/90/"
        slug = discovery._extract_slug(url, r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+)/")
        
        assert slug == "12345"
    
    def test_extracts_rms_hex_slug(self):
        """Should extract hex RMS slug."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://bookings12.rmscloud.com/Rates/Index/4FF68C2A213D0E23/1"
        slug = discovery._extract_slug(url, r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+)/")
        
        assert slug == "4FF68C2A213D0E23"
    
    def test_extracts_cloudbeds_slug(self):
        """Should extract Cloudbeds slug."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://hotels.cloudbeds.com/reservation/abc-hotel-123"
        slug = discovery._extract_slug(url, r"/reservation/([A-Za-z0-9_-]+)")
        
        assert slug == "abc-hotel-123"
    
    def test_extracts_mews_uuid(self):
        """Should extract Mews UUID slug."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://app.mews.com/distributor/12345678-1234-1234-1234-123456789abc"
        slug = discovery._extract_slug(url, r"/distributor/([a-f0-9-]{36})")
        
        assert slug == "12345678-1234-1234-1234-123456789abc"
    
    def test_returns_none_for_no_match(self):
        """Should return None when pattern doesn't match."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://example.com/no-match"
        slug = discovery._extract_slug(url, r"/reservation/([A-Za-z0-9_-]+)")
        
        assert slug is None
    
    def test_handles_url_encoded_characters(self):
        """Should handle URL-encoded characters."""
        discovery = ArchiveSlugDiscovery()
        
        url = "https://hotels.cloudbeds.com/reservation/hotel%20name%20test"
        slug = discovery._extract_slug(url, r"/reservation/([A-Za-z0-9_\s-]+)")
        
        # URL should be decoded
        assert slug is not None


@pytest.mark.no_db
class TestDiscoveredSlugModel:
    """Tests for DiscoveredSlug Pydantic model."""
    
    def test_creates_valid_slug(self):
        """Should create valid DiscoveredSlug."""
        slug = DiscoveredSlug(
            engine="rms",
            slug="12345",
            source_url="https://example.com",
            archive_source="wayback",
        )
        
        assert slug.engine == "rms"
        assert slug.slug == "12345"
        assert slug.timestamp is None
    
    def test_creates_slug_with_timestamp(self):
        """Should create DiscoveredSlug with timestamp."""
        slug = DiscoveredSlug(
            engine="cloudbeds",
            slug="hotel-abc",
            source_url="https://example.com",
            archive_source="commoncrawl",
            timestamp="20240115",
        )
        
        assert slug.timestamp == "20240115"


@pytest.mark.no_db
class TestQueryAlienvault:
    """Tests for AlienVault OTX query method."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
        )
        result = await discovery.query_alienvault(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_extracts_slugs_from_response(self):
        """Should extract slugs from AlienVault API response."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="rms",
            wayback_url_pattern="bookings*.rmscloud.com/Search/Index/*",
            slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
            commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
            domains=["rmscloud.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "url_list": [
                {"url": "https://bookings1.rmscloud.com/Search/Index/12345"},
                {"url": "https://bookings2.rmscloud.com/Search/Index/67890"},
                {"url": "https://rmscloud.com/some-other-page"},
            ],
            "has_next": False,
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_alienvault(pattern)

        assert len(slugs) == 2
        assert slugs[0].slug == "12345"
        assert slugs[0].archive_source == "alienvault"
        assert slugs[1].slug == "67890"

    @pytest.mark.asyncio
    async def test_handles_rate_limit(self):
        """Should stop gracefully on 429 rate limit."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 429

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_alienvault(pattern)

        assert slugs == []


@pytest.mark.no_db
class TestQueryUrlscan:
    """Tests for URLScan.io query method."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
        )
        result = await discovery.query_urlscan(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_extracts_slugs_from_response(self):
        """Should extract slugs from URLScan search results."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="cloudbeds",
            wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
            slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
            domains=["cloudbeds.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "page": {"url": "https://hotels.cloudbeds.com/reservation/my-hotel"},
                    "task": {"url": "https://hotels.cloudbeds.com/reservation/my-hotel"},
                    "sort": [1234567890, "abc"],
                },
                {
                    "page": {"url": "https://hotels.cloudbeds.com/reservation/another-hotel"},
                    "task": {"url": "https://cloudbeds.com/other"},
                    "sort": [1234567891, "def"],
                },
            ],
        }

        # Second call returns empty to stop pagination
        mock_response_empty = MagicMock()
        mock_response_empty.status_code = 200
        mock_response_empty.raise_for_status = MagicMock()
        mock_response_empty.json.return_value = {"results": []}

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=[mock_response, mock_response_empty])
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_urlscan(pattern)

        # "my-hotel" from both page.url and task.url, "another-hotel" from page.url
        slug_values = [s.slug for s in slugs]
        assert "my-hotel" in slug_values
        assert "another-hotel" in slug_values
        assert all(s.archive_source == "urlscan" for s in slugs)

    @pytest.mark.asyncio
    async def test_handles_rate_limit_with_retry_after(self):
        """Should honor Retry-After header on 429."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "1"}

        mock_empty = MagicMock()
        mock_empty.status_code = 200
        mock_empty.raise_for_status = MagicMock()
        mock_empty.json.return_value = {"results": []}

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=[mock_429, mock_empty])
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                slugs = await discovery.query_urlscan(pattern)
                # Should have called sleep with the Retry-After value
                mock_sleep.assert_any_call(1)

        assert slugs == []


@pytest.mark.no_db
class TestQueryVirustotal:
    """Tests for VirusTotal query method."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
        )
        result = await discovery.query_virustotal(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_without_api_key(self):
        """Should return empty list when VIRUSTOTAL_API_KEY is not set."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="rms",
            wayback_url_pattern="bookings*.rmscloud.com/Search/Index/*",
            slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
            commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
            domains=["rmscloud.com"],
        )
        with patch.dict("os.environ", {}, clear=True):
            result = await discovery.query_virustotal(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_extracts_slugs_from_response(self):
        """Should extract slugs from VirusTotal API response."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="cloudbeds",
            wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
            slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
            domains=["cloudbeds.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"attributes": {"url": "https://hotels.cloudbeds.com/reservation/hotel-abc"}},
                {"attributes": {"url": "https://hotels.cloudbeds.com/reservation/hotel-xyz"}},
                {"attributes": {"url": "https://cloudbeds.com/blog/something"}},
            ],
            "links": {},
        }

        with patch.dict("os.environ", {"VIRUSTOTAL_API_KEY": "test-key"}):
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(return_value=mock_response)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_client

                slugs = await discovery.query_virustotal(pattern)

        assert len(slugs) == 2
        assert slugs[0].slug == "hotel-abc"
        assert slugs[0].archive_source == "virustotal"
        assert slugs[1].slug == "hotel-xyz"

    @pytest.mark.asyncio
    async def test_handles_rate_limit(self):
        """Should wait on 429 rate limit."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_429 = MagicMock()
        mock_429.status_code = 429

        mock_empty = MagicMock()
        mock_empty.status_code = 200
        mock_empty.raise_for_status = MagicMock()
        mock_empty.json.return_value = {"data": [], "links": {}}

        with patch.dict("os.environ", {"VIRUSTOTAL_API_KEY": "test-key"}):
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(side_effect=[mock_429, mock_empty])
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_client_cls.return_value = mock_client

                with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    slugs = await discovery.query_virustotal(pattern)
                    mock_sleep.assert_any_call(60)

        assert slugs == []


@pytest.mark.no_db
class TestSourceToggles:
    """Tests for source enable/disable flags."""

    def test_default_all_enabled(self):
        """All sources should be enabled by default."""
        discovery = ArchiveSlugDiscovery()
        assert discovery.enable_wayback is True
        assert discovery.enable_commoncrawl is True
        assert discovery.enable_alienvault is True
        assert discovery.enable_urlscan is True
        assert discovery.enable_virustotal is True
        assert discovery.enable_crtsh is True
        assert discovery.enable_arquivo is True
        assert discovery.enable_github is True

    def test_can_disable_sources(self):
        """Should be able to disable individual sources."""
        discovery = ArchiveSlugDiscovery(
            enable_alienvault=False,
            enable_urlscan=False,
            enable_virustotal=False,
            enable_crtsh=False,
            enable_arquivo=False,
            enable_github=False,
        )
        assert discovery.enable_alienvault is False
        assert discovery.enable_urlscan is False
        assert discovery.enable_virustotal is False
        assert discovery.enable_crtsh is False
        assert discovery.enable_arquivo is False
        assert discovery.enable_github is False
        assert discovery.enable_wayback is True
        assert discovery.enable_commoncrawl is True


@pytest.mark.no_db
class TestNewSiteMinderPatterns:
    """Tests for the additional SiteMinder domain patterns."""

    def test_has_bookingbutton_pattern(self):
        """Should have SiteMinder BookingButton pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "siteminder_bookingbutton" in names

    def test_has_directonline_pattern(self):
        """Should have SiteMinder DirectOnline pattern defined."""
        names = [p.name for p in BOOKING_ENGINE_PATTERNS]
        assert "siteminder_directonline" in names

    def test_bookingbutton_extracts_slug(self):
        """Should extract slug from thebookingbutton.com URL."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_bookingbutton")
        discovery = ArchiveSlugDiscovery()
        slug = discovery._extract_slug(
            "https://app.thebookingbutton.com/properties/grand-hotel-resort", pattern.slug_regex
        )
        assert slug == "grand-hotel-resort"

    def test_directonline_extracts_slug(self):
        """Should extract slug from book-directonline.com URL."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_directonline")
        discovery = ArchiveSlugDiscovery()
        slug = discovery._extract_slug(
            "https://book-directonline.com/properties/seaside-inn", pattern.slug_regex
        )
        assert slug == "seaside-inn"

    def test_bookingbutton_has_domains(self):
        """siteminder_bookingbutton should have domains configured."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_bookingbutton")
        assert pattern.domains == ["thebookingbutton.com"]

    def test_directonline_has_domains(self):
        """siteminder_directonline should have domains configured."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder_directonline")
        assert pattern.domains == ["book-directonline.com"]


@pytest.mark.no_db
class TestMewsPattern:
    """Tests for the fixed Mews pattern."""

    def test_mews_uses_specific_subdomain(self):
        """Mews wayback pattern should use app.mews.com not *.mews.com."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "mews")
        assert pattern.wayback_url_pattern == "app.mews.com/distributor/*"

    def test_mews_extracts_uuid_from_app_url(self):
        """Should extract UUID from app.mews.com URL."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "mews")
        discovery = ArchiveSlugDiscovery()
        slug = discovery._extract_slug(
            "https://app.mews.com/distributor/3d7c4572-1c0b-4891-86d2-ab5800e35acc",
            pattern.slug_regex,
        )
        assert slug == "3d7c4572-1c0b-4891-86d2-ab5800e35acc"


@pytest.mark.no_db
class TestQueryArquivo:
    """Tests for Arquivo.pt query method."""

    @pytest.mark.asyncio
    async def test_extracts_slugs_from_response(self):
        """Should extract slugs from Arquivo.pt CDX response."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="cloudbeds",
            wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
            slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
            domains=["cloudbeds.com"],
        )

        # Arquivo.pt returns JSON array of arrays
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = [
            ["com,cloudbeds,hotels)/reservation/hotel-abc", "20230101120000", "https://hotels.cloudbeds.com/reservation/hotel-abc", "text/html", "200", "HASH1", "1234"],
            ["com,cloudbeds,hotels)/reservation/hotel-xyz", "20230601120000", "https://hotels.cloudbeds.com/reservation/hotel-xyz", "text/html", "200", "HASH2", "5678"],
            ["com,cloudbeds)/other-page", "20230101120000", "https://cloudbeds.com/other-page", "text/html", "200", "HASH3", "9012"],
        ]

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_arquivo(pattern)

        assert len(slugs) == 2
        assert slugs[0].slug == "hotel-abc"
        assert slugs[0].archive_source == "arquivo"
        assert slugs[0].timestamp == "20230101120000"
        assert slugs[1].slug == "hotel-xyz"

    @pytest.mark.asyncio
    async def test_handles_empty_response(self):
        """Should handle empty Arquivo.pt response."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = []

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_arquivo(pattern)

        assert slugs == []

    @pytest.mark.asyncio
    async def test_handles_rate_limit(self):
        """Should skip domain on 429 rate limit."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 429

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_arquivo(pattern)

        assert slugs == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
        )
        result = await discovery.query_arquivo(pattern)
        assert result == []


@pytest.mark.no_db
class TestQueryGithub:
    """Tests for GitHub Code Search query method."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
        )
        result = await discovery.query_github(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_extracts_slugs_from_text_matches(self):
        """Should extract slugs from GitHub code search text_matches."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="mews",
            wayback_url_pattern="app.mews.com/distributor/*",
            slug_regex=r"/distributor/([a-f0-9-]{36})",
            commoncrawl_url_pattern="*.mews.com/distributor/*",
            domains=["mews.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "total_count": 1,
            "items": [
                {
                    "name": "booking.html",
                    "path": "src/booking.html",
                    "text_matches": [
                        {
                            "fragment": 'iframe src="https://app.mews.com/distributor/3d7c4572-1c0b-4891-86d2-ab5800e35acc"',
                        },
                        {
                            "fragment": 'url: "https://app.mews.com/distributor/fa8c9482-6916-44c1-b283-af5700f7e302"',
                        },
                    ],
                }
            ],
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_github(pattern)

        assert len(slugs) == 2
        slug_values = [s.slug for s in slugs]
        assert "3d7c4572-1c0b-4891-86d2-ab5800e35acc" in slug_values
        assert "fa8c9482-6916-44c1-b283-af5700f7e302" in slug_values
        assert all(s.archive_source == "github" for s in slugs)

    @pytest.mark.asyncio
    async def test_handles_rate_limit(self):
        """Should stop on 403 rate limit."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=["example.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.headers = {"X-RateLimit-Reset": "1234567890"}

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_github(pattern)

        assert slugs == []

    @pytest.mark.asyncio
    async def test_extracts_siteminder_slugs(self):
        """Should extract SiteMinder slugs from GitHub code snippets."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="siteminder_directbook",
            wayback_url_pattern="direct-book.com/properties/*",
            slug_regex=r"/properties/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="*.direct-book.com/properties/*",
            domains=["direct-book.com"],
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "total_count": 1,
            "items": [
                {
                    "name": "config.js",
                    "path": "src/config.js",
                    "text_matches": [
                        {
                            "fragment": 'bookingUrl: "https://direct-book.com/properties/sunset-beach-hotel"',
                        },
                    ],
                }
            ],
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            slugs = await discovery.query_github(pattern)

        assert len(slugs) == 1
        assert slugs[0].slug == "sunset-beach-hotel"
        assert slugs[0].archive_source == "github"


@pytest.mark.no_db
class TestQueryCrtsh:
    """Tests for crt.sh Certificate Transparency query method."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_crtsh_regex(self):
        """Should return empty list when pattern has no crtsh_subdomain_regex."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="mews",
            wayback_url_pattern="app.mews.com/distributor/*",
            slug_regex=r"/distributor/([a-f0-9-]{36})",
            commoncrawl_url_pattern="*.mews.com/distributor/*",
            domains=["mews.com"],
            # No crtsh_subdomain_regex — should skip
        )
        result = await discovery.query_crtsh(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_no_domains(self):
        """Should return empty list when pattern has no domains."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="*.example.com/*",
            slug_regex=r"/(\d+)",
            commoncrawl_url_pattern="*.example.com/*",
            domains=[],
            crtsh_subdomain_regex=r"([a-z0-9_-]+)\.example\.com",
        )
        result = await discovery.query_crtsh(pattern)
        assert result == []

    @pytest.mark.asyncio
    async def test_extracts_rms_slugs_from_subdomains(self):
        """Should extract RMS booking server numbers from crt.sh cert names."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="rms",
            wayback_url_pattern="bookings*.rmscloud.com/Search/Index/*",
            slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
            commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
            domains=["rmscloud.com"],
            crtsh_subdomain_regex=r"bookings(\d+)\.rmscloud\.com",
        )

        # Mock the Postgres call to fail, then mock the HTTP fallback
        with patch.object(discovery, "_query_crtsh_postgres", return_value=None):
            with patch.object(
                discovery,
                "_query_crtsh_http",
                return_value=[
                    "bookings1234.rmscloud.com",
                    "bookings5678.rmscloud.com",
                    "*.rmscloud.com",  # Should be skipped (wildcard)
                    "rmscloud.com",  # Should be skipped (bare domain)
                    "www.rmscloud.com",  # Should be skipped (infra subdomain)
                    "api.rmscloud.com",  # Should be skipped (infra subdomain)
                ],
            ):
                slugs = await discovery.query_crtsh(pattern)

        assert len(slugs) == 2
        slug_values = [s.slug for s in slugs]
        assert "1234" in slug_values
        assert "5678" in slug_values
        assert all(s.archive_source == "crtsh" for s in slugs)
        assert all(s.engine == "rms" for s in slugs)

    @pytest.mark.asyncio
    async def test_extracts_subdomain_based_slugs(self):
        """Should extract slugs from subdomain-based engines (bookonlinenow, resnexus)."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="bookonlinenow",
            wayback_url_pattern="*.book-onlinenow.net/*",
            slug_regex=r"([a-z0-9_-]+)\.book-onlinenow\.net",
            commoncrawl_url_pattern="*.book-onlinenow.net/*",
            domains=["book-onlinenow.net"],
            crtsh_subdomain_regex=r"([a-z0-9_-]+)\.book-onlinenow\.net",
        )

        with patch.object(discovery, "_query_crtsh_postgres", return_value=None):
            with patch.object(
                discovery,
                "_query_crtsh_http",
                return_value=[
                    "sunset-hotel.book-onlinenow.net",
                    "mountain-lodge.book-onlinenow.net",
                    "www.book-onlinenow.net",  # skipped
                ],
            ):
                slugs = await discovery.query_crtsh(pattern)

        assert len(slugs) == 2
        slug_values = [s.slug for s in slugs]
        assert "sunset-hotel" in slug_values
        assert "mountain-lodge" in slug_values

    @pytest.mark.asyncio
    async def test_postgres_fallback_to_http(self):
        """Should fall back to HTTP when Postgres fails."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="rms_ibe",
            wayback_url_pattern="ibe*.rmscloud.com/*",
            slug_regex=r"ibe\d*\.rmscloud\.com/(\d+)",
            commoncrawl_url_pattern="ibe*.rmscloud.com/*",
            domains=["rmscloud.com"],
            crtsh_subdomain_regex=r"ibe(\d+)\.rmscloud\.com",
        )

        # Postgres returns None (failure), HTTP returns results
        with patch.object(discovery, "_query_crtsh_postgres", return_value=None):
            with patch.object(
                discovery,
                "_query_crtsh_http",
                return_value=["ibe100.rmscloud.com", "ibe200.rmscloud.com"],
            ):
                slugs = await discovery.query_crtsh(pattern)

        assert len(slugs) == 2
        assert slugs[0].slug == "100"
        assert slugs[1].slug == "200"

    @pytest.mark.asyncio
    async def test_postgres_success_skips_http(self):
        """Should not call HTTP when Postgres succeeds."""
        discovery = ArchiveSlugDiscovery()
        pattern = BookingEnginePattern(
            name="rms",
            wayback_url_pattern="bookings*.rmscloud.com/Search/Index/*",
            slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
            commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
            domains=["rmscloud.com"],
            crtsh_subdomain_regex=r"bookings(\d+)\.rmscloud\.com",
        )

        with patch.object(
            discovery,
            "_query_crtsh_postgres",
            return_value=["bookings42.rmscloud.com"],
        ) as mock_pg:
            with patch.object(discovery, "_query_crtsh_http") as mock_http:
                slugs = await discovery.query_crtsh(pattern)

        mock_pg.assert_called_once()
        mock_http.assert_not_called()
        assert len(slugs) == 1
        assert slugs[0].slug == "42"

    @pytest.mark.asyncio
    async def test_http_api_parses_json_response(self):
        """Should parse crt.sh HTTP JSON API response correctly."""
        discovery = ArchiveSlugDiscovery()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = [
            {"name_value": "bookings100.rmscloud.com"},
            {"name_value": "bookings200.rmscloud.com\n*.rmscloud.com"},
            {"name_value": "*.rmscloud.com"},
        ]

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await discovery._query_crtsh_http("rmscloud.com")

        # Should have 2 unique non-wildcard names
        assert "bookings100.rmscloud.com" in result
        assert "bookings200.rmscloud.com" in result
        assert "*.rmscloud.com" not in result

    @pytest.mark.asyncio
    async def test_http_api_handles_rate_limit(self):
        """Should return empty list on 429."""
        discovery = ArchiveSlugDiscovery()

        mock_response = MagicMock()
        mock_response.status_code = 429

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await discovery._query_crtsh_http("rmscloud.com")

        assert result == []


@pytest.mark.no_db
class TestCrtshSubdomainPatterns:
    """Tests for crtsh_subdomain_regex on engine patterns."""

    def test_rms_has_crtsh_pattern(self):
        """RMS should have crtsh_subdomain_regex configured."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "rms")
        assert pattern.crtsh_subdomain_regex is not None

    def test_rms_ibe_has_crtsh_pattern(self):
        """RMS IBE should have crtsh_subdomain_regex configured."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "rms_ibe")
        assert pattern.crtsh_subdomain_regex is not None

    def test_rms_rates_no_crtsh_pattern(self):
        """RMS Rates should NOT have crtsh_subdomain_regex (shares subdomains with rms)."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "rms_rates")
        assert pattern.crtsh_subdomain_regex is None

    def test_bookonlinenow_has_crtsh_pattern(self):
        """BookOnlineNow should have crtsh_subdomain_regex (subdomain=slug)."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "bookonlinenow")
        assert pattern.crtsh_subdomain_regex is not None

    def test_mews_no_crtsh_pattern(self):
        """Mews should NOT have crtsh_subdomain_regex (slug is UUID in path, not subdomain)."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "mews")
        assert pattern.crtsh_subdomain_regex is None

    def test_siteminder_no_crtsh_pattern(self):
        """SiteMinder should NOT have crtsh_subdomain_regex (slug is in path)."""
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "siteminder")
        assert pattern.crtsh_subdomain_regex is None

    def test_crtsh_pattern_extracts_from_subdomain(self):
        """crtsh_subdomain_regex should extract slug from full subdomain name."""
        discovery = ArchiveSlugDiscovery()
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "rms")
        slug = discovery._extract_slug(
            "bookings4567.rmscloud.com", pattern.crtsh_subdomain_regex
        )
        assert slug == "4567"


@pytest.mark.online
class TestArchiveSlugDiscoveryIntegration:
    """Integration tests that hit real archive APIs."""

    @pytest.mark.asyncio
    async def test_queries_wayback_machine(self):
        """Should query Wayback Machine CDX API."""
        discovery = ArchiveSlugDiscovery(timeout=30.0, max_results_per_query=10)

        # Use a pattern likely to have results
        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
            slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
        )

        slugs = await discovery.query_wayback(pattern)

        # May or may not have results, but shouldn't crash
        assert isinstance(slugs, list)

    @pytest.mark.asyncio
    async def test_handles_wayback_timeout(self):
        """Should handle Wayback Machine timeout gracefully."""
        discovery = ArchiveSlugDiscovery(timeout=0.001)  # Very short timeout

        pattern = BookingEnginePattern(
            name="test",
            wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
            slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
            commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
        )

        # Should not raise, just return empty list
        slugs = await discovery.query_wayback(pattern)

        assert isinstance(slugs, list)

    @pytest.mark.asyncio
    async def test_queries_alienvault(self):
        """Should query AlienVault OTX API."""
        discovery = ArchiveSlugDiscovery(timeout=30.0)
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "rms")

        slugs = await discovery.query_alienvault(pattern)
        assert isinstance(slugs, list)

    @pytest.mark.asyncio
    async def test_queries_urlscan(self):
        """Should query URLScan.io API."""
        discovery = ArchiveSlugDiscovery(timeout=30.0)
        pattern = next(p for p in BOOKING_ENGINE_PATTERNS if p.name == "cloudbeds")

        slugs = await discovery.query_urlscan(pattern)
        assert isinstance(slugs, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
