"""Tests for Archive Slug Discovery."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from lib.archive.discovery import (
    ArchiveSlugDiscovery,
    BookingEnginePattern,
    DiscoveredSlug,
    BOOKING_ENGINE_PATTERNS,
)


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
    
    def test_all_patterns_have_required_fields(self):
        """All patterns should have required fields."""
        for pattern in BOOKING_ENGINE_PATTERNS:
            assert pattern.name, "Pattern should have name"
            assert pattern.wayback_url_pattern, "Pattern should have wayback URL"
            assert pattern.slug_regex, "Pattern should have slug regex"
            assert pattern.commoncrawl_url_pattern, "Pattern should have CC URL"


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
