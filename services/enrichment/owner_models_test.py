"""Unit tests for owner enrichment models."""

import pytest
from services.enrichment.owner_models import (
    DecisionMaker,
    DomainIntel,
    OwnerEnrichmentResult,
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
    LAYER_GOV_DATA,
)


@pytest.mark.no_db
class TestDecisionMaker:

    def test_create_minimal(self):
        dm = DecisionMaker(sources=["test"])
        assert dm.sources == ["test"]
        assert dm.full_name is None
        assert dm.email is None
        assert dm.email_verified is False
        assert dm.confidence == 0.0

    def test_create_full(self):
        dm = DecisionMaker(
            full_name="John Smith",
            title="General Manager",
            email="john@hotel.com",
            email_verified=True,
            phone="555-1234",
            sources=["website_scrape"],
            confidence=0.85,
            raw_source_url="https://hotel.com/about",
        )
        assert dm.full_name == "John Smith"
        assert dm.title == "General Manager"
        assert dm.email_verified is True
        assert dm.confidence == 0.85


@pytest.mark.no_db
class TestDomainIntel:

    def test_defaults(self):
        intel = DomainIntel(domain="example.com")
        assert intel.domain == "example.com"
        assert intel.is_privacy_protected is True
        assert intel.mx_records == []
        assert intel.email_provider is None

    def test_with_whois(self):
        intel = DomainIntel(
            domain="hotel.com",
            registrant_name="John Doe",
            registrant_org="Hotel LLC",
            is_privacy_protected=False,
            whois_source="live_whois",
        )
        assert intel.registrant_name == "John Doe"
        assert intel.is_privacy_protected is False


@pytest.mark.no_db
class TestOwnerEnrichmentResult:

    def test_found_any_empty(self):
        result = OwnerEnrichmentResult(hotel_id=1)
        assert result.found_any is False

    def test_found_any_with_dm(self):
        dm = DecisionMaker(sources=["test"], full_name="Test")
        result = OwnerEnrichmentResult(hotel_id=1, decision_makers=[dm])
        assert result.found_any is True

    def test_layers_bitmask(self):
        result = OwnerEnrichmentResult(hotel_id=1)
        result.layers_completed |= LAYER_RDAP
        result.layers_completed |= LAYER_DNS
        assert result.layers_completed & LAYER_RDAP
        assert result.layers_completed & LAYER_DNS
        assert not (result.layers_completed & LAYER_WEBSITE)


@pytest.mark.no_db
class TestLayerConstants:

    def test_layer_values_are_powers_of_two(self):
        layers = [LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
                  LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
                  LAYER_GOV_DATA]
        for layer in layers:
            assert layer & (layer - 1) == 0, f"{layer} is not a power of 2"

    def test_layers_are_unique(self):
        layers = [LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
                  LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
                  LAYER_GOV_DATA]
        assert len(set(layers)) == len(layers)

    def test_gov_data_layer_value(self):
        assert LAYER_GOV_DATA == 64
