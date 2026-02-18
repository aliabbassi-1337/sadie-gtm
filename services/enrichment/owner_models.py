"""Data models for owner discovery pipeline."""

from typing import Optional
from pydantic import BaseModel


class DecisionMaker(BaseModel):
    """A discovered decision maker (owner, GM, director, etc.)."""

    full_name: Optional[str] = None
    title: Optional[str] = None
    email: Optional[str] = None
    email_verified: bool = False
    phone: Optional[str] = None
    sources: list[str]  # rdap, whois_history, dns_soa, website_scrape, review_response, llm_extract, gov_registry
    confidence: float = 0.0  # 0.0-1.0
    raw_source_url: Optional[str] = None


class DomainIntel(BaseModel):
    """DNS and WHOIS intelligence for a domain."""

    domain: str

    # WHOIS/RDAP
    registrant_name: Optional[str] = None
    registrant_org: Optional[str] = None
    registrant_email: Optional[str] = None
    registrar: Optional[str] = None
    registration_date: Optional[str] = None
    is_privacy_protected: bool = True
    whois_source: Optional[str] = None  # rdap, whois_history_wayback

    # DNS
    email_provider: Optional[str] = None  # google_workspace, microsoft_365, godaddy_email, etc.
    mx_records: list[str] = []
    soa_email: Optional[str] = None
    spf_record: Optional[str] = None
    dmarc_record: Optional[str] = None
    is_catch_all: Optional[bool] = None

    # CT Certificate Intelligence
    ct_org_name: Optional[str] = None
    ct_alt_domains: list[str] = []
    ct_cert_count: int = 0


class OwnerEnrichmentResult(BaseModel):
    """Result of running the owner enrichment waterfall for a single hotel."""

    hotel_id: int
    domain: Optional[str] = None
    decision_makers: list[DecisionMaker] = []
    domain_intel: Optional[DomainIntel] = None
    layers_completed: int = 0  # bitmask
    error: Optional[str] = None

    @property
    def found_any(self) -> bool:
        return len(self.decision_makers) > 0


# Layer bitmask constants
LAYER_RDAP = 1
LAYER_WHOIS_HISTORY = 2
LAYER_DNS = 4
LAYER_WEBSITE = 8
LAYER_REVIEWS = 16
LAYER_EMAIL_VERIFY = 32
LAYER_GOV_DATA = 64
LAYER_CT_CERTS = 128
LAYER_ABN_ASIC = 256
