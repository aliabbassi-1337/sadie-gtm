-- Owner/decision-maker enrichment tables
-- Tracks hotel owners, GMs, and key stakeholders with contact info
-- Populated by multi-layer waterfall: RDAP, historical WHOIS, DNS, website scraping, review mining, email verification

-- Decision maker contacts (1:many with hotels - a hotel can have multiple contacts)
CREATE TABLE IF NOT EXISTS sadie_gtm.hotel_decision_makers (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL REFERENCES sadie_gtm.hotels(id) ON DELETE CASCADE,

    -- Person info
    full_name TEXT,
    title TEXT,                     -- "Owner", "General Manager", "Managing Director", etc.
    email TEXT,
    email_verified BOOLEAN DEFAULT FALSE,
    phone TEXT,

    -- Source tracking
    source TEXT NOT NULL,           -- rdap, whois_history, dns_soa, website_scrape, review_response, llm_extract, gov_registry
    confidence REAL DEFAULT 0.0,    -- 0.0-1.0
    raw_source_url TEXT,            -- URL where info was found

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Prevent duplicate person per hotel (same name+title combo)
    UNIQUE(hotel_id, full_name, title)
);

CREATE INDEX IF NOT EXISTS idx_hotel_dm_hotel_id ON sadie_gtm.hotel_decision_makers(hotel_id);
CREATE INDEX IF NOT EXISTS idx_hotel_dm_source ON sadie_gtm.hotel_decision_makers(source);
CREATE INDEX IF NOT EXISTS idx_hotel_dm_confidence ON sadie_gtm.hotel_decision_makers(confidence DESC);

-- Enrichment status tracking (1:1 with hotels)
CREATE TABLE IF NOT EXISTS sadie_gtm.hotel_owner_enrichment (
    hotel_id INTEGER PRIMARY KEY REFERENCES sadie_gtm.hotels(id) ON DELETE CASCADE,
    status INTEGER DEFAULT 0,       -- 0=pending, 1=complete, 2=no_results
    layers_completed INTEGER DEFAULT 0,  -- bitmask: 1=rdap, 2=whois_history, 4=dns, 8=website, 16=reviews, 32=email_verify
    last_attempt TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hotel_owner_enrich_status ON sadie_gtm.hotel_owner_enrichment(status);

-- DNS intelligence cache (avoid re-querying DNS for same domain)
CREATE TABLE IF NOT EXISTS sadie_gtm.domain_dns_cache (
    domain TEXT PRIMARY KEY,
    email_provider TEXT,            -- google_workspace, microsoft_365, godaddy_email, zoho, self_hosted, other
    mx_records TEXT[],
    soa_email TEXT,
    spf_record TEXT,
    dmarc_record TEXT,
    is_catch_all BOOLEAN,
    queried_at TIMESTAMPTZ DEFAULT NOW()
);

-- WHOIS/RDAP cache (avoid re-querying for same domain)
CREATE TABLE IF NOT EXISTS sadie_gtm.domain_whois_cache (
    domain TEXT PRIMARY KEY,
    registrant_name TEXT,
    registrant_org TEXT,
    registrant_email TEXT,
    registrar TEXT,
    registration_date TIMESTAMPTZ,
    expiration_date TIMESTAMPTZ,
    is_privacy_protected BOOLEAN DEFAULT TRUE,
    source TEXT,                    -- rdap, whois_history_wayback
    raw_data JSONB,
    queried_at TIMESTAMPTZ DEFAULT NOW()
);

-- Triggers for updated_at
CREATE TRIGGER hotel_decision_makers_updated_at
    BEFORE UPDATE ON sadie_gtm.hotel_decision_makers
    FOR EACH ROW
    EXECUTE FUNCTION sadie_gtm.update_updated_at();

CREATE TRIGGER hotel_owner_enrichment_updated_at
    BEFORE UPDATE ON sadie_gtm.hotel_owner_enrichment
    FOR EACH ROW
    EXECUTE FUNCTION sadie_gtm.update_updated_at();

-- Comments
COMMENT ON TABLE sadie_gtm.hotel_decision_makers IS 'Hotel owners, GMs, and key stakeholders with contact info';
COMMENT ON TABLE sadie_gtm.hotel_owner_enrichment IS 'Status tracking for owner enrichment waterfall pipeline';
COMMENT ON TABLE sadie_gtm.domain_dns_cache IS 'Cached DNS intelligence (MX, SPF, SOA, DMARC) per domain';
COMMENT ON TABLE sadie_gtm.domain_whois_cache IS 'Cached WHOIS/RDAP registrant data per domain';
COMMENT ON COLUMN sadie_gtm.hotel_owner_enrichment.layers_completed IS 'Bitmask: 1=rdap, 2=whois_history, 4=dns, 8=website, 16=reviews, 32=email_verify';
