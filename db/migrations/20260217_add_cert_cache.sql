-- CT certificate intelligence cache
-- Stores organization names + SAN domains extracted from Certificate Transparency logs

CREATE TABLE IF NOT EXISTS sadie_gtm.domain_cert_cache (
    domain TEXT PRIMARY KEY,
    org_name TEXT,                    -- subject.O from OV/EV certs
    alt_domains TEXT[],              -- non-CDN SANs found
    cert_count INTEGER DEFAULT 0,
    earliest_cert TIMESTAMPTZ,
    latest_cert TIMESTAMPTZ,
    has_ov_ev BOOLEAN DEFAULT FALSE, -- whether any OV/EV certs found
    raw_data JSONB,
    queried_at TIMESTAMPTZ DEFAULT NOW()
);
