-- CT certificate intelligence cache table
CREATE TABLE IF NOT EXISTS sadie_gtm.domain_cert_cache (
    domain       TEXT PRIMARY KEY,
    org_name     TEXT,
    alt_domains  TEXT[],
    cert_count   INTEGER DEFAULT 0,
    earliest_cert TIMESTAMPTZ,
    latest_cert  TIMESTAMPTZ,
    has_ov_ev    BOOLEAN DEFAULT FALSE,
    raw_data     JSONB,
    queried_at   TIMESTAMPTZ DEFAULT NOW()
);
