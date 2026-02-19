-- ABN Lookup + ASIC director cache
-- Stores entity info from abr.business.gov.au and director names from ASIC

CREATE TABLE IF NOT EXISTS sadie_gtm.abn_lookup_cache (
    abn TEXT PRIMARY KEY,
    entity_name TEXT NOT NULL,
    entity_type TEXT,              -- IND, PRV, PUB, TRUST, etc.
    status TEXT,                   -- Active, Cancelled
    state TEXT,
    postcode TEXT,
    business_names TEXT[],
    acn TEXT,
    directors TEXT[],              -- From ASIC follow-up
    raw_data JSONB,
    queried_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_abn_cache_acn
    ON sadie_gtm.abn_lookup_cache(acn) WHERE acn IS NOT NULL;
