-- Owner enrichment pipeline queries
-- Tables: hotel_owner_enrichment, hotel_decision_makers, domain_whois_cache, domain_dns_cache

-- name: get_hotels_pending_owner_enrichment
-- Get hotels needing owner enrichment (not yet completed)
SELECT h.id as hotel_id, h.name, h.website, h.city, h.state, h.country,
       h.email, h.phone_website, h.phone_google
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
WHERE h.website IS NOT NULL
  AND h.website != ''
  AND (hoe.hotel_id IS NULL OR hoe.status NOT IN (1))
ORDER BY h.id
LIMIT :limit;

-- name: get_hotels_pending_owner_enrichment_by_layer
-- Get hotels missing a specific enrichment layer (bitmask check)
SELECT h.id as hotel_id, h.name, h.website, h.city, h.state, h.country,
       h.email, h.phone_website, h.phone_google
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
WHERE h.website IS NOT NULL
  AND h.website != ''
  AND (hoe.hotel_id IS NULL OR hoe.layers_completed & :layer = 0)
ORDER BY h.id
LIMIT :limit;

-- name: insert_decision_maker<!
-- Upsert a decision maker. On conflict, merge best data and append sources.
INSERT INTO sadie_gtm.hotel_decision_makers
    (hotel_id, full_name, title, email, email_verified, phone, sources, confidence, raw_source_url)
VALUES (:hotel_id, :full_name, :title, :email, :email_verified, :phone, :sources, :confidence, :raw_source_url)
ON CONFLICT (hotel_id, full_name, title) DO UPDATE
SET email = COALESCE(NULLIF(EXCLUDED.email, ''), sadie_gtm.hotel_decision_makers.email),
    email_verified = EXCLUDED.email_verified OR sadie_gtm.hotel_decision_makers.email_verified,
    phone = COALESCE(NULLIF(EXCLUDED.phone, ''), sadie_gtm.hotel_decision_makers.phone),
    sources = (SELECT array_agg(DISTINCT s) FROM unnest(array_cat(sadie_gtm.hotel_decision_makers.sources, EXCLUDED.sources)) s),
    confidence = GREATEST(EXCLUDED.confidence, sadie_gtm.hotel_decision_makers.confidence),
    raw_source_url = COALESCE(EXCLUDED.raw_source_url, sadie_gtm.hotel_decision_makers.raw_source_url),
    updated_at = NOW()
RETURNING id;

-- name: update_enrichment_status!
-- Upsert enrichment status. layers_completed OR'd with existing.
INSERT INTO sadie_gtm.hotel_owner_enrichment (hotel_id, status, layers_completed, last_attempt)
VALUES (:hotel_id, :status, :layers_completed, NOW())
ON CONFLICT (hotel_id) DO UPDATE
SET status = :status,
    layers_completed = sadie_gtm.hotel_owner_enrichment.layers_completed | :layers_completed,
    last_attempt = NOW();

-- name: cache_domain_intel!
-- Upsert WHOIS/RDAP cache. COALESCE preserves existing non-null values.
INSERT INTO sadie_gtm.domain_whois_cache
    (domain, registrant_name, registrant_org, registrant_email,
     registrar, registration_date, is_privacy_protected, source, queried_at)
VALUES (:domain, :registrant_name, :registrant_org, :registrant_email,
        :registrar, :registration_date, :is_privacy_protected, :source, NOW())
ON CONFLICT (domain) DO UPDATE
SET registrant_name = COALESCE(EXCLUDED.registrant_name, sadie_gtm.domain_whois_cache.registrant_name),
    registrant_org = COALESCE(EXCLUDED.registrant_org, sadie_gtm.domain_whois_cache.registrant_org),
    registrant_email = COALESCE(EXCLUDED.registrant_email, sadie_gtm.domain_whois_cache.registrant_email),
    is_privacy_protected = EXCLUDED.is_privacy_protected,
    queried_at = NOW();

-- name: cache_dns_intel!
-- Upsert DNS intelligence cache.
INSERT INTO sadie_gtm.domain_dns_cache
    (domain, email_provider, mx_records, soa_email,
     spf_record, dmarc_record, is_catch_all, queried_at)
VALUES (:domain, :email_provider, :mx_records, :soa_email,
        :spf_record, :dmarc_record, :is_catch_all, NOW())
ON CONFLICT (domain) DO UPDATE
SET email_provider = EXCLUDED.email_provider,
    mx_records = EXCLUDED.mx_records,
    soa_email = EXCLUDED.soa_email,
    spf_record = EXCLUDED.spf_record,
    dmarc_record = EXCLUDED.dmarc_record,
    is_catch_all = EXCLUDED.is_catch_all,
    queried_at = NOW();

-- name: get_enrichment_stats^
-- Get owner enrichment pipeline statistics.
SELECT
    COUNT(*) FILTER (WHERE h.website IS NOT NULL AND h.website != '') as total_with_website,
    COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 0) as pending,
    COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 1) as complete,
    COUNT(hoe.hotel_id) FILTER (WHERE hoe.status = 2) as no_results,
    COUNT(DISTINCT hdm.hotel_id) as hotels_with_contacts,
    COUNT(hdm.id) as total_contacts,
    COUNT(hdm.id) FILTER (WHERE hdm.email_verified) as verified_emails
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
LEFT JOIN sadie_gtm.hotel_decision_makers hdm ON h.id = hdm.hotel_id;

-- name: get_decision_makers_for_hotel
-- Get all decision makers for a hotel, ordered by confidence.
SELECT id, full_name, title, email, email_verified, phone,
       sources, confidence, raw_source_url, created_at
FROM sadie_gtm.hotel_decision_makers
WHERE hotel_id = :hotel_id
ORDER BY confidence DESC;

-- name: cache_cert_intel!
-- Upsert CT certificate intelligence cache.
INSERT INTO sadie_gtm.domain_cert_cache
    (domain, org_name, alt_domains, cert_count,
     earliest_cert, latest_cert, has_ov_ev, raw_data, queried_at)
VALUES (:domain, :org_name, :alt_domains, :cert_count,
        :earliest_cert, :latest_cert, :has_ov_ev, :raw_data, NOW())
ON CONFLICT (domain) DO UPDATE
SET org_name = COALESCE(EXCLUDED.org_name, sadie_gtm.domain_cert_cache.org_name),
    alt_domains = EXCLUDED.alt_domains,
    cert_count = EXCLUDED.cert_count,
    earliest_cert = EXCLUDED.earliest_cert,
    latest_cert = EXCLUDED.latest_cert,
    has_ov_ev = EXCLUDED.has_ov_ev,
    raw_data = EXCLUDED.raw_data,
    queried_at = NOW();

-- name: get_cached_cert_intel^
-- Get cached CT certificate intelligence for a domain.
SELECT domain, org_name, alt_domains, cert_count,
       earliest_cert, latest_cert, has_ov_ev, raw_data, queried_at
FROM sadie_gtm.domain_cert_cache
WHERE domain = :domain;

-- name: find_gov_matches
-- Find government-sourced hotel records matching a hotel by city+state and name.
-- Uses case-insensitive substring matching on name.
SELECT id, name, email, phone_google, phone_website, address, source, external_id, external_id_type
FROM sadie_gtm.hotels
WHERE source IN (
    'dbpr_license', 'dbpr_motel', 'texas_hot', 'sf_assessor',
    'la_county', 'md_sdat_cama', 'nyc_dof', 'hawaii_vpi',
    'chicago_license', 'nsw_liquor'
)
  AND LOWER(city) = LOWER(:city)
  AND LOWER(state) = LOWER(:state)
  AND id != :hotel_id
  AND (
      LOWER(name) = LOWER(:name)
      OR LOWER(name) LIKE '%' || LOWER(:name) || '%'
      OR LOWER(:name) LIKE '%' || LOWER(name) || '%'
  )
LIMIT 5;

-- name: cache_abn_entity!
-- Upsert ABN Lookup + ASIC director cache.
INSERT INTO sadie_gtm.abn_lookup_cache
    (abn, entity_name, entity_type, status, state, postcode,
     business_names, acn, directors, raw_data, queried_at)
VALUES (:abn, :entity_name, :entity_type, :status, :state, :postcode,
        :business_names, :acn, :directors, :raw_data, NOW())
ON CONFLICT (abn) DO UPDATE
SET entity_name = EXCLUDED.entity_name,
    entity_type = EXCLUDED.entity_type,
    status = EXCLUDED.status,
    state = EXCLUDED.state,
    postcode = EXCLUDED.postcode,
    business_names = EXCLUDED.business_names,
    acn = COALESCE(EXCLUDED.acn, sadie_gtm.abn_lookup_cache.acn),
    directors = COALESCE(EXCLUDED.directors, sadie_gtm.abn_lookup_cache.directors),
    raw_data = EXCLUDED.raw_data,
    queried_at = NOW();

-- name: get_cached_abn_entity^
-- Get cached ABN entity by ABN number.
SELECT abn, entity_name, entity_type, status, state, postcode,
       business_names, acn, directors, raw_data, queried_at
FROM sadie_gtm.abn_lookup_cache
WHERE abn = :abn;

-- name: find_cached_abn_by_name
-- Find cached ABN entities matching a business name (case-insensitive).
SELECT abn, entity_name, entity_type, status, state, postcode,
       business_names, acn, directors, queried_at
FROM sadie_gtm.abn_lookup_cache
WHERE (LOWER(entity_name) LIKE '%' || LOWER(:name) || '%'
       OR EXISTS (SELECT 1 FROM unnest(business_names) bn WHERE LOWER(bn) LIKE '%' || LOWER(:name) || '%'))
  AND queried_at > NOW() - INTERVAL '30 days'
ORDER BY queried_at DESC
LIMIT 5;
