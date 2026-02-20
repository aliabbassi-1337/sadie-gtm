-- Partial index for fast lookup of real people (non-entity) DMs missing email.
-- Matches the hardcoded regex in workflows/enrich_contacts.py load_dms_needing_contacts().
-- Without this, the !~* regex forces a full table scan through Supabase pooler (~6min).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dm_people_needing_email
ON sadie_gtm.hotel_decision_makers (hotel_id, id)
WHERE (email IS NULL OR email = '')
  AND full_name !~* '(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)';
