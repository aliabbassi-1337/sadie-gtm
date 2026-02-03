-- Normalize bad data to NULL so enricher can overwrite
-- This makes queries simpler (just check IS NULL) and data more consistent

-- State: empty strings and placeholder "-"
UPDATE sadie_gtm.hotels
SET state = NULL, updated_at = NOW()
WHERE state = '' OR state = '-';

-- Empty strings in other fields to NULL
UPDATE sadie_gtm.hotels
SET address = NULL, updated_at = NOW()
WHERE address = '';

UPDATE sadie_gtm.hotels
SET phone_google = NULL, updated_at = NOW()
WHERE phone_google = '';

UPDATE sadie_gtm.hotels
SET phone_website = NULL, updated_at = NOW()
WHERE phone_website = '';

UPDATE sadie_gtm.hotels
SET email = NULL, updated_at = NOW()
WHERE email = '';

UPDATE sadie_gtm.hotels
SET city = NULL, updated_at = NOW()
WHERE city = '';

-- NOTE: country = 'NA' is valid (Namibia), don't null it
