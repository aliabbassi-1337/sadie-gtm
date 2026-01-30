-- Migration: Normalize location data
-- HOS-1401: Data Quality - Fix location parsing & normalization

-- 1. Normalize US states to 2-letter codes
UPDATE sadie_gtm.hotels SET state = 'AL', updated_at = NOW() WHERE LOWER(state) = 'alabama';
UPDATE sadie_gtm.hotels SET state = 'AK', updated_at = NOW() WHERE LOWER(state) = 'alaska';
UPDATE sadie_gtm.hotels SET state = 'AZ', updated_at = NOW() WHERE LOWER(state) = 'arizona';
UPDATE sadie_gtm.hotels SET state = 'AR', updated_at = NOW() WHERE LOWER(state) = 'arkansas';
UPDATE sadie_gtm.hotels SET state = 'CA', updated_at = NOW() WHERE LOWER(state) = 'california';
UPDATE sadie_gtm.hotels SET state = 'CO', updated_at = NOW() WHERE LOWER(state) = 'colorado';
UPDATE sadie_gtm.hotels SET state = 'CT', updated_at = NOW() WHERE LOWER(state) = 'connecticut';
UPDATE sadie_gtm.hotels SET state = 'DE', updated_at = NOW() WHERE LOWER(state) = 'delaware';
UPDATE sadie_gtm.hotels SET state = 'FL', updated_at = NOW() WHERE LOWER(state) = 'florida';
UPDATE sadie_gtm.hotels SET state = 'GA', updated_at = NOW() WHERE LOWER(state) = 'georgia';
UPDATE sadie_gtm.hotels SET state = 'HI', updated_at = NOW() WHERE LOWER(state) = 'hawaii';
UPDATE sadie_gtm.hotels SET state = 'ID', updated_at = NOW() WHERE LOWER(state) = 'idaho';
UPDATE sadie_gtm.hotels SET state = 'IL', updated_at = NOW() WHERE LOWER(state) = 'illinois';
UPDATE sadie_gtm.hotels SET state = 'IN', updated_at = NOW() WHERE LOWER(state) = 'indiana';
UPDATE sadie_gtm.hotels SET state = 'IA', updated_at = NOW() WHERE LOWER(state) = 'iowa';
UPDATE sadie_gtm.hotels SET state = 'KS', updated_at = NOW() WHERE LOWER(state) = 'kansas';
UPDATE sadie_gtm.hotels SET state = 'KY', updated_at = NOW() WHERE LOWER(state) = 'kentucky';
UPDATE sadie_gtm.hotels SET state = 'LA', updated_at = NOW() WHERE LOWER(state) = 'louisiana';
UPDATE sadie_gtm.hotels SET state = 'ME', updated_at = NOW() WHERE LOWER(state) = 'maine';
UPDATE sadie_gtm.hotels SET state = 'MD', updated_at = NOW() WHERE LOWER(state) = 'maryland';
UPDATE sadie_gtm.hotels SET state = 'MA', updated_at = NOW() WHERE LOWER(state) = 'massachusetts';
UPDATE sadie_gtm.hotels SET state = 'MI', updated_at = NOW() WHERE LOWER(state) = 'michigan';
UPDATE sadie_gtm.hotels SET state = 'MN', updated_at = NOW() WHERE LOWER(state) = 'minnesota';
UPDATE sadie_gtm.hotels SET state = 'MS', updated_at = NOW() WHERE LOWER(state) = 'mississippi';
UPDATE sadie_gtm.hotels SET state = 'MO', updated_at = NOW() WHERE LOWER(state) = 'missouri';
UPDATE sadie_gtm.hotels SET state = 'MT', updated_at = NOW() WHERE LOWER(state) = 'montana';
UPDATE sadie_gtm.hotels SET state = 'NE', updated_at = NOW() WHERE LOWER(state) = 'nebraska';
UPDATE sadie_gtm.hotels SET state = 'NV', updated_at = NOW() WHERE LOWER(state) = 'nevada';
UPDATE sadie_gtm.hotels SET state = 'NH', updated_at = NOW() WHERE LOWER(state) = 'new hampshire';
UPDATE sadie_gtm.hotels SET state = 'NJ', updated_at = NOW() WHERE LOWER(state) = 'new jersey';
UPDATE sadie_gtm.hotels SET state = 'NM', updated_at = NOW() WHERE LOWER(state) = 'new mexico';
UPDATE sadie_gtm.hotels SET state = 'NY', updated_at = NOW() WHERE LOWER(state) = 'new york';
UPDATE sadie_gtm.hotels SET state = 'NC', updated_at = NOW() WHERE LOWER(state) = 'north carolina';
UPDATE sadie_gtm.hotels SET state = 'ND', updated_at = NOW() WHERE LOWER(state) = 'north dakota';
UPDATE sadie_gtm.hotels SET state = 'OH', updated_at = NOW() WHERE LOWER(state) = 'ohio';
UPDATE sadie_gtm.hotels SET state = 'OK', updated_at = NOW() WHERE LOWER(state) = 'oklahoma';
UPDATE sadie_gtm.hotels SET state = 'OR', updated_at = NOW() WHERE LOWER(state) = 'oregon';
UPDATE sadie_gtm.hotels SET state = 'PA', updated_at = NOW() WHERE LOWER(state) = 'pennsylvania';
UPDATE sadie_gtm.hotels SET state = 'RI', updated_at = NOW() WHERE LOWER(state) = 'rhode island';
UPDATE sadie_gtm.hotels SET state = 'SC', updated_at = NOW() WHERE LOWER(state) = 'south carolina';
UPDATE sadie_gtm.hotels SET state = 'SD', updated_at = NOW() WHERE LOWER(state) = 'south dakota';
UPDATE sadie_gtm.hotels SET state = 'TN', updated_at = NOW() WHERE LOWER(state) = 'tennessee';
UPDATE sadie_gtm.hotels SET state = 'TX', updated_at = NOW() WHERE LOWER(state) = 'texas';
UPDATE sadie_gtm.hotels SET state = 'UT', updated_at = NOW() WHERE LOWER(state) = 'utah';
UPDATE sadie_gtm.hotels SET state = 'VT', updated_at = NOW() WHERE LOWER(state) = 'vermont';
UPDATE sadie_gtm.hotels SET state = 'VA', updated_at = NOW() WHERE LOWER(state) = 'virginia';
UPDATE sadie_gtm.hotels SET state = 'WA', updated_at = NOW() WHERE LOWER(state) = 'washington';
UPDATE sadie_gtm.hotels SET state = 'WV', updated_at = NOW() WHERE LOWER(state) = 'west virginia';
UPDATE sadie_gtm.hotels SET state = 'WI', updated_at = NOW() WHERE LOWER(state) = 'wisconsin';
UPDATE sadie_gtm.hotels SET state = 'WY', updated_at = NOW() WHERE LOWER(state) = 'wyoming';
UPDATE sadie_gtm.hotels SET state = 'DC', updated_at = NOW() WHERE LOWER(state) = 'district of columbia';

-- 2. Normalize Australian states
UPDATE sadie_gtm.hotels SET state = 'NSW', updated_at = NOW() WHERE LOWER(state) = 'new south wales';
UPDATE sadie_gtm.hotels SET state = 'VIC', updated_at = NOW() WHERE LOWER(state) = 'victoria' AND country IN ('AU', 'Australia');
UPDATE sadie_gtm.hotels SET state = 'QLD', updated_at = NOW() WHERE LOWER(state) = 'queensland';
UPDATE sadie_gtm.hotels SET state = 'WA', updated_at = NOW() WHERE LOWER(state) = 'western australia';
UPDATE sadie_gtm.hotels SET state = 'SA', updated_at = NOW() WHERE LOWER(state) = 'south australia';
UPDATE sadie_gtm.hotels SET state = 'TAS', updated_at = NOW() WHERE LOWER(state) = 'tasmania';
UPDATE sadie_gtm.hotels SET state = 'NT', updated_at = NOW() WHERE LOWER(state) = 'northern territory';
UPDATE sadie_gtm.hotels SET state = 'ACT', updated_at = NOW() WHERE LOWER(state) = 'australian capital territory';

-- 3. Normalize Canadian provinces
UPDATE sadie_gtm.hotels SET state = 'BC', updated_at = NOW() WHERE LOWER(state) = 'british columbia';
UPDATE sadie_gtm.hotels SET state = 'AB', updated_at = NOW() WHERE LOWER(state) = 'alberta';
UPDATE sadie_gtm.hotels SET state = 'SK', updated_at = NOW() WHERE LOWER(state) = 'saskatchewan';
UPDATE sadie_gtm.hotels SET state = 'MB', updated_at = NOW() WHERE LOWER(state) = 'manitoba';
UPDATE sadie_gtm.hotels SET state = 'ON', updated_at = NOW() WHERE LOWER(state) = 'ontario';
UPDATE sadie_gtm.hotels SET state = 'QC', updated_at = NOW() WHERE LOWER(state) = 'quebec';
UPDATE sadie_gtm.hotels SET state = 'NB', updated_at = NOW() WHERE LOWER(state) = 'new brunswick';
UPDATE sadie_gtm.hotels SET state = 'NS', updated_at = NOW() WHERE LOWER(state) = 'nova scotia';
UPDATE sadie_gtm.hotels SET state = 'PE', updated_at = NOW() WHERE LOWER(state) = 'prince edward island';
UPDATE sadie_gtm.hotels SET state = 'NL', updated_at = NOW() WHERE LOWER(state) = 'newfoundland and labrador';
UPDATE sadie_gtm.hotels SET state = 'YT', updated_at = NOW() WHERE LOWER(state) = 'yukon';
UPDATE sadie_gtm.hotels SET state = 'NT', updated_at = NOW() WHERE LOWER(state) = 'northwest territories' AND country IN ('CA', 'Canada');
UPDATE sadie_gtm.hotels SET state = 'NU', updated_at = NOW() WHERE LOWER(state) = 'nunavut';

-- 4. Normalize countries to ISO 3166-1 alpha-2
UPDATE sadie_gtm.hotels SET country = 'US', updated_at = NOW() WHERE LOWER(country) IN ('usa', 'united states', 'united states of america', 'u.s.a.', 'u.s.');
UPDATE sadie_gtm.hotels SET country = 'AU', updated_at = NOW() WHERE LOWER(country) IN ('australia', 'aus');
UPDATE sadie_gtm.hotels SET country = 'CA', updated_at = NOW() WHERE LOWER(country) IN ('canada', 'can');
UPDATE sadie_gtm.hotels SET country = 'GB', updated_at = NOW() WHERE LOWER(country) IN ('uk', 'united kingdom', 'great britain', 'england', 'scotland', 'wales');
UPDATE sadie_gtm.hotels SET country = 'NZ', updated_at = NOW() WHERE LOWER(country) IN ('new zealand', 'nzl');
UPDATE sadie_gtm.hotels SET country = 'MX', updated_at = NOW() WHERE LOWER(country) IN ('mexico', 'méxico', 'mex');
UPDATE sadie_gtm.hotels SET country = 'TH', updated_at = NOW() WHERE LOWER(country) IN ('thailand', 'thai');
UPDATE sadie_gtm.hotels SET country = 'ID', updated_at = NOW() WHERE LOWER(country) IN ('indonesia', 'idn');
UPDATE sadie_gtm.hotels SET country = 'PH', updated_at = NOW() WHERE LOWER(country) IN ('philippines', 'phl');
UPDATE sadie_gtm.hotels SET country = 'IN', updated_at = NOW() WHERE LOWER(country) IN ('india', 'ind');
UPDATE sadie_gtm.hotels SET country = 'JP', updated_at = NOW() WHERE LOWER(country) IN ('japan', 'jpn');
UPDATE sadie_gtm.hotels SET country = 'DE', updated_at = NOW() WHERE LOWER(country) IN ('germany', 'deutschland', 'deu');
UPDATE sadie_gtm.hotels SET country = 'FR', updated_at = NOW() WHERE LOWER(country) IN ('france', 'fra');
UPDATE sadie_gtm.hotels SET country = 'IT', updated_at = NOW() WHERE LOWER(country) IN ('italy', 'italia', 'ita');
UPDATE sadie_gtm.hotels SET country = 'ES', updated_at = NOW() WHERE LOWER(country) IN ('spain', 'españa', 'esp');
UPDATE sadie_gtm.hotels SET country = 'PT', updated_at = NOW() WHERE LOWER(country) IN ('portugal', 'prt');
UPDATE sadie_gtm.hotels SET country = 'BR', updated_at = NOW() WHERE LOWER(country) IN ('brazil', 'brasil', 'bra');
UPDATE sadie_gtm.hotels SET country = 'AR', updated_at = NOW() WHERE LOWER(country) IN ('argentina', 'arg');
UPDATE sadie_gtm.hotels SET country = 'CL', updated_at = NOW() WHERE LOWER(country) IN ('chile', 'chl');
UPDATE sadie_gtm.hotels SET country = 'CO', updated_at = NOW() WHERE LOWER(country) IN ('colombia', 'col');
UPDATE sadie_gtm.hotels SET country = 'ZA', updated_at = NOW() WHERE LOWER(country) IN ('south africa', 'zaf');
UPDATE sadie_gtm.hotels SET country = 'AE', updated_at = NOW() WHERE LOWER(country) IN ('uae', 'united arab emirates', 'dubai');
UPDATE sadie_gtm.hotels SET country = 'SG', updated_at = NOW() WHERE LOWER(country) IN ('singapore', 'sgp');
UPDATE sadie_gtm.hotels SET country = 'MY', updated_at = NOW() WHERE LOWER(country) IN ('malaysia', 'mys');
UPDATE sadie_gtm.hotels SET country = 'VN', updated_at = NOW() WHERE LOWER(country) IN ('vietnam', 'viet nam', 'vnm');
UPDATE sadie_gtm.hotels SET country = 'KR', updated_at = NOW() WHERE LOWER(country) IN ('south korea', 'korea', 'kor');
UPDATE sadie_gtm.hotels SET country = 'CN', updated_at = NOW() WHERE LOWER(country) IN ('china', 'chn');
UPDATE sadie_gtm.hotels SET country = 'HK', updated_at = NOW() WHERE LOWER(country) IN ('hong kong', 'hkg');
UPDATE sadie_gtm.hotels SET country = 'TW', updated_at = NOW() WHERE LOWER(country) IN ('taiwan', 'twn');
UPDATE sadie_gtm.hotels SET country = 'IE', updated_at = NOW() WHERE LOWER(country) IN ('ireland', 'irl');
UPDATE sadie_gtm.hotels SET country = 'NL', updated_at = NOW() WHERE LOWER(country) IN ('netherlands', 'holland', 'nld');
UPDATE sadie_gtm.hotels SET country = 'BE', updated_at = NOW() WHERE LOWER(country) IN ('belgium', 'bel');
UPDATE sadie_gtm.hotels SET country = 'CH', updated_at = NOW() WHERE LOWER(country) IN ('switzerland', 'che');
UPDATE sadie_gtm.hotels SET country = 'AT', updated_at = NOW() WHERE LOWER(country) IN ('austria', 'aut');
UPDATE sadie_gtm.hotels SET country = 'GR', updated_at = NOW() WHERE LOWER(country) IN ('greece', 'grc');
UPDATE sadie_gtm.hotels SET country = 'TR', updated_at = NOW() WHERE LOWER(country) IN ('turkey', 'türkiye', 'tur');
UPDATE sadie_gtm.hotels SET country = 'PL', updated_at = NOW() WHERE LOWER(country) IN ('poland', 'pol');
UPDATE sadie_gtm.hotels SET country = 'CZ', updated_at = NOW() WHERE LOWER(country) IN ('czech republic', 'czechia', 'cze');
UPDATE sadie_gtm.hotels SET country = 'HU', updated_at = NOW() WHERE LOWER(country) IN ('hungary', 'hun');
UPDATE sadie_gtm.hotels SET country = 'HR', updated_at = NOW() WHERE LOWER(country) IN ('croatia', 'hrv');
UPDATE sadie_gtm.hotels SET country = 'RO', updated_at = NOW() WHERE LOWER(country) IN ('romania', 'rou');
UPDATE sadie_gtm.hotels SET country = 'BG', updated_at = NOW() WHERE LOWER(country) IN ('bulgaria', 'bgr');
UPDATE sadie_gtm.hotels SET country = 'SE', updated_at = NOW() WHERE LOWER(country) IN ('sweden', 'swe');
UPDATE sadie_gtm.hotels SET country = 'NO', updated_at = NOW() WHERE LOWER(country) IN ('norway', 'nor');
UPDATE sadie_gtm.hotels SET country = 'DK', updated_at = NOW() WHERE LOWER(country) IN ('denmark', 'dnk');
UPDATE sadie_gtm.hotels SET country = 'FI', updated_at = NOW() WHERE LOWER(country) IN ('finland', 'fin');
UPDATE sadie_gtm.hotels SET country = 'IS', updated_at = NOW() WHERE LOWER(country) IN ('iceland', 'isl');
UPDATE sadie_gtm.hotels SET country = 'CR', updated_at = NOW() WHERE LOWER(country) IN ('costa rica', 'cri');
UPDATE sadie_gtm.hotels SET country = 'PA', updated_at = NOW() WHERE LOWER(country) IN ('panama', 'pan');
UPDATE sadie_gtm.hotels SET country = 'PE', updated_at = NOW() WHERE LOWER(country) IN ('peru', 'per');
UPDATE sadie_gtm.hotels SET country = 'EC', updated_at = NOW() WHERE LOWER(country) IN ('ecuador', 'ecu');
UPDATE sadie_gtm.hotels SET country = 'DO', updated_at = NOW() WHERE LOWER(country) IN ('dominican republic', 'dom');
UPDATE sadie_gtm.hotels SET country = 'JM', updated_at = NOW() WHERE LOWER(country) IN ('jamaica', 'jam');
UPDATE sadie_gtm.hotels SET country = 'BS', updated_at = NOW() WHERE LOWER(country) IN ('bahamas', 'bhs');
UPDATE sadie_gtm.hotels SET country = 'BB', updated_at = NOW() WHERE LOWER(country) IN ('barbados', 'brb');
UPDATE sadie_gtm.hotels SET country = 'KE', updated_at = NOW() WHERE LOWER(country) IN ('kenya', 'ken');
UPDATE sadie_gtm.hotels SET country = 'TZ', updated_at = NOW() WHERE LOWER(country) IN ('tanzania', 'tza');
UPDATE sadie_gtm.hotels SET country = 'MA', updated_at = NOW() WHERE LOWER(country) IN ('morocco', 'mar');
UPDATE sadie_gtm.hotels SET country = 'EG', updated_at = NOW() WHERE LOWER(country) IN ('egypt', 'egy');
UPDATE sadie_gtm.hotels SET country = 'IL', updated_at = NOW() WHERE LOWER(country) IN ('israel', 'isr');
UPDATE sadie_gtm.hotels SET country = 'RU', updated_at = NOW() WHERE LOWER(country) IN ('russia', 'russian federation', 'rus');
UPDATE sadie_gtm.hotels SET country = 'UA', updated_at = NOW() WHERE LOWER(country) IN ('ukraine', 'ukr');
UPDATE sadie_gtm.hotels SET country = 'FJ', updated_at = NOW() WHERE LOWER(country) IN ('fiji', 'fji');
UPDATE sadie_gtm.hotels SET country = 'MV', updated_at = NOW() WHERE LOWER(country) IN ('maldives', 'mdv');
UPDATE sadie_gtm.hotels SET country = 'LK', updated_at = NOW() WHERE LOWER(country) IN ('sri lanka', 'lka');
UPDATE sadie_gtm.hotels SET country = 'NP', updated_at = NOW() WHERE LOWER(country) IN ('nepal', 'npl');
UPDATE sadie_gtm.hotels SET country = 'KH', updated_at = NOW() WHERE LOWER(country) IN ('cambodia', 'khm');
UPDATE sadie_gtm.hotels SET country = 'MM', updated_at = NOW() WHERE LOWER(country) IN ('myanmar', 'burma', 'mmr');
UPDATE sadie_gtm.hotels SET country = 'LA', updated_at = NOW() WHERE LOWER(country) IN ('laos', 'lao');

-- 5. Remove zip codes from city names (common pattern: "7770 Vestervig" -> "Vestervig")
UPDATE sadie_gtm.hotels 
SET city = TRIM(REGEXP_REPLACE(city, '^\d{4,6}\s+', '')), updated_at = NOW()
WHERE city ~ '^\d{4,6}\s+\w';

-- 6. Clean up city names with brackets (e.g., "Bangkok [Krung Thep]" -> "Bangkok")
UPDATE sadie_gtm.hotels 
SET city = TRIM(SPLIT_PART(city, '[', 1)), updated_at = NOW()
WHERE city LIKE '%[%';

-- 7. Set country from state for obvious cases where country is NULL
-- US states
UPDATE sadie_gtm.hotels SET country = 'US', updated_at = NOW()
WHERE country IS NULL AND state IN (
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
);

-- Australian states
UPDATE sadie_gtm.hotels SET country = 'AU', updated_at = NOW()
WHERE country IS NULL AND state IN ('NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT');

-- Canadian provinces
UPDATE sadie_gtm.hotels SET country = 'CA', updated_at = NOW()
WHERE country IS NULL AND state IN ('BC', 'AB', 'SK', 'MB', 'ON', 'QC', 'NB', 'NS', 'PE', 'NL', 'YT', 'NU');
