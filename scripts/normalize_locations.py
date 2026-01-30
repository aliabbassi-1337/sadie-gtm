#!/usr/bin/env python3
"""Normalize location data (state/country codes)."""

import asyncio
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


# State normalization mappings
US_STATES = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
}

AU_STATES = {
    'new south wales': 'NSW', 'victoria': 'VIC', 'queensland': 'QLD',
    'south australia': 'SA', 'western australia': 'WA', 'tasmania': 'TAS',
    'northern territory': 'NT', 'australian capital territory': 'ACT',
}

CA_PROVINCES = {
    'british columbia': 'BC', 'alberta': 'AB', 'saskatchewan': 'SK',
    'manitoba': 'MB', 'ontario': 'ON', 'quebec': 'QC', 'new brunswick': 'NB',
    'nova scotia': 'NS', 'prince edward island': 'PE', 'newfoundland and labrador': 'NL',
    'yukon': 'YT', 'northwest territories': 'NT', 'nunavut': 'NU',
}

# Country normalization
COUNTRIES = {
    'usa': 'US', 'united states': 'US', 'united states of america': 'US',
    'u.s.a.': 'US', 'u.s.': 'US',
    'australia': 'AU', 'aus': 'AU',
    'canada': 'CA', 'can': 'CA',
    'uk': 'GB', 'united kingdom': 'GB', 'great britain': 'GB', 
    'england': 'GB', 'scotland': 'GB', 'wales': 'GB',
    'new zealand': 'NZ', 'nzl': 'NZ',
    'mexico': 'MX', 'méxico': 'MX', 'mex': 'MX',
    'thailand': 'TH', 'thai': 'TH',
    'indonesia': 'ID', 'idn': 'ID',
    'philippines': 'PH', 'phl': 'PH',
    'india': 'IN', 'ind': 'IN',
    'japan': 'JP', 'jpn': 'JP',
    'germany': 'DE', 'deutschland': 'DE',
    'france': 'FR', 'fra': 'FR',
    'italy': 'IT', 'italia': 'IT',
    'spain': 'ES', 'españa': 'ES',
    'brazil': 'BR', 'brasil': 'BR',
    'south africa': 'ZA',
    'uae': 'AE', 'united arab emirates': 'AE', 'dubai': 'AE',
    'singapore': 'SG',
    'malaysia': 'MY',
    'vietnam': 'VN', 'viet nam': 'VN',
    'south korea': 'KR', 'korea': 'KR',
    'china': 'CN',
    'hong kong': 'HK',
    'taiwan': 'TW',
    'ireland': 'IE',
    'netherlands': 'NL', 'holland': 'NL',
    'switzerland': 'CH',
    'austria': 'AT',
    'greece': 'GR',
    'portugal': 'PT',
    'costa rica': 'CR',
    'fiji': 'FJ',
    'maldives': 'MV',
    'sri lanka': 'LK',
    'nepal': 'NP',
    'cambodia': 'KH',
}

# State -> Country inference
STATE_COUNTRY = {
    **{code: 'US' for code in US_STATES.values()},
    **{code: 'AU' for code in AU_STATES.values()},
    **{code: 'CA' for code in CA_PROVINCES.values()},
}
# Fix NT overlap (both AU and CA have NT)
STATE_COUNTRY['NT'] = 'AU'  # Default to Australia for NT


def normalize_state(state: str) -> str:
    """Normalize state to 2-3 letter code."""
    if not state:
        return state
    state_lower = state.lower().strip()
    
    # Check all mappings
    for mapping in [US_STATES, AU_STATES, CA_PROVINCES]:
        if state_lower in mapping:
            return mapping[state_lower]
    
    return state


def normalize_country(country: str) -> str:
    """Normalize country to ISO 3166-1 alpha-2."""
    if not country:
        return country
    country_lower = country.lower().strip()
    return COUNTRIES.get(country_lower, country)


async def main():
    conn = await asyncpg.connect(
        host=os.getenv('SADIE_DB_HOST'),
        port=int(os.getenv('SADIE_DB_PORT', 5432)),
        user=os.getenv('SADIE_DB_USER'),
        password=os.getenv('SADIE_DB_PASSWORD'),
        database=os.getenv('SADIE_DB_NAME'),
        ssl='require'
    )
    
    # 1. Normalize states
    logger.info("=== NORMALIZING STATES ===")
    rows = await conn.fetch('''
        SELECT id, state FROM sadie_gtm.hotels
        WHERE state IS NOT NULL AND LENGTH(state) > 3
    ''')
    logger.info(f"Found {len(rows)} hotels with long state names")
    
    state_updated = 0
    for row in rows:
        new_state = normalize_state(row['state'])
        if new_state != row['state']:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET state = $1, updated_at = NOW() WHERE id = $2',
                new_state, row['id']
            )
            state_updated += 1
    logger.info(f"Normalized {state_updated} state codes")
    
    # 2. Normalize countries
    logger.info("=== NORMALIZING COUNTRIES ===")
    rows = await conn.fetch('''
        SELECT id, country FROM sadie_gtm.hotels
        WHERE country IS NOT NULL AND LENGTH(country) > 2
    ''')
    logger.info(f"Found {len(rows)} hotels with long country names")
    
    country_updated = 0
    for row in rows:
        new_country = normalize_country(row['country'])
        if new_country != row['country']:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET country = $1, updated_at = NOW() WHERE id = $2',
                new_country, row['id']
            )
            country_updated += 1
    logger.info(f"Normalized {country_updated} country codes")
    
    # 3. Infer country from state
    logger.info("=== INFERRING COUNTRY FROM STATE ===")
    rows = await conn.fetch('''
        SELECT id, state FROM sadie_gtm.hotels
        WHERE country IS NULL AND state IS NOT NULL AND LENGTH(state) <= 3
    ''')
    logger.info(f"Found {len(rows)} hotels with state but no country")
    
    country_inferred = 0
    for row in rows:
        inferred = STATE_COUNTRY.get(row['state'])
        if inferred:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET country = $1, updated_at = NOW() WHERE id = $2',
                inferred, row['id']
            )
            country_inferred += 1
    logger.info(f"Inferred {country_inferred} countries from state codes")
    
    # 4. Clean city names (remove zip codes)
    logger.info("=== CLEANING CITY NAMES ===")
    rows = await conn.fetch('''
        SELECT id, city FROM sadie_gtm.hotels
        WHERE city ~ '^[0-9]{4,6}\\s+\\w'
    ''')
    logger.info(f"Found {len(rows)} cities with leading zip codes")
    
    city_cleaned = 0
    for row in rows:
        new_city = re.sub(r'^\d{4,6}\s+', '', row['city']).strip()
        if new_city and new_city != row['city']:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET city = $1, updated_at = NOW() WHERE id = $2',
                new_city, row['id']
            )
            city_cleaned += 1
    logger.info(f"Cleaned {city_cleaned} city names")
    
    # 5. Clean city names (remove brackets)
    rows = await conn.fetch('''
        SELECT id, city FROM sadie_gtm.hotels
        WHERE city LIKE '%[%'
    ''')
    logger.info(f"Found {len(rows)} cities with brackets")
    
    for row in rows:
        new_city = row['city'].split('[')[0].strip()
        if new_city and new_city != row['city']:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET city = $1, updated_at = NOW() WHERE id = $2',
                new_city, row['id']
            )
            city_cleaned += 1
    logger.info(f"Total city names cleaned: {city_cleaned}")
    
    await conn.close()
    
    logger.info("=== SUMMARY ===")
    logger.info(f"States normalized: {state_updated}")
    logger.info(f"Countries normalized: {country_updated}")
    logger.info(f"Countries inferred: {country_inferred}")
    logger.info(f"City names cleaned: {city_cleaned}")
    logger.info("Done!")


if __name__ == "__main__":
    asyncio.run(main())
