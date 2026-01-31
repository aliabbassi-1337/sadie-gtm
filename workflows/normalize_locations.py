#!/usr/bin/env python3
"""
Location Normalization Workflow - Normalize country, state, and city data.

Converts short codes to full names:
- Countries: USA -> United States, AU -> Australia, etc.
- States: FL -> Florida, CA -> California, NSW -> New South Wales, etc.
- Fixes malformed data: "WY 83012" -> "Wyoming", state="VIC" with country="USA" -> country="Australia"

Usage:
    # Check what needs normalization
    uv run python -m workflows.normalize_locations --status

    # Dry run - show what would change
    uv run python -m workflows.normalize_locations --dry-run

    # Run normalization
    uv run python -m workflows.normalize_locations

    # Run specific normalization only
    uv run python -m workflows.normalize_locations --countries-only
    uv run python -m workflows.normalize_locations --states-only
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
import asyncpg

from db.client import init_db, close_db

# Country code to full name mapping
COUNTRY_NAMES = {
    "USA": "United States",
    "US": "United States",
    "AU": "Australia",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "CA": "Canada",  # Be careful - CA is also California
    "NZ": "New Zealand",
    "DE": "Germany",
    "FR": "France",
    "ES": "Spain",
    "IT": "Italy",
    "MX": "Mexico",
    "JP": "Japan",
    "CN": "China",
    "IN": "India",
    "BR": "Brazil",
    "AR": "Argentina",
    "CL": "Chile",
    "CO": "Colombia",
    "PE": "Peru",
    "ZA": "South Africa",
    "EG": "Egypt",
    "MA": "Morocco",
    "KE": "Kenya",
    "TH": "Thailand",
    "VN": "Vietnam",
    "ID": "Indonesia",
    "MY": "Malaysia",
    "SG": "Singapore",
    "PH": "Philippines",
    "KR": "South Korea",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    "AE": "United Arab Emirates",
    "SA": "Saudi Arabia",
    "IL": "Israel",
    "TR": "Turkey",
    "GR": "Greece",
    "PT": "Portugal",
    "NL": "Netherlands",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "PL": "Poland",
    "CZ": "Czech Republic",
    "HU": "Hungary",
    "RO": "Romania",
    "IE": "Ireland",
    "PR": "Puerto Rico",
}

# US state codes to full names
US_STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
}

# Australian state codes to full names
AU_STATE_NAMES = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "WA": "Western Australia",
    "SA": "South Australia",
    "TAS": "Tasmania",
    "ACT": "Australian Capital Territory",
    "NT": "Northern Territory",
}

# Canadian province codes to full names
CA_PROVINCE_NAMES = {
    "ON": "Ontario", "QC": "Quebec", "BC": "British Columbia", "AB": "Alberta",
    "MB": "Manitoba", "SK": "Saskatchewan", "NS": "Nova Scotia", "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador", "PE": "Prince Edward Island",
    "NT": "Northwest Territories", "YT": "Yukon", "NU": "Nunavut",
}

# UK country codes
UK_COUNTRY_NAMES = {
    "ENG": "England",
    "SCT": "Scotland", 
    "WLS": "Wales",
    "NIR": "Northern Ireland",
}

DB_URL = "postgresql://postgres.yunairadgmaqesxejqap:SadieGTM321-@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"


async def get_status(conn: asyncpg.Connection) -> dict:
    """Get counts of data needing normalization."""
    stats = {}
    
    # Countries that need normalization
    rows = await conn.fetch("""
        SELECT country, COUNT(*) as cnt 
        FROM sadie_gtm.hotels 
        WHERE country IN ('USA', 'US', 'AU', 'UK', 'GB', 'NZ', 'DE', 'FR', 'ES', 'IT', 'MX', 'JP', 'CN', 'BR')
        GROUP BY country ORDER BY cnt DESC
    """)
    stats['countries_to_normalize'] = sum(r['cnt'] for r in rows)
    stats['country_breakdown'] = {r['country']: r['cnt'] for r in rows}
    
    # Australian states incorrectly in USA
    aus_count = await conn.fetchval("""
        SELECT COUNT(*) FROM sadie_gtm.hotels 
        WHERE country IN ('USA', 'United States') 
          AND state IN ('VIC', 'NSW', 'QLD', 'TAS', 'ACT', 'SA', 'NT')
    """)
    stats['australian_in_usa'] = aus_count
    
    # US states that are 2-letter codes
    us_codes = await conn.fetchval("""
        SELECT COUNT(*) FROM sadie_gtm.hotels 
        WHERE country IN ('USA', 'United States') 
          AND state ~ '^[A-Z]{2}$'
          AND state NOT IN ('VIC', 'NSW', 'QLD', 'TAS', 'ACT', 'NT')
    """)
    stats['us_state_codes'] = us_codes
    
    # States with zip codes attached
    zips = await conn.fetchval("""
        SELECT COUNT(*) FROM sadie_gtm.hotels 
        WHERE state ~ '[0-9]'
    """)
    stats['states_with_zips'] = zips
    
    return stats


async def fix_australian_hotels(conn: asyncpg.Connection, dry_run: bool = False) -> int:
    """Fix hotels with Australian states incorrectly marked as USA."""
    aus_states = ('VIC', 'NSW', 'QLD', 'TAS', 'ACT', 'SA', 'NT', 'WA')
    
    if dry_run:
        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM sadie_gtm.hotels 
            WHERE country IN ('USA', 'United States') 
              AND state IN {aus_states}
        """)
        logger.info(f"[DRY RUN] Would fix {count} Australian hotels incorrectly in USA")
        return count
    
    # Update country to Australia and normalize state names
    for code, name in AU_STATE_NAMES.items():
        result = await conn.execute("""
            UPDATE sadie_gtm.hotels 
            SET country = 'Australia', state = $2, updated_at = NOW()
            WHERE country IN ('USA', 'United States') AND state = $1
        """, code, name)
        updated = int(result.split()[-1])
        if updated > 0:
            logger.info(f"  Fixed {updated} hotels: state={code} -> country=Australia, state={name}")
    
    return await conn.fetchval("SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country = 'Australia'")


async def fix_states_with_zips(conn: asyncpg.Connection, dry_run: bool = False) -> int:
    """Fix states that have zip codes attached like 'WY 83012'."""
    
    rows = await conn.fetch("""
        SELECT DISTINCT state FROM sadie_gtm.hotels 
        WHERE state ~ '^[A-Z]{2} [0-9]+$'
    """)
    
    if not rows:
        return 0
    
    fixed = 0
    for row in rows:
        old_state = row['state']
        code = old_state.split()[0]  # Extract "WY" from "WY 83012"
        new_state = US_STATE_NAMES.get(code, code)
        
        if dry_run:
            count = await conn.fetchval("SELECT COUNT(*) FROM sadie_gtm.hotels WHERE state = $1", old_state)
            logger.info(f"[DRY RUN] Would fix {count} hotels: '{old_state}' -> '{new_state}'")
            fixed += count
        else:
            result = await conn.execute("""
                UPDATE sadie_gtm.hotels SET state = $2, updated_at = NOW() WHERE state = $1
            """, old_state, new_state)
            updated = int(result.split()[-1])
            if updated > 0:
                logger.info(f"  Fixed {updated} hotels: '{old_state}' -> '{new_state}'")
                fixed += updated
    
    return fixed


async def normalize_countries(conn: asyncpg.Connection, dry_run: bool = False) -> int:
    """Normalize country codes to full names."""
    fixed = 0
    
    for code, name in COUNTRY_NAMES.items():
        if code == "CA":  # Skip CA - could be California or Canada
            continue
        if code in ("SA",):  # Skip SA - could be South Australia or Saudi Arabia
            continue
            
        count = await conn.fetchval("SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country = $1", code)
        if count == 0:
            continue
        
        if dry_run:
            logger.info(f"[DRY RUN] Would normalize {count} hotels: country='{code}' -> '{name}'")
            fixed += count
        else:
            result = await conn.execute("""
                UPDATE sadie_gtm.hotels SET country = $2, updated_at = NOW() WHERE country = $1
            """, code, name)
            updated = int(result.split()[-1])
            if updated > 0:
                logger.info(f"  Normalized {updated} hotels: country='{code}' -> '{name}'")
                fixed += updated
    
    return fixed


async def normalize_us_states(conn: asyncpg.Connection, dry_run: bool = False) -> int:
    """Normalize US state codes to full names."""
    fixed = 0
    
    for code, name in US_STATE_NAMES.items():
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM sadie_gtm.hotels 
            WHERE state = $1 AND country IN ('USA', 'United States')
        """, code)
        if count == 0:
            continue
        
        if dry_run:
            logger.info(f"[DRY RUN] Would normalize {count} hotels: state='{code}' -> '{name}'")
            fixed += count
        else:
            result = await conn.execute("""
                UPDATE sadie_gtm.hotels SET state = $2, updated_at = NOW() 
                WHERE state = $1 AND country IN ('USA', 'United States')
            """, code, name)
            updated = int(result.split()[-1])
            if updated > 0:
                logger.debug(f"  Normalized {updated} hotels: state='{code}' -> '{name}'")
                fixed += updated
    
    return fixed


async def run_status():
    """Show normalization status."""
    conn = await asyncpg.connect(DB_URL, ssl='require', statement_cache_size=0)
    
    try:
        stats = await get_status(conn)
        
        logger.info("=== Location Normalization Status ===\n")
        logger.info(f"Countries needing normalization: {stats['countries_to_normalize']}")
        for country, count in stats['country_breakdown'].items():
            logger.info(f"  {country}: {count}")
        
        logger.info(f"\nAustralian hotels incorrectly in USA: {stats['australian_in_usa']}")
        logger.info(f"US state codes to normalize: {stats['us_state_codes']}")
        logger.info(f"States with zip codes attached: {stats['states_with_zips']}")
        
    finally:
        await conn.close()


async def run_normalize(
    dry_run: bool = False,
    countries_only: bool = False,
    states_only: bool = False,
):
    """Run location normalization."""
    conn = await asyncpg.connect(DB_URL, ssl='require', statement_cache_size=0)
    
    try:
        total_fixed = 0
        
        if not states_only:
            logger.info("=== Fixing Australian hotels incorrectly in USA ===")
            fixed = await fix_australian_hotels(conn, dry_run)
            total_fixed += fixed
        
        if not states_only:
            logger.info("\n=== Fixing states with zip codes ===")
            fixed = await fix_states_with_zips(conn, dry_run)
            total_fixed += fixed
        
        if not states_only:
            logger.info("\n=== Normalizing country codes ===")
            fixed = await normalize_countries(conn, dry_run)
            total_fixed += fixed
        
        if not countries_only:
            logger.info("\n=== Normalizing US state codes ===")
            fixed = await normalize_us_states(conn, dry_run)
            total_fixed += fixed
        
        logger.info(f"\n{'[DRY RUN] Would fix' if dry_run else 'Fixed'} {total_fixed} total records")
        
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Normalize location data (countries, states, cities)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--status", action="store_true", help="Show normalization status only")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without making changes")
    parser.add_argument("--countries-only", action="store_true", help="Only normalize countries")
    parser.add_argument("--states-only", action="store_true", help="Only normalize states")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(run_status())
    else:
        asyncio.run(run_normalize(
            dry_run=args.dry_run,
            countries_only=args.countries_only,
            states_only=args.states_only,
        ))


if __name__ == "__main__":
    main()
