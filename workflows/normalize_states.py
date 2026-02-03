"""
Workflow: Normalize State Names
===============================
Normalizes US state abbreviations to full names and fixes common variations.

USAGE:
    # Check what would be fixed (dry run)
    uv run python workflows/normalize_states.py --dry-run

    # Apply fixes
    uv run python workflows/normalize_states.py

    # Show current state variations
    uv run python workflows/normalize_states.py --status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, get_conn


# US state abbreviations to full names
US_STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
    "PR": "Puerto Rico",
    "VI": "Virgin Islands",
    "GU": "Guam",
}

# Common variations/typos to fix
STATE_VARIATIONS = {
    # Case variations (all caps)
    "ALABAMA": "Alabama",
    "ALASKA": "Alaska",
    "ARIZONA": "Arizona",
    "ARKANSAS": "Arkansas",
    "CALIFORNIA": "California",
    "COLORADO": "Colorado",
    "CONNECTICUT": "Connecticut",
    "DELAWARE": "Delaware",
    "FLORIDA": "Florida",
    "GEORGIA": "Georgia",
    "HAWAII": "Hawaii",
    "IDAHO": "Idaho",
    "ILLINOIS": "Illinois",
    "INDIANA": "Indiana",
    "IOWA": "Iowa",
    "KANSAS": "Kansas",
    "KENTUCKY": "Kentucky",
    "LOUISIANA": "Louisiana",
    "MAINE": "Maine",
    "MARYLAND": "Maryland",
    "MASSACHUSETTS": "Massachusetts",
    "MICHIGAN": "Michigan",
    "MINNESOTA": "Minnesota",
    "MISSISSIPPI": "Mississippi",
    "MISSOURI": "Missouri",
    "MONTANA": "Montana",
    "NEBRASKA": "Nebraska",
    "NEVADA": "Nevada",
    "NEW HAMPSHIRE": "New Hampshire",
    "NEW JERSEY": "New Jersey",
    "NEW MEXICO": "New Mexico",
    "NEW YORK": "New York",
    "NORTH CAROLINA": "North Carolina",
    "NORTH DAKOTA": "North Dakota",
    "OHIO": "Ohio",
    "OKLAHOMA": "Oklahoma",
    "OREGON": "Oregon",
    "PENNSYLVANIA": "Pennsylvania",
    "RHODE ISLAND": "Rhode Island",
    "SOUTH CAROLINA": "South Carolina",
    "SOUTH DAKOTA": "South Dakota",
    "TENNESSEE": "Tennessee",
    "TEXAS": "Texas",
    "UTAH": "Utah",
    "VERMONT": "Vermont",
    "VIRGINIA": "Virginia",
    "WASHINGTON": "Washington",
    "WEST VIRGINIA": "West Virginia",
    "WISCONSIN": "Wisconsin",
    "WYOMING": "Wyoming",
    # Lowercase
    "california": "California",
    "texas": "Texas",
    "florida": "Florida",
    "new york": "New York",
    "maryland": "Maryland",
    # Typos and variations
    "Calif": "California",
    "Calif.": "California",
    "Ca": "California",
    "Tx": "Texas",
    "Fl": "Florida",
    "Ny": "New York",
    "N.Y.": "New York",
    "D.C.": "District of Columbia",
    "Washington DC": "District of Columbia",
    "Washington D.C.": "District of Columbia",
}


async def get_state_variations():
    """Get all unique state values and their counts."""
    async with get_conn() as conn:
        rows = await conn.fetch('''
            SELECT state, COUNT(*) as cnt
            FROM sadie_gtm.hotels 
            WHERE status != -1 
              AND country = 'United States'
              AND state IS NOT NULL
            GROUP BY state
            ORDER BY cnt DESC
        ''')
        return [(r['state'], r['cnt']) for r in rows]


async def normalize_state(old_value: str, new_value: str, dry_run: bool = False) -> int:
    """Normalize a state value to the correct full name."""
    async with get_conn() as conn:
        if dry_run:
            count = await conn.fetchval('''
                SELECT COUNT(*) FROM sadie_gtm.hotels 
                WHERE status != -1 AND country = 'United States' AND state = $1
            ''', old_value)
            return count
        else:
            result = await conn.execute('''
                UPDATE sadie_gtm.hotels 
                SET state = $2, updated_at = CURRENT_TIMESTAMP
                WHERE status != -1 AND country = 'United States' AND state = $1
            ''', old_value, new_value)
            # Parse "UPDATE X" to get count
            count = int(result.split()[-1]) if result else 0
            return count


async def run_normalization(dry_run: bool = False):
    """Run state normalization."""
    await init_db()
    
    try:
        variations = await get_state_variations()
        
        logger.info(f"Found {len(variations)} unique state values")
        
        fixes = []
        
        # Build set of valid full state names
        valid_states = set(US_STATES.values())
        
        for state, count in variations:
            # Skip if already a valid full state name
            if state in valid_states:
                continue
                
            # Check if it's an abbreviation
            if state.upper() in US_STATES:
                full_name = US_STATES[state.upper()]
                fixes.append((state, full_name, count))
            # Check if it's a known variation
            elif state in STATE_VARIATIONS:
                fixes.append((state, STATE_VARIATIONS[state], count))
            elif state.lower() in STATE_VARIATIONS:
                fixes.append((state, STATE_VARIATIONS[state.lower()], count))
        
        if not fixes:
            logger.info("No state normalization needed")
            return
        
        logger.info(f"{'Would fix' if dry_run else 'Fixing'} {len(fixes)} state variations:")
        
        total_fixed = 0
        for old_val, new_val, count in fixes:
            logger.info(f"  \"{old_val}\" -> \"{new_val}\" ({count} hotels)")
            if not dry_run:
                fixed = await normalize_state(old_val, new_val, dry_run=False)
                total_fixed += fixed
        
        if dry_run:
            logger.info(f"\nDry run complete. Would fix {sum(c for _, _, c in fixes)} hotels.")
        else:
            logger.success(f"\nNormalized {total_fixed} hotels.")
            
    finally:
        await close_db()


async def show_status():
    """Show current state variations."""
    await init_db()
    
    try:
        variations = await get_state_variations()
        
        logger.info("Current US state values:")
        logger.info("=" * 50)
        
        needs_fix = []
        
        for state, count in variations:
            # Check if it needs normalization
            if state.upper() in US_STATES and state != US_STATES[state.upper()]:
                needs_fix.append((state, US_STATES[state.upper()], count))
                logger.warning(f"  {state:25} {count:6} -> {US_STATES[state.upper()]}")
            elif state in STATE_VARIATIONS or state.lower() in STATE_VARIATIONS:
                target = STATE_VARIATIONS.get(state, STATE_VARIATIONS.get(state.lower()))
                needs_fix.append((state, target, count))
                logger.warning(f"  {state:25} {count:6} -> {target}")
            else:
                logger.info(f"  {state:25} {count:6}")
        
        logger.info("=" * 50)
        logger.info(f"Total: {len(variations)} unique values, {len(needs_fix)} need normalization")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Normalize US state names")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without applying")
    parser.add_argument("--status", action="store_true", help="Show current state variations")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(show_status())
    else:
        asyncio.run(run_normalization(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
