"""
Workflow: Normalize State Names
===============================
Normalizes US state abbreviations to full names and fixes common variations.

Uses centralized state mappings from services/enrichment/state_utils.py

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
from services.enrichment.state_utils import (
    US_STATES,
    AU_STATES,
    GARBAGE_STATES,
    normalize_state,
)


# Common variations/typos to fix (extends base mappings)
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

# Australian states that might be incorrectly in US data
AU_STATE_NAMES = set(AU_STATES.values())


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


async def normalize_state_in_db(old_value: str, new_value: str, dry_run: bool = False) -> int:
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


async def fix_australian_states_in_us(dry_run: bool = False) -> int:
    """Fix Australian states incorrectly marked as US."""
    async with get_conn() as conn:
        total = 0
        for au_state in AU_STATE_NAMES:
            if dry_run:
                count = await conn.fetchval('''
                    SELECT COUNT(*) FROM sadie_gtm.hotels 
                    WHERE status != -1 AND country = 'United States' AND state = $1
                ''', au_state)
            else:
                result = await conn.execute('''
                    UPDATE sadie_gtm.hotels 
                    SET country = 'Australia', updated_at = CURRENT_TIMESTAMP
                    WHERE status != -1 AND country = 'United States' AND state = $1
                ''', au_state)
                count = int(result.split()[-1]) if result else 0
            
            if count > 0:
                logger.warning(f"  Australian state in US: {au_state} ({count} hotels) -> country = Australia")
                total += count
        
        return total


async def fix_garbage_states(dry_run: bool = False) -> int:
    """Set garbage state values to NULL."""
    async with get_conn() as conn:
        total = 0
        for garbage in GARBAGE_STATES:
            if dry_run:
                count = await conn.fetchval('''
                    SELECT COUNT(*) FROM sadie_gtm.hotels 
                    WHERE status != -1 AND country = 'United States' AND state = $1
                ''', garbage)
            else:
                result = await conn.execute('''
                    UPDATE sadie_gtm.hotels 
                    SET state = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE status != -1 AND country = 'United States' AND state = $1
                ''', garbage)
                count = int(result.split()[-1]) if result else 0
            
            if count > 0:
                logger.info(f"  Garbage state: '{garbage}' ({count} hotels) -> NULL")
                total += count
        
        return total


async def run_normalization(dry_run: bool = False):
    """Run state normalization."""
    await init_db()
    
    try:
        # First fix Australian states in US data
        logger.info("Checking for Australian states in US data...")
        au_fixed = await fix_australian_states_in_us(dry_run)
        if au_fixed > 0:
            logger.info(f"{'Would fix' if dry_run else 'Fixed'} {au_fixed} Australian states")
        
        # Fix garbage states
        logger.info("Checking for garbage state values...")
        garbage_fixed = await fix_garbage_states(dry_run)
        if garbage_fixed > 0:
            logger.info(f"{'Would fix' if dry_run else 'Fixed'} {garbage_fixed} garbage states")
        
        # Get current state variations
        variations = await get_state_variations()
        
        logger.info(f"Found {len(variations)} unique state values")
        
        fixes = []
        
        # Build set of valid full state names
        valid_states = set(US_STATES.values())
        
        for state, count in variations:
            # Skip if already a valid full state name
            if state in valid_states:
                continue
            
            # Use centralized normalize_state function
            normalized = normalize_state(state, 'United States')
            if normalized != state and normalized in valid_states:
                fixes.append((state, normalized, count))
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
                fixed = await normalize_state_in_db(old_val, new_val, dry_run=False)
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
        valid_states = set(US_STATES.values())
        
        for state, count in variations:
            # Check if it's an Australian state
            if state in AU_STATE_NAMES:
                needs_fix.append((state, f"country -> Australia", count))
                logger.error(f"  {state:25} {count:6} (Australian state in US!)")
            # Check if it needs normalization
            elif state.upper() in US_STATES and state != US_STATES[state.upper()]:
                needs_fix.append((state, US_STATES[state.upper()], count))
                logger.warning(f"  {state:25} {count:6} -> {US_STATES[state.upper()]}")
            elif state in STATE_VARIATIONS or state.lower() in STATE_VARIATIONS:
                target = STATE_VARIATIONS.get(state, STATE_VARIATIONS.get(state.lower()))
                needs_fix.append((state, target, count))
                logger.warning(f"  {state:25} {count:6} -> {target}")
            elif state.upper() in GARBAGE_STATES or state in GARBAGE_STATES:
                needs_fix.append((state, "NULL", count))
                logger.warning(f"  {state:25} {count:6} -> NULL (garbage)")
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
