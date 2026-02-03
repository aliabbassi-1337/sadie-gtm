"""Normalize bad data to NULL so enrichers can overwrite it.

Usage:
    # Dry run - show what would be cleaned
    uv run python -m workflows.normalize_data --dry-run
    
    # Run the normalization
    uv run python -m workflows.normalize_data
    
    # Normalize specific fields only
    uv run python -m workflows.normalize_data --fields state,city
"""

import asyncio
import argparse
from typing import Dict, List, Optional
from loguru import logger

from db.client import init_db, close_db, get_conn


# Bad values that should be normalized to NULL
BAD_VALUES = ["", " ", "-", ".", "N/A", "n/a", "NA", "null", "NULL", "None", "none", "Unknown", "unknown"]

# Fields to normalize (column_name -> description)
NORMALIZABLE_FIELDS = {
    "state": "State/Province",
    "city": "City",
    "country": "Country",
    "address": "Address",
    "phone_google": "Phone (Google)",
    "phone_website": "Phone (Website)",
    "email": "Email",
    "zip_code": "ZIP Code",
    "contact_name": "Contact Name",
}

# Special cases - values that look bad but are actually valid
VALID_EXCEPTIONS = {
    "country": ["NA"],  # NA = Namibia (valid ISO code)
}


async def get_bad_data_counts(fields: Optional[List[str]] = None) -> Dict[str, Dict[str, int]]:
    """Get counts of bad data for each field."""
    fields = fields or list(NORMALIZABLE_FIELDS.keys())
    results = {}
    
    async with get_conn() as conn:
        for field in fields:
            if field not in NORMALIZABLE_FIELDS:
                continue
                
            # Build exclusion for valid exceptions
            exceptions = VALID_EXCEPTIONS.get(field, [])
            exception_clause = ""
            if exceptions:
                placeholders = ", ".join(f"${i+1}" for i in range(len(exceptions)))
                exception_clause = f" AND {field} NOT IN ({placeholders})"
            
            # Count each bad value type
            field_counts = {}
            
            # Empty string
            query = f"SELECT COUNT(*) FROM sadie_gtm.hotels WHERE {field} = ''"
            row = await conn.fetchrow(query)
            if row[0] > 0:
                field_counts["empty string"] = row[0]
            
            # Whitespace only
            query = f"SELECT COUNT(*) FROM sadie_gtm.hotels WHERE {field} ~ '^\\s+$'"
            row = await conn.fetchrow(query)
            if row[0] > 0:
                field_counts["whitespace"] = row[0]
            
            # Dash
            query = f"SELECT COUNT(*) FROM sadie_gtm.hotels WHERE {field} = '-'"
            row = await conn.fetchrow(query)
            if row[0] > 0:
                field_counts["dash (-)"] = row[0]
            
            # Other placeholders (excluding valid exceptions)
            bad_placeholders = ["N/A", "n/a", "null", "NULL", "None", "none", "Unknown", "unknown", "."]
            if "NA" not in exceptions:
                bad_placeholders.append("NA")
            
            if bad_placeholders:
                placeholders_sql = ", ".join(f"'{v}'" for v in bad_placeholders)
                query = f"SELECT {field}, COUNT(*) as cnt FROM sadie_gtm.hotels WHERE {field} IN ({placeholders_sql}) GROUP BY {field}"
                rows = await conn.fetch(query)
                for r in rows:
                    if r["cnt"] > 0:
                        field_counts[f'"{r[field]}"'] = r["cnt"]
            
            if field_counts:
                results[field] = field_counts
    
    return results


async def normalize_field(field: str, dry_run: bool = False) -> int:
    """Normalize bad values to NULL for a specific field.
    
    Returns count of updated rows.
    """
    if field not in NORMALIZABLE_FIELDS:
        logger.warning(f"Unknown field: {field}")
        return 0
    
    exceptions = VALID_EXCEPTIONS.get(field, [])
    
    async with get_conn() as conn:
        # Build WHERE clause for all bad values
        conditions = [
            f"{field} = ''",
            f"{field} ~ '^\\s+$'",  # whitespace only
            f"{field} = '-'",
            f"{field} = '.'",
            f"LOWER({field}) IN ('n/a', 'null', 'none', 'unknown')",
        ]
        
        # Add NA unless it's a valid exception (e.g., Namibia for country)
        if "NA" not in exceptions:
            conditions.append(f"{field} = 'NA'")
        
        where_clause = " OR ".join(f"({c})" for c in conditions)
        
        if dry_run:
            # Just count
            query = f"SELECT COUNT(*) FROM sadie_gtm.hotels WHERE {where_clause}"
            row = await conn.fetchrow(query)
            return row[0]
        else:
            # Actually update
            query = f"""
                UPDATE sadie_gtm.hotels 
                SET {field} = NULL, updated_at = NOW()
                WHERE {where_clause}
            """
            result = await conn.execute(query)
            # Parse "UPDATE N" to get count
            return int(result.split()[-1]) if result else 0


async def run(fields: Optional[List[str]] = None, dry_run: bool = False):
    """Run normalization on specified fields (or all fields if None)."""
    await init_db()
    
    fields = fields or list(NORMALIZABLE_FIELDS.keys())
    
    print("=" * 60)
    print("DATA NORMALIZATION")
    print("=" * 60)
    
    if dry_run:
        print("\n[DRY RUN] Showing bad data that would be normalized to NULL:\n")
        
        counts = await get_bad_data_counts(fields)
        
        total = 0
        for field, bad_values in counts.items():
            field_total = sum(bad_values.values())
            total += field_total
            print(f"{NORMALIZABLE_FIELDS.get(field, field)} ({field}):")
            for value_type, count in bad_values.items():
                print(f"  {value_type}: {count}")
            print(f"  TOTAL: {field_total}")
            print()
        
        print(f"Grand total: {total} bad values across {len(counts)} fields")
        
        if "country" in fields:
            print("\nNote: country='NA' is kept (Namibia - valid ISO code)")
    
    else:
        print("\nNormalizing bad data to NULL...\n")
        
        total = 0
        for field in fields:
            if field not in NORMALIZABLE_FIELDS:
                logger.warning(f"Skipping unknown field: {field}")
                continue
            
            count = await normalize_field(field, dry_run=False)
            if count > 0:
                print(f"  {NORMALIZABLE_FIELDS[field]} ({field}): {count} rows normalized")
                total += count
        
        print(f"\nTotal: {total} rows normalized to NULL")
    
    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize bad data to NULL")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be cleaned without making changes")
    parser.add_argument("--fields", type=str, help="Comma-separated list of fields to normalize (default: all)")
    
    args = parser.parse_args()
    
    fields = None
    if args.fields:
        fields = [f.strip() for f in args.fields.split(",")]
    
    asyncio.run(run(fields=fields, dry_run=args.dry_run))
