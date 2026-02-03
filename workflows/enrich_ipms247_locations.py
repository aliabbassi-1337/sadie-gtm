"""IPMS247 Location Enrichment Workflow - Fix missing/incorrect location data.

USAGE:
    # Status check
    uv run python workflows/enrich_ipms247_locations.py status

    # Infer countries from city/state names
    uv run python workflows/enrich_ipms247_locations.py infer-countries --dry-run
    uv run python workflows/enrich_ipms247_locations.py infer-countries

    # Normalize state codes (FL -> Florida, etc.)
    uv run python workflows/enrich_ipms247_locations.py normalize-states

    # Reverse geocode (fill city/state from coordinates)
    uv run python workflows/enrich_ipms247_locations.py reverse-geocode --limit 100

    # Run all fixes
    uv run python workflows/enrich_ipms247_locations.py fix-all
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from typing import Optional
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.leadgen.geocoding import reverse_geocode


# =============================================================================
# LOCATION MAPPINGS
# =============================================================================

# US state codes to full names
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam',
}

# Indian state codes
INDIAN_STATES = {
    'AN': 'Andaman and Nicobar Islands', 'AP': 'Andhra Pradesh', 'AR': 'Arunachal Pradesh',
    'AS': 'Assam', 'BR': 'Bihar', 'CH': 'Chandigarh', 'CT': 'Chhattisgarh', 'DD': 'Daman and Diu',
    'DL': 'Delhi', 'GA': 'Goa', 'GJ': 'Gujarat', 'HP': 'Himachal Pradesh', 'HR': 'Haryana',
    'JH': 'Jharkhand', 'JK': 'Jammu and Kashmir', 'KA': 'Karnataka', 'KL': 'Kerala',
    'LA': 'Ladakh', 'LD': 'Lakshadweep', 'MH': 'Maharashtra', 'ML': 'Meghalaya', 'MN': 'Manipur',
    'MP': 'Madhya Pradesh', 'MZ': 'Mizoram', 'NL': 'Nagaland', 'OD': 'Odisha', 'PB': 'Punjab',
    'PY': 'Puducherry', 'RJ': 'Rajasthan', 'SK': 'Sikkim', 'TN': 'Tamil Nadu', 'TS': 'Telangana',
    'TR': 'Tripura', 'UK': 'Uttarakhand', 'UP': 'Uttar Pradesh', 'WB': 'West Bengal',
}

# City/state to country mapping (comprehensive)
LOCATION_TO_COUNTRY = {
    # US States (full names)
    **{v.lower(): 'United States' for v in US_STATES.values()},
    # Malaysian states/cities
    'johor': 'Malaysia', 'selangor': 'Malaysia', 'melaka': 'Malaysia', 'penang': 'Malaysia',
    'perak': 'Malaysia', 'kedah': 'Malaysia', 'kelantan': 'Malaysia', 'terengganu': 'Malaysia',
    'pahang': 'Malaysia', 'negeri sembilan': 'Malaysia', 'perlis': 'Malaysia',
    'sabah': 'Malaysia', 'sarawak': 'Malaysia', 'labuan': 'Malaysia', 'putrajaya': 'Malaysia',
    'wilayah persekutuan': 'Malaysia', 'mersing': 'Malaysia', 'petaling jaya': 'Malaysia',
    'kuala lumpur': 'Malaysia', 'langkawi': 'Malaysia', 'george town': 'Malaysia',
    'pulau pinang': 'Malaysia', 'miri': 'Malaysia', 'kota kinabalu': 'Malaysia',
    # Kenya
    'nairobi': 'Kenya', 'mombasa': 'Kenya', 'malindi': 'Kenya', 'kenya': 'Kenya',
    # South Africa
    'durban': 'South Africa', 'cape town': 'South Africa', 'johannesburg': 'South Africa',
    'hluhluwe': 'South Africa', 'pretoria': 'South Africa',
    # UAE
    'dubai': 'United Arab Emirates', 'abu dhabi': 'United Arab Emirates',
    # Brazil
    'rio de janeiro': 'Brazil', 'sao paulo': 'Brazil', 'salvador': 'Brazil',
    # Saudi Arabia
    'taif': 'Saudi Arabia', 'riyadh': 'Saudi Arabia', 'jeddah': 'Saudi Arabia',
    # Romania
    'romania': 'Romania', 'constanta': 'Romania', 'bucharest': 'Romania',
    # Ecuador
    'tungurahua': 'Ecuador', 'quito': 'Ecuador', 'guayaquil': 'Ecuador',
    # Belize
    'caye caulker': 'Belize', 'belize city': 'Belize', 'san ignacio': 'Belize', 'ambergris': 'Belize',
    # Cambodia
    'kampot': 'Cambodia', 'kampot province': 'Cambodia', 'siem reap': 'Cambodia', 'phnom penh': 'Cambodia',
    # Ghana
    'accra': 'Ghana', 'osu - accra': 'Ghana', 'kumasi': 'Ghana',
    # Uganda
    'mbale': 'Uganda', 'kampala': 'Uganda',
    # Gabon
    'libreville': 'Gabon',
    # Tanzania
    'zanzibar': 'Tanzania', 'dar es salaam': 'Tanzania', 'michamvi': 'Tanzania', 'nungwi': 'Tanzania',
    # Hungary
    'badacsonytomaj': 'Hungary', 'gyenesdiás': 'Hungary', 'balatonederics': 'Hungary',
    'ábrahámhegy': 'Hungary', 'lesenceistvánd': 'Hungary', 'badacsonytördemic': 'Hungary',
    'budapest': 'Hungary',
    # Maldives
    'male': 'Maldives', 'dhiffushi': 'Maldives', 'kaafu atoll': 'Maldives',
    'gulhi': 'Maldives', 'dhigurah': 'Maldives', 'maafushi': 'Maldives',
    # Bangladesh
    'dhaka': 'Bangladesh', 'chittagong': 'Bangladesh', 'sylhet': 'Bangladesh',
    # Panama
    'boquete': 'Panama', 'panama city': 'Panama', 'las tablas': 'Panama', 'david': 'Panama',
    # Cameroon
    'yaounde': 'Cameroon', 'douala': 'Cameroon',
    # Costa Rica
    'guanacaste': 'Costa Rica', 'playa brasilito': 'Costa Rica', 'san jose': 'Costa Rica',
    # Spain
    'gran canaria': 'Spain', 'tenerife': 'Spain', 'playa del ingles': 'Spain',
    'barcelona': 'Spain', 'madrid': 'Spain', 'malaga': 'Spain',
    # Morocco
    'merzouga': 'Morocco', 'erg chebbi': 'Morocco', 'marrakech': 'Morocco', 'casablanca': 'Morocco',
    # Iceland
    'reykholt': 'Iceland', 'reykjavik': 'Iceland',
    # Jordan
    'amman': 'Jordan', 'jordan': 'Jordan', 'petra': 'Jordan',
    # Dominican Republic
    'santo domingo': 'Dominican Republic', 'distrito nacional': 'Dominican Republic',
    'punta cana': 'Dominican Republic',
    # Greece
    'pefkochori': 'Greece', 'chalkidiki': 'Greece', 'santorini': 'Greece', 'mykonos': 'Greece',
    'athens': 'Greece', 'crete': 'Greece',
    # Rwanda
    'kigali': 'Rwanda',
    # Kosovo
    'pristina': 'Kosovo',
    # North Macedonia
    'mavrovo': 'North Macedonia', 'skopje': 'North Macedonia',
    # Italy
    'loano': 'Italy', 'rimini': 'Italy', 'rome': 'Italy', 'florence': 'Italy', 'venice': 'Italy',
    # Sri Lanka
    'colombo': 'Sri Lanka', 'kandy': 'Sri Lanka', 'galle': 'Sri Lanka', 'tissamaharama': 'Sri Lanka',
    # Nepal
    'kathmandu': 'Nepal', 'pokhara': 'Nepal',
    # Thailand
    'koh samui': 'Thailand', 'phuket': 'Thailand', 'bangkok': 'Thailand', 'chiang mai': 'Thailand',
    # Indonesia
    'bali': 'Indonesia', 'ubud': 'Indonesia', 'jakarta': 'Indonesia', 'yogyakarta': 'Indonesia',
    # Philippines
    'manila': 'Philippines', 'cebu': 'Philippines', 'boracay': 'Philippines', 'palawan': 'Philippines',
    # Vietnam
    'phan thiet': 'Vietnam', 'ho chi minh': 'Vietnam', 'hanoi': 'Vietnam', 'da nang': 'Vietnam',
    # Singapore
    'singapore': 'Singapore',
    # Hong Kong
    'hong kong': 'Hong Kong',
    # Taiwan
    'taipei': 'Taiwan',
    # Japan
    'tokyo': 'Japan', 'osaka': 'Japan', 'kyoto': 'Japan',
    # South Korea
    'seoul': 'South Korea', 'busan': 'South Korea',
    # Portugal
    'lisbon': 'Portugal', 'porto': 'Portugal', 'faro': 'Portugal',
    # Netherlands
    'amsterdam': 'Netherlands',
    # Germany
    'berlin': 'Germany', 'munich': 'Germany', 'frankfurt': 'Germany',
    # France
    'paris': 'France', 'nice': 'France', 'marseille': 'France',
    # Austria
    'vienna': 'Austria', 'salzburg': 'Austria',
    # Turkey
    'istanbul': 'Turkey', 'antalya': 'Turkey', 'bodrum': 'Turkey',
    # Croatia
    'dubrovnik': 'Croatia', 'split': 'Croatia', 'zagreb': 'Croatia',
    # Montenegro
    'kotor': 'Montenegro',
    # Argentina
    'buenos aires': 'Argentina',
    # Chile
    'santiago': 'Chile',
    # Peru
    'lima': 'Peru', 'cusco': 'Peru',
    # Colombia
    'bogota': 'Colombia', 'cartagena': 'Colombia', 'medellin': 'Colombia',
    # Egypt
    'cairo': 'Egypt', 'luxor': 'Egypt', 'hurghada': 'Egypt', 'sharm el sheikh': 'Egypt',
    # Tunisia
    'tunis': 'Tunisia',
    # Israel
    'tel aviv': 'Israel', 'jerusalem': 'Israel',
}


# =============================================================================
# ENRICHMENT FUNCTIONS
# =============================================================================

async def get_status(source: str = 'ipms247_archive') -> dict:
    """Get location enrichment status for source."""
    await init_db()
    try:
        async with get_conn() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source = $1", source
            )
            with_country = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source = $1 AND country IS NOT NULL", source
            )
            with_city = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source = $1 AND city IS NOT NULL", source
            )
            with_state = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source = $1 AND state IS NOT NULL", source
            )
            with_coords = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source = $1 AND location IS NOT NULL", source
            )
            
            # Get country distribution
            countries = await conn.fetch("""
                SELECT country, COUNT(*) as cnt
                FROM sadie_gtm.hotels WHERE source = $1
                GROUP BY country ORDER BY cnt DESC LIMIT 15
            """, source)
            
            return {
                'total': total,
                'with_country': with_country,
                'with_city': with_city,
                'with_state': with_state,
                'with_coords': with_coords,
                'countries': [(r['country'], r['cnt']) for r in countries],
            }
    finally:
        await close_db()


async def infer_countries(source: str = 'ipms247_archive', dry_run: bool = False) -> dict:
    """Infer country from city/state names."""
    await init_db()
    stats = {'checked': 0, 'updated': 0, 'patterns': {}}
    
    try:
        async with get_conn() as conn:
            # Get hotels with NULL country
            rows = await conn.fetch("""
                SELECT id, name, city, state
                FROM sadie_gtm.hotels 
                WHERE source = $1 AND country IS NULL
            """, source)
            
            stats['checked'] = len(rows)
            updates = []
            
            for r in rows:
                city = (r['city'] or '').lower().strip().rstrip('.')
                state = (r['state'] or '').lower().strip().rstrip('.')
                
                country = None
                matched_key = None
                
                # Check city against mappings
                for key in LOCATION_TO_COUNTRY:
                    if key in city or city == key:
                        country = LOCATION_TO_COUNTRY[key]
                        matched_key = key
                        break
                
                # Check state against mappings
                if not country:
                    for key in LOCATION_TO_COUNTRY:
                        if key in state or state == key:
                            country = LOCATION_TO_COUNTRY[key]
                            matched_key = key
                            break
                
                # Check for country names in city/state (e.g., "Malaysia.")
                if not country:
                    for pattern in ['malaysia', 'india', 'tanzania', 'kenya', 'thailand', 'indonesia']:
                        if pattern in city or pattern in state:
                            country = LOCATION_TO_COUNTRY.get(pattern, pattern.title())
                            matched_key = pattern
                            break
                
                if country:
                    updates.append((r['id'], country))
                    stats['patterns'][matched_key] = stats['patterns'].get(matched_key, 0) + 1
            
            stats['updated'] = len(updates)
            
            if not dry_run and updates:
                for hotel_id, country in updates:
                    await conn.execute(
                        "UPDATE sadie_gtm.hotels SET country = $1 WHERE id = $2",
                        country, hotel_id
                    )
            
            return stats
            
    finally:
        await close_db()


async def normalize_states(source: str = 'ipms247_archive') -> dict:
    """Normalize US/Indian state codes to full names."""
    await init_db()
    stats = {'us_states': 0, 'indian_states': 0}
    
    try:
        async with get_conn() as conn:
            # Normalize US state codes and set country
            for code, name in US_STATES.items():
                result = await conn.execute("""
                    UPDATE sadie_gtm.hotels 
                    SET state = $1, country = 'United States'
                    WHERE source = $2 AND UPPER(state) = $3
                """, name, source, code)
                count = int(result.split()[-1])
                stats['us_states'] += count
            
            # Normalize Indian state codes and set country
            for code, name in INDIAN_STATES.items():
                result = await conn.execute("""
                    UPDATE sadie_gtm.hotels 
                    SET state = $1, country = 'India'
                    WHERE source = $2 AND UPPER(state) = $3
                """, name, source, code)
                count = int(result.split()[-1])
                stats['indian_states'] += count
            
            return stats
            
    finally:
        await close_db()


async def reverse_geocode_missing(
    source: str = 'ipms247_archive',
    limit: int = 100,
    concurrency: int = 1,  # Nominatim rate limit
) -> dict:
    """Fill city/state from coordinates using reverse geocoding."""
    await init_db()
    stats = {'processed': 0, 'enriched': 0, 'failed': 0}
    
    try:
        async with get_conn() as conn:
            # Get hotels with coords but missing city
            rows = await conn.fetch("""
                SELECT id, name, 
                       ST_Y(location::geometry) as lat,
                       ST_X(location::geometry) as lng
                FROM sadie_gtm.hotels 
                WHERE source = $1 
                AND location IS NOT NULL
                AND city IS NULL
                LIMIT $2
            """, source, limit)
            
            logger.info(f"Found {len(rows)} hotels needing reverse geocoding")
            
            for r in rows:
                stats['processed'] += 1
                
                result = await reverse_geocode(r['lat'], r['lng'])
                
                if result and result.city:
                    await conn.execute("""
                        UPDATE sadie_gtm.hotels 
                        SET city = $1, state = COALESCE(state, $2), country = COALESCE(country, $3)
                        WHERE id = $4
                    """, result.city, result.state, result.country, r['id'])
                    
                    stats['enriched'] += 1
                    logger.info(f"  {r['name'][:40]}: {result.city}, {result.state}")
                else:
                    stats['failed'] += 1
                    logger.warning(f"  {r['name'][:40]}: no result")
                
                # Nominatim rate limit
                await asyncio.sleep(1.1)
            
            return stats
            
    finally:
        await close_db()


async def fix_all(source: str = 'ipms247_archive') -> dict:
    """Run all location fixes."""
    stats = {}
    
    logger.info("Step 1: Normalizing state codes...")
    stats['normalize_states'] = await normalize_states(source)
    logger.info(f"  US states: {stats['normalize_states']['us_states']}")
    logger.info(f"  Indian states: {stats['normalize_states']['indian_states']}")
    
    logger.info("Step 2: Inferring countries...")
    stats['infer_countries'] = await infer_countries(source, dry_run=False)
    logger.info(f"  Updated: {stats['infer_countries']['updated']}")
    
    return stats


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="IPMS247 Location Enrichment")
    parser.add_argument('--source', default='ipms247_archive', help='Source filter')
    
    subparsers = parser.add_subparsers(dest='command')
    
    # Status
    subparsers.add_parser('status', help='Show enrichment status')
    
    # Infer countries
    infer_parser = subparsers.add_parser('infer-countries', help='Infer countries from city/state')
    infer_parser.add_argument('--dry-run', action='store_true')
    
    # Normalize states
    subparsers.add_parser('normalize-states', help='Normalize state codes')
    
    # Reverse geocode
    geo_parser = subparsers.add_parser('reverse-geocode', help='Reverse geocode missing cities')
    geo_parser.add_argument('--limit', type=int, default=100)
    
    # Fix all
    subparsers.add_parser('fix-all', help='Run all location fixes')
    
    args = parser.parse_args()
    
    if args.command == 'status':
        status = asyncio.run(get_status(args.source))
        print(f"\n{'='*60}")
        print(f"LOCATION STATUS: {args.source}")
        print(f"{'='*60}")
        print(f"Total hotels:      {status['total']}")
        print(f"With country:      {status['with_country']} ({status['with_country']*100//status['total'] if status['total'] else 0}%)")
        print(f"With city:         {status['with_city']} ({status['with_city']*100//status['total'] if status['total'] else 0}%)")
        print(f"With state:        {status['with_state']} ({status['with_state']*100//status['total'] if status['total'] else 0}%)")
        print(f"With coordinates:  {status['with_coords']} ({status['with_coords']*100//status['total'] if status['total'] else 0}%)")
        print(f"\nCountry distribution:")
        for country, count in status['countries']:
            print(f"  {str(country):25}: {count}")
    
    elif args.command == 'infer-countries':
        stats = asyncio.run(infer_countries(args.source, args.dry_run))
        print(f"\nChecked: {stats['checked']}")
        print(f"{'Would update' if args.dry_run else 'Updated'}: {stats['updated']}")
        if stats['patterns']:
            print(f"\nTop patterns matched:")
            for k, v in sorted(stats['patterns'].items(), key=lambda x: -x[1])[:10]:
                print(f"  {k}: {v}")
    
    elif args.command == 'normalize-states':
        stats = asyncio.run(normalize_states(args.source))
        print(f"\nUS states normalized: {stats['us_states']}")
        print(f"Indian states normalized: {stats['indian_states']}")
    
    elif args.command == 'reverse-geocode':
        stats = asyncio.run(reverse_geocode_missing(args.source, args.limit))
        print(f"\nProcessed: {stats['processed']}")
        print(f"Enriched: {stats['enriched']}")
        print(f"Failed: {stats['failed']}")
    
    elif args.command == 'fix-all':
        stats = asyncio.run(fix_all(args.source))
        print(f"\n{'='*60}")
        print("ALL FIXES COMPLETE")
        print(f"{'='*60}")
        print(f"State normalization: {stats['normalize_states']['us_states'] + stats['normalize_states']['indian_states']}")
        print(f"Country inference: {stats['infer_countries']['updated']}")
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
