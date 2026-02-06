"""State normalization utilities for enrichment.

This is the SINGLE SOURCE OF TRUTH for state normalization.
All enrichment code should use normalize_state() from this module.
"""

import re
from typing import Optional

# US state abbreviation -> full name
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

# Australian state mappings
AU_STATES = {
    'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
    'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
    'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory',
}

# Canadian province abbreviation -> full name
CA_PROVINCES = {
    'AB': 'Alberta',
    'BC': 'British Columbia',
    'MB': 'Manitoba',
    'NB': 'New Brunswick',
    'NL': 'Newfoundland and Labrador',
    'NS': 'Nova Scotia',
    'NT': 'Northwest Territories',
    'NU': 'Nunavut',
    'ON': 'Ontario',
    'PE': 'Prince Edward Island',
    'QC': 'Quebec',
    'SK': 'Saskatchewan',
    'YT': 'Yukon',
}

# Canadian province variations/typos -> normalized name
CA_PROVINCE_VARIATIONS = {
    # Abbreviation case variants
    'on': 'Ontario',
    'bc': 'British Columbia',
    'ab': 'Alberta',
    # Typos
    'British Colombia': 'British Columbia',
    'British Columbia,': 'British Columbia',
    # Accent variations
    'Québec': 'Quebec',
    # Short forms
    'Newfoundland': 'Newfoundland and Labrador',
    'P.E.I.': 'Prince Edward Island',
    'PEI': 'Prince Edward Island',
    # Cities incorrectly used as province
    'Montreal': 'Quebec',
    'Toronto': 'Ontario',
    'Banff': 'Alberta',
    'Bonnyville': 'Alberta',
    # Junk
    '*': None,
    '-': None,
    'CA': None,  # Ambiguous - could be California
    # Wrong country (US states leaking in)
    'California': None,
    'Florida': None,
    'Texas': None,
    'New York': None,
    'Washington': None,
    'Oregon': None,
    'Arizona': None,
    'Nevada': None,
    'Colorado': None,
    'Hawaii': None,
    'Georgia': None,
    'Virginia': None,
    'Montana': None,
    'Idaho': None,
    'Maine': None,
    'Vermont': None,
    # Wrong country (Australian states leaking in)
    'Western Australia': None,
    'South Australia': None,
    'New South Wales': None,
    'Queensland': None,
    'Victoria': None,
    'Tasmania': None,
    'Australian Capital Territory': None,
    'Northern Territory': None,
}

# UK region normalization: variation -> canonical region name
# Goal: consolidate raw values into ~40 usable regions for sales
# Scotland/Wales/NI -> nation-level, England -> major counties only
UK_REGION_VARIATIONS = {
    # ===== Junk values -> clear them =====
    '*': None,
    '.': None,
    '-': None,
    'na': None,
    'UK': None,
    'United Kingdom': None,
    '+44 (0) 207 587 3019': None,
    'SA43 1PP': None,

    # ===== Prefix junk =====
    ',Somerset': 'Somerset',
    '*Sutherland': 'Highland',

    # ===== Nation-level: case normalization =====
    'ENGLAND': 'England',
    'SCOTLAND': 'Scotland',

    # ===== Gaelic / bilingual names =====
    'Alba / Scotland': 'Scotland',
    'Cymru / Wales': 'Wales',
    'Northern Ireland / Tuaisceart Éireann': 'Northern Ireland',

    # ===== ENGLAND: cities/towns -> counties =====
    'Arundel': 'West Sussex',
    'Bath': 'Somerset',
    'Bath and North East Somerset': 'Somerset',
    'Bicester': 'Oxfordshire',
    'Bournemouth': 'Dorset',
    'Bournemouth, Dorset': 'Dorset',
    'Brampton': 'Cumbria',
    'Bristol': 'Somerset',
    'Cambridge': 'Cambridgeshire',
    'Carlisle': 'Cumbria',
    'Central Milton Keynes, Buckinghamshire': 'Buckinghamshire',
    'Cheltenham': 'Gloucestershire',
    'Chichester': 'West Sussex',
    'Chorley': 'Lancashire',
    'Congleton': 'Cheshire',
    'Derby': 'Derbyshire',
    'Devizes': 'Wiltshire',
    'Dudley': 'West Midlands',
    'Durham City': 'Durham',
    'Gloucester': 'Gloucestershire',
    'Greater London': 'London',
    'Harrogate': 'North Yorkshire',
    'Henley on Thames': 'Oxfordshire',
    'Ilchester': 'Somerset',
    'Ilford': 'Essex',
    'Ilfracombe': 'Devon',
    'Lancaster': 'Lancashire',
    'Liverpool': 'Merseyside',
    'Manchester': 'Greater Manchester',
    'Margate': 'Kent',
    'Middlesex': 'London',
    'Morcambe': 'Lancashire',
    'Newlands': 'Cumbria',
    'Northampton': 'Northamptonshire',
    'Northumberland Newcastle Upon Tyne': 'Northumberland',
    'North East Lincolnshire': 'Lincolnshire',
    'Oxford': 'Oxfordshire',
    'Portsmouth': 'Hampshire',
    'Reading': 'Berkshire',
    'Sheffield': 'South Yorkshire',
    'Spalding': 'Lincolnshire',
    'The Lake District': 'Cumbria',
    'Truro': 'Cornwall',
    'Warminster': 'Wiltshire',
    'Winchester': 'Hampshire',
    'York': 'North Yorkshire',
    'Avon': 'Somerset',
    'Humberside': 'East Yorkshire',
    'Teeside': 'North Yorkshire',
    'Yorkshire': 'North Yorkshire',

    # ===== ENGLAND: case/typo fixes =====
    'DEVON': 'Devon',
    'DERBYSHIRE': 'Derbyshire',
    'EAST SUSSEX': 'East Sussex',
    'STROUD': 'Gloucestershire',
    'HASTINGS': 'East Sussex',
    'WINDERMERE': 'Cumbria',
    'buckimghamshire': 'Buckinghamshire',
    'gloucestershire': 'Gloucestershire',
    'Derbys': 'Derbyshire',
    'Cambs': 'Cambridgeshire',
    'Hants': 'Hampshire',
    'Lancahire': 'Lancashire',
    'Warwichshire': 'Warwickshire',
    'Hudderfield': 'West Yorkshire',
    'Yorkshire - North': 'North Yorkshire',
    'Isle Of Wight': 'Isle of Wight',
    'Londres': 'London',
    'Kings Cross': 'London',

    # ===== SCOTLAND: cities/towns -> regions =====
    'Aberdeen': 'Aberdeenshire',
    'Ardlui': 'Stirlingshire',
    'Caithness': 'Highland',
    'Edinburgh': 'Lothian',
    'Glasgow': 'Greater Glasgow',
    'Highlands': 'Highland',
    'Inverness': 'Highland',
    'Kincardineshire': 'Aberdeenshire',
    'Kirkcudbrightshire': 'Dumfries & Galloway',
    'Ross and Cromarty': 'Highland',
    'Stranraer': 'Dumfries & Galloway',
    'Sutherland': 'Highland',
    'Central': 'Stirlingshire',
    'Argyll': 'Argyll and Bute',
    'Perth and Kinross': 'Perthshire',
    'Midlothian': 'Lothian',
    'East Lothian': 'Lothian',
    'dumfries and galloway': 'Dumfries & Galloway',
    'Dumfries and Galloway': 'Dumfries & Galloway',
    'Shetland Islands': 'Highland',

    # ===== WALES: cities/towns -> counties =====
    'Barry': 'Vale of Glamorgan',
    'Cardiff': 'South Glamorgan',
    'Ceridigion': 'Ceredigion',
    'Crickhowell': 'Powys',
    'Llandrindod Wells': 'Powys',
    'Llandudno': 'Conwy',
    'Merthyr Tydfil': 'Wales',
    'Mold': 'Flintshire',
    'Narberth': 'Pembrokeshire',
    'North Wales': 'Wales',
    'Swansea': 'West Glamorgan',
    'Tenby': 'Pembrokeshire',
    'Dyfed': 'Pembrokeshire',

    # ===== NORTHERN IRELAND: consolidate to NI =====
    'Antrim': 'Northern Ireland',
    'County Antrim': 'Northern Ireland',
    'Co. Derry': 'Northern Ireland',
    'Co. Down': 'Northern Ireland',
    'Co. Fermanagh': 'Northern Ireland',
    'Fermanagh': 'Northern Ireland',

    # ===== ENGLAND: small counties -> England =====
    'Bedfordshire': 'England',
    'Cambridgeshire': 'England',
    'Durham': 'England',
    'East Yorkshire': 'England',
    'Hertfordshire': 'England',
    'West Midlands': 'England',

    # ===== Consolidate overlapping areas =====
    'East Ayrshire': 'Scotland',
    'West Kilbride': 'Scotland',
    'Whitchurch': 'Shropshire',

    # ===== CHANNEL ISLANDS =====
    'St Peter Port': 'Channel Islands',
    'Guernsey': 'Channel Islands',
    'Jersey': 'Channel Islands',

    # ===== Wrong country (Australian states) =====
    'Western Australia': None,
    'South Australia': None,
    'New South Wales': None,
    'Queensland': None,
    'QLD': None,
    'Victoria': None,
    'Tasmania': None,
    'Northern Territory': None,
    'Australian Capital Territory': None,

    # ===== Wrong country (US states) =====
    'California': None,
    'Florida': None,
    'Texas': None,
    'New York': None,
    'Washington': None,
    'Oregon': None,
    'Arizona': None,
    'Nevada': None,
    'Colorado': None,
    'Hawaii': None,
    'Georgia': None,

    # ===== Foreign =====
    'Barcelona': None,
    'Ireland': None,
}

# Common variations, typos, and case issues -> normalized full name
# This handles edge cases that abbreviation lookup misses
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

# Valid full state names (for checking if already normalized)
VALID_STATE_NAMES = set(US_STATES.values())

# Derived mappings for text extraction
STATE_NAMES = {name.lower(): name for name in US_STATES.values()}
STATE_ABBREVS = {abbrev.lower(): full for abbrev, full in US_STATES.items()}

# Valid full state names (for validation)
VALID_STATE_NAMES = set(US_STATES.values())

# Common variations/typos that map to valid states
STATE_VARIATIONS = {
    # Case variations
    "CALIFORNIA": "California", "TEXAS": "Texas", "FLORIDA": "Florida",
    "NEW YORK": "New York", "GEORGIA": "Georgia",
    # Lowercase
    "california": "California", "texas": "Texas", "florida": "Florida",
    # Typos and variations
    "Calif": "California", "Calif.": "California", "Ca": "California",
    "Tx": "Texas", "Fl": "Florida", "Ny": "New York", "N.Y.": "New York",
    "D.C.": "District of Columbia", "Washington DC": "District of Columbia",
    "Washington D.C.": "District of Columbia",
}


def normalize_state(state: Optional[str], country: Optional[str] = None) -> Optional[str]:
    """Normalize state to full name.
    
    Normalizes for known countries (US, Australia, Canada, UK).
    Does NOT normalize for other countries to avoid false positives
    (e.g., AR is Argentina's country code, not Arkansas).
    
    Args:
        state: State value (could be abbreviation, full name, or variation)
        country: Country - required for proper normalization
    
    Returns:
        Full state name if found and country matches, otherwise original value.
        Returns None if the value should be cleared (junk/wrong country).
    """
    if not state:
        return state
    
    state_stripped = state.strip()
    
    # Must have country context to normalize properly
    if not country:
        return state
    
    country_lower = country.lower().strip()
    
    # US context
    is_us = country_lower in ('united states', 'usa', 'us', 'united states of america')
    if is_us:
        # Already a valid US full name
        if state_stripped in VALID_STATE_NAMES:
            return state_stripped
        # Check variations (handles case issues, typos)
        if state_stripped in STATE_VARIATIONS:
            return STATE_VARIATIONS[state_stripped]
        # Check US state abbreviations
        state_upper = state_stripped.upper()
        if state_upper in US_STATES:
            return US_STATES[state_upper]
        return state
    
    # Australia context
    is_au = country_lower in ('australia', 'au')
    if is_au:
        au_full_names = set(AU_STATES.values())
        if state_stripped in au_full_names:
            return state_stripped
        state_upper = state_stripped.upper()
        if state_upper in AU_STATES:
            return AU_STATES[state_upper]
        return state
    
    # Canada context
    is_ca = country_lower in ('canada', 'ca')
    if is_ca:
        # Check variations/typos first (handles California -> None, etc.)
        if state_stripped in CA_PROVINCE_VARIATIONS:
            return CA_PROVINCE_VARIATIONS[state_stripped]
        # Already a valid CA full name
        ca_full_names = set(CA_PROVINCES.values())
        if state_stripped in ca_full_names:
            return state_stripped
        # Check province abbreviations
        state_upper = state_stripped.upper()
        if state_upper in CA_PROVINCES:
            return CA_PROVINCES[state_upper]
        return state
    
    # UK context
    is_uk = country_lower in ('united kingdom', 'uk', 'gb', 'great britain')
    if is_uk:
        if state_stripped in UK_REGION_VARIATIONS:
            return UK_REGION_VARIATIONS[state_stripped]
        return state
    
    # Other countries - don't normalize (avoid false positives)
    return state


def extract_state_from_text(text: str) -> Optional[str]:
    """Extract US state from a text string (address, city, etc).
    
    Looks for:
    1. Full state names (case insensitive)
    2. State abbreviations with context clues
    
    Returns the full state name if found, None otherwise.
    """
    if not text:
        return None
    
    text_lower = text.lower().strip()
    
    # First try to find full state names (more specific, less false positives)
    for state_lower, state_full in STATE_NAMES.items():
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(state_lower) + r'\b'
        if re.search(pattern, text_lower):
            return state_full
    
    # Then try abbreviations with context clues
    for abbrev_lower, state_full in STATE_ABBREVS.items():
        # Skip ambiguous 2-letter combos that are common words
        if abbrev_lower in ['in', 'or', 'me', 'hi', 'ok', 'la', 'al']:
            # Only match these if they appear in specific patterns
            patterns = [
                r',\s*' + abbrev_lower + r'\s*\d{5}',  # ", IN 46001"
                r',\s*' + abbrev_lower + r'\s*$',      # ", IN" at end
                r'\b' + abbrev_lower + r'\s+\d{5}',    # "IN 46001"
            ]
        else:
            # Less ambiguous abbreviations - can match more broadly
            patterns = [
                r',\s*' + abbrev_lower + r'\s*\d{5}',  # ", CA 90210"
                r',\s*' + abbrev_lower + r'\s*$',      # ", CA" at end  
                r',\s*' + abbrev_lower + r'\s*,',      # ", CA,"
                r'\s' + abbrev_lower + r'\s+\d{5}',    # " CA 90210"
                r',\s*' + abbrev_lower + r'\b',        # ", CA" anywhere
            ]
        
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return state_full
    
    return None


def extract_state(address: Optional[str], city: Optional[str]) -> Optional[str]:
    """Extract state from address or city field.
    
    Tries address first (more likely to contain state), then city.
    
    Args:
        address: Hotel address field
        city: Hotel city field
        
    Returns:
        Full state name if found, None otherwise
    """
    # Try address first
    if address:
        state = extract_state_from_text(address)
        if state:
            return state
    
    # Then try city (sometimes formatted as "City, State")
    if city:
        state = extract_state_from_text(city)
        if state:
            return state
    
    return None


# Known garbage/invalid state values to reject
GARBAGE_STATES = {
    '-', '--', 'XX', 'N/A', 'NA', 'NONE', 'NULL', 'UNKNOWN', 'OTHER',
    'MEASURE', 'TEST', 'TBD', 'TBA', 'PENDING', '.',
}


def is_valid_state(state: str, country: Optional[str] = None) -> bool:
    """Check if a state value is valid (not garbage).
    
    Args:
        state: State value to validate
        country: Optional country for context-aware validation
        
    Returns:
        True if state appears valid, False if garbage
    """
    if not state:
        return False
    
    state_stripped = state.strip()
    
    # Check against known garbage values
    if state_stripped.upper() in GARBAGE_STATES:
        return False
    
    # Too short (single char) is garbage
    if len(state_stripped) < 2:
        return False
    
    # If we have country context, validate against known states
    if country:
        country_lower = country.lower().strip()
        
        # US context - must be valid US state
        if country_lower in ('united states', 'usa', 'us', 'united states of america'):
            state_upper = state_stripped.upper()
            # Valid if it's a full name or abbreviation
            return (
                state_stripped in VALID_STATE_NAMES or
                state_upper in US_STATES or
                state_stripped in STATE_VARIATIONS
            )
        
        # Australia context
        if country_lower in ('australia', 'au'):
            state_upper = state_stripped.upper()
            au_full_names = set(AU_STATES.values())
            return state_upper in AU_STATES or state_stripped in au_full_names
    
    # Without country context, just reject known garbage
    return True


def validate_and_normalize_state(state: str, country: Optional[str] = None) -> Optional[str]:
    """Validate and normalize a state value. Returns None if invalid.
    
    Use this when saving state values to ensure no garbage gets persisted.
    
    Args:
        state: State value to validate and normalize
        country: Country for context-aware normalization
        
    Returns:
        Normalized state name if valid, None if garbage/invalid
    """
    if not state:
        return None
    
    # Check if it's garbage
    if not is_valid_state(state, country):
        return None
    
    # Normalize and return
    return normalize_state(state, country)
