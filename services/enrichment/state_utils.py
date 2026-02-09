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

# Canadian province/territory mappings
CA_PROVINCES = {
    'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba',
    'NB': 'New Brunswick', 'NL': 'Newfoundland and Labrador',
    'NS': 'Nova Scotia', 'NT': 'Northwest Territories', 'NU': 'Nunavut',
    'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec',
    'SK': 'Saskatchewan', 'YT': 'Yukon',
}

# UK constituent countries (not subdivided further)
UK_COUNTRIES = {
    'England', 'Scotland', 'Wales', 'Northern Ireland',
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


def normalize_state(state: Optional[str], country: Optional[str] = None) -> Optional[str]:
    """Normalize state to full name.
    
    Only normalizes for known countries (US, Australia).
    Does NOT normalize for other countries to avoid false positives
    (e.g., AR is Argentina's country code, not Arkansas).
    
    Args:
        state: State value (could be abbreviation, full name, or variation)
        country: Country - required for proper normalization
    
    Returns:
        Full state name if found and country matches, otherwise original value
    """
    if not state:
        return state
    
    state_stripped = state.strip()
    
    # Already a valid US full name? Return as-is
    if state_stripped in VALID_STATE_NAMES:
        return state_stripped
    
    # Already a valid AU full name? Return as-is  
    au_full_names = set(AU_STATES.values())
    if state_stripped in au_full_names:
        return state_stripped
    
    # Must have country context to normalize abbreviations
    if not country:
        return state
    
    country_lower = country.lower().strip()
    
    # US context
    is_us = country_lower in ('united states', 'usa', 'us', 'united states of america')
    if is_us:
        # Check variations first (handles case issues, typos)
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
        state_upper = state_stripped.upper()
        if state_upper in AU_STATES:
            return AU_STATES[state_upper]

        return state

    # Canada context
    is_ca = country_lower in ('canada', 'ca')
    if is_ca:
        state_upper = state_stripped.upper()
        if state_upper in CA_PROVINCES:
            return CA_PROVINCES[state_upper]

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

    # Numeric / zip-code values are garbage (e.g., "90210", "3000", "2321")
    if state_stripped.isdigit():
        return False
    if state_stripped[0].isdigit():
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

        # Canada context
        if country_lower in ('canada', 'ca'):
            state_upper = state_stripped.upper()
            ca_full_names = set(CA_PROVINCES.values())
            return state_upper in CA_PROVINCES or state_stripped in ca_full_names

        # UK context
        if country_lower in ('united kingdom', 'uk', 'gb', 'great britain'):
            return state_stripped in UK_COUNTRIES

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
