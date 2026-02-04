"""State normalization utilities for enrichment."""

import re
from typing import Optional

# US state mappings
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
}

# Australian state mappings
AU_STATES = {
    'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
    'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
    'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory',
}

# Derived mappings for text extraction
STATE_NAMES = {name.lower(): name for name in US_STATES.values()}
STATE_ABBREVS = {abbrev.lower(): full for abbrev, full in US_STATES.items()}


def normalize_state(state: Optional[str], country: Optional[str] = None) -> Optional[str]:
    """Normalize state abbreviation to full name.
    
    Args:
        state: State value (could be abbreviation or full name)
        country: Country hint (unused currently, but available for future logic)
    
    Returns:
        Full state name if abbreviation found, otherwise original value
    """
    if not state:
        return state
    
    state_upper = state.upper().strip()
    
    # Check Australian states first (avoid WA conflict with Washington)
    if state_upper in AU_STATES:
        return AU_STATES[state_upper]
    
    # Check US states
    if state_upper in US_STATES:
        return US_STATES[state_upper]
    
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
