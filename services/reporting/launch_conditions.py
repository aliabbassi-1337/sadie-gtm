"""Launch conditions - Centralized criteria for determining if a hotel is launchable.

This module defines all the conditions that must be met for a hotel to be launched.
These conditions are used by:
- get_launchable_hotels query
- get_launchable_count query  
- launch_ready_hotels query

LAUNCH CRITERIA (all required):
- status = 0 (pending)
- valid name (not junk/test/system names)
- country (required; state is optional)
- booking engine detected (hbe.status = 1)
- enrichment completed (hbe.enrichment_status = 1)

NOT REQUIRED (optional enrichment):
- state
- email
- phone
- room_count
- customer proximity
"""

from typing import List, Optional, Set


# =============================================================================
# JUNK NAME FILTERS
# =============================================================================

# Exact matches that are never valid hotel names
JUNK_NAMES_EXACT: Set[str] = {
    "&#65279;",  # BOM character
    "-",
    "--",
    "---",
    ".",
    "..",
    "Error",
    "Online Bookings",
    "Search",
    "Book Now",
    "Booking Engine",
    "Hotel Booking Engine",
    "Reservation",
    "Reservations",
    "View or Change a Reservation",
    "My reservations",
    "Modify/Cancel reservation",
    "Book Now Pay on Check-in",
    "DEACTIVATED ACCOUNT DO NO BOOK",
    "Rates",
    "Hotel",
}

# Pattern matches (case-insensitive ILIKE patterns)
JUNK_NAME_PATTERNS_ILIKE: List[str] = [
    "%test%",
    "%demo%",
    "%sandbox%",
    "%sample%",
    "unknown%",
    "%internal%server%error%",
    "%check availability%",
    "%booking engine%",
]

# Pattern matches (case-sensitive LIKE patterns)
JUNK_NAME_PATTERNS_LIKE: List[str] = [
    "% RMS %",
    "RMS %",
    "% RMS",
]

# Regex patterns
JUNK_NAME_REGEX: List[str] = [
    r"^[0-9-]+$",  # Names that are only numbers and dashes
]

# Minimum name length
MIN_NAME_LENGTH = 3


# =============================================================================
# SQL GENERATION
# =============================================================================

def get_name_validation_sql(table_alias: str = "h") -> str:
    """Generate SQL WHERE clause for name validation.
    
    Args:
        table_alias: The alias used for the hotels table (default "h")
        
    Returns:
        SQL string with all name validation conditions
    """
    conditions = []
    
    # Basic validity
    conditions.append(f"{table_alias}.name IS NOT NULL")
    conditions.append(f"{table_alias}.name != ''")
    conditions.append(f"{table_alias}.name != ' '")
    conditions.append(f"LENGTH({table_alias}.name) > {MIN_NAME_LENGTH - 1}")
    
    # Exact match exclusions
    exact_list = ", ".join(f"'{name}'" for name in sorted(JUNK_NAMES_EXACT))
    conditions.append(f"{table_alias}.name NOT IN ({exact_list})")
    
    # ILIKE pattern exclusions
    for pattern in JUNK_NAME_PATTERNS_ILIKE:
        conditions.append(f"{table_alias}.name NOT ILIKE '{pattern}'")
    
    # LIKE pattern exclusions
    for pattern in JUNK_NAME_PATTERNS_LIKE:
        conditions.append(f"{table_alias}.name NOT LIKE '{pattern}'")
    
    # Regex exclusions
    for pattern in JUNK_NAME_REGEX:
        conditions.append(f"{table_alias}.name !~ '{pattern}'")
    
    return "\n  AND ".join(conditions)


def get_location_validation_sql(table_alias: str = "h") -> str:
    """Generate SQL WHERE clause for location validation.
    
    Args:
        table_alias: The alias used for the hotels table (default "h")
        
    Returns:
        SQL string with location validation conditions
    """
    return f"""{table_alias}.country IS NOT NULL AND {table_alias}.country != ''"""


def get_launchable_where_clause(table_alias: str = "h") -> str:
    """Generate complete WHERE clause for launchable hotels.
    
    Args:
        table_alias: The alias used for the hotels table (default "h")
        
    Returns:
        Complete SQL WHERE clause (without the WHERE keyword)
    """
    parts = [
        f"{table_alias}.status = 0",
        f"-- Location requirements\n  {get_location_validation_sql(table_alias)}",
        f"-- Valid name requirements\n  {get_name_validation_sql(table_alias)}",
    ]
    return "\n  AND ".join(parts)


# =============================================================================
# PYTHON VALIDATION
# =============================================================================

def is_valid_name(name: Optional[str]) -> bool:
    """Check if a hotel name is valid for launching.
    
    Args:
        name: The hotel name to validate
        
    Returns:
        True if the name is valid, False otherwise
    """
    if not name or name.strip() == "":
        return False
    
    if len(name) < MIN_NAME_LENGTH:
        return False
    
    # Exact match check
    if name in JUNK_NAMES_EXACT:
        return False
    
    # Case-insensitive pattern check
    name_lower = name.lower()
    for pattern in JUNK_NAME_PATTERNS_ILIKE:
        # Convert SQL LIKE pattern to simple check
        pattern_clean = pattern.replace("%", "").lower()
        if pattern.startswith("%") and pattern.endswith("%"):
            if pattern_clean in name_lower:
                return False
        elif pattern.startswith("%"):
            if name_lower.endswith(pattern_clean):
                return False
        elif pattern.endswith("%"):
            if name_lower.startswith(pattern_clean):
                return False
    
    # Case-sensitive pattern check
    for pattern in JUNK_NAME_PATTERNS_LIKE:
        pattern_clean = pattern.replace("%", "")
        if pattern.startswith("%") and pattern.endswith("%"):
            if pattern_clean in name:
                return False
        elif pattern.startswith("%"):
            if name.endswith(pattern_clean):
                return False
        elif pattern.endswith("%"):
            if name.startswith(pattern_clean):
                return False
    
    # Regex check
    import re
    for pattern in JUNK_NAME_REGEX:
        if re.match(pattern, name):
            return False
    
    return True


def is_valid_location(state: Optional[str], country: Optional[str]) -> bool:
    """Check if a hotel has valid location for launching.
    
    Args:
        state: The state/region (optional, not required)
        country: The country (required)
        
    Returns:
        True if location is valid, False otherwise
    """
    if not country or country.strip() == "":
        return False
    return True


def is_launchable(
    status: int,
    name: Optional[str],
    state: Optional[str],
    country: Optional[str],
    has_booking_engine: bool,
) -> bool:
    """Check if a hotel meets all launch criteria.
    
    Args:
        status: Hotel status (must be 0 for pending)
        name: Hotel name
        state: State/region
        country: Country
        has_booking_engine: Whether hotel has active booking engine (hbe.status = 1)
        
    Returns:
        True if hotel is launchable, False otherwise
    """
    if status != 0:
        return False
    
    if not is_valid_name(name):
        return False
    
    if not is_valid_location(state, country):
        return False
    
    if not has_booking_engine:
        return False
    
    return True


# =============================================================================
# DEBUG / INSPECTION
# =============================================================================

def get_rejection_reason(
    status: int,
    name: Optional[str],
    state: Optional[str],
    country: Optional[str],
    has_booking_engine: bool,
) -> Optional[str]:
    """Get the reason why a hotel cannot be launched.
    
    Args:
        status: Hotel status
        name: Hotel name
        state: State/region
        country: Country
        has_booking_engine: Whether hotel has active booking engine
        
    Returns:
        Rejection reason string, or None if launchable
    """
    if status != 0:
        return f"status={status} (must be 0)"
    
    if not name or name.strip() == "":
        return "name is empty"
    
    if len(name) < MIN_NAME_LENGTH:
        return f"name too short ({len(name)} < {MIN_NAME_LENGTH})"
    
    if name in JUNK_NAMES_EXACT:
        return f"name is junk exact match: '{name}'"
    
    name_lower = name.lower()
    for pattern in JUNK_NAME_PATTERNS_ILIKE:
        pattern_clean = pattern.replace("%", "").lower()
        if pattern.startswith("%") and pattern.endswith("%"):
            if pattern_clean in name_lower:
                return f"name matches junk pattern: '{pattern}'"
        elif pattern.startswith("%"):
            if name_lower.endswith(pattern_clean):
                return f"name matches junk pattern: '{pattern}'"
        elif pattern.endswith("%"):
            if name_lower.startswith(pattern_clean):
                return f"name matches junk pattern: '{pattern}'"
    
    for pattern in JUNK_NAME_PATTERNS_LIKE:
        pattern_clean = pattern.replace("%", "")
        if pattern.startswith("%") and pattern.endswith("%"):
            if pattern_clean in name:
                return f"name matches junk pattern: '{pattern}'"
        elif pattern.startswith("%"):
            if name.endswith(pattern_clean):
                return f"name matches junk pattern: '{pattern}'"
        elif pattern.endswith("%"):
            if name.startswith(pattern_clean):
                return f"name matches junk pattern: '{pattern}'"
    
    import re
    for pattern in JUNK_NAME_REGEX:
        if re.match(pattern, name):
            return f"name matches junk regex: '{pattern}'"
    
    if not country or country.strip() == "":
        return "country is empty"
    
    if not has_booking_engine:
        return "no active booking engine (hbe.status != 1)"
    
    return None  # Launchable
