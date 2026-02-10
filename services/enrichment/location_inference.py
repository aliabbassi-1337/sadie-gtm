"""Location inference from hotel data signals.

Infers country and state from:
1. Website TLD (.com.au -> Australia, .ae -> UAE, etc.)
2. Phone prefix (+61 -> Australia, +971 -> UAE, etc.)
3. Address text patterns (postcodes, state abbreviations in address)

This is the SINGLE SOURCE OF TRUTH for location inference logic.
"""

import re
from typing import Optional, Tuple
from urllib.parse import urlparse

from services.enrichment.state_utils import AU_STATES, CA_PROVINCES, US_STATES, UK_COUNTRIES


# ============================================================================
# TLD -> Country mapping
# ============================================================================

# Country-code TLDs (ccTLDs) to country name
# Multi-part TLDs checked first (e.g. .com.au before .au)
TLD_TO_COUNTRY = {
    # Australia
    ".com.au": "Australia",
    ".net.au": "Australia",
    ".org.au": "Australia",
    ".edu.au": "Australia",
    ".gov.au": "Australia",
    ".au": "Australia",
    # New Zealand
    ".co.nz": "New Zealand",
    ".net.nz": "New Zealand",
    ".org.nz": "New Zealand",
    ".nz": "New Zealand",
    # United Kingdom
    ".co.uk": "United Kingdom",
    ".org.uk": "United Kingdom",
    ".gov.uk": "United Kingdom",
    ".uk": "United Kingdom",
    # India
    ".co.in": "India",
    ".net.in": "India",
    ".org.in": "India",
    ".in": "India",
    # UAE
    ".ae": "United Arab Emirates",
    # South Africa
    ".co.za": "South Africa",
    ".za": "South Africa",
    # Mexico
    ".com.mx": "Mexico",
    ".mx": "Mexico",
    # Brazil
    ".com.br": "Brazil",
    ".br": "Brazil",
    # Argentina
    ".com.ar": "Argentina",
    ".ar": "Argentina",
    # Japan
    ".co.jp": "Japan",
    ".jp": "Japan",
    # South Korea
    ".co.kr": "South Korea",
    ".kr": "South Korea",
    # China
    ".com.cn": "China",
    ".cn": "China",
    # Thailand
    ".co.th": "Thailand",
    ".th": "Thailand",
    # Vietnam
    ".com.vn": "Vietnam",
    ".vn": "Vietnam",
    # Malaysia
    ".com.my": "Malaysia",
    ".my": "Malaysia",
    # Philippines
    ".com.ph": "Philippines",
    ".ph": "Philippines",
    # Singapore
    ".com.sg": "Singapore",
    ".sg": "Singapore",
    # Indonesia
    ".co.id": "Indonesia",
    ".id": "Indonesia",
    # Canada
    ".ca": "Canada",
    # Germany
    ".de": "Germany",
    # France
    ".fr": "France",
    # Spain
    ".es": "Spain",
    # Italy
    ".it": "Italy",
    # Portugal
    ".pt": "Portugal",
    # Netherlands
    ".nl": "Netherlands",
    # Belgium
    ".be": "Belgium",
    # Switzerland
    ".ch": "Switzerland",
    # Austria
    ".at": "Austria",
    # Sweden
    ".se": "Sweden",
    # Norway
    ".no": "Norway",
    # Denmark
    ".dk": "Denmark",
    # Finland
    ".fi": "Finland",
    # Ireland
    ".ie": "Ireland",
    # Greece
    ".gr": "Greece",
    # Turkey
    ".tr": "Turkey",
    # Poland
    ".pl": "Poland",
    # Czech Republic
    ".cz": "Czech Republic",
    # Hungary
    ".hu": "Hungary",
    # Romania
    ".ro": "Romania",
    # Croatia
    ".hr": "Croatia",
    # Russia
    ".ru": "Russia",
    # Sri Lanka
    ".lk": "Sri Lanka",
    # Cambodia
    ".kh": "Cambodia",
    # Fiji
    ".fj": "Fiji",
    # Colombia (.co is excluded — widely used as generic TLD by US startups)
    ".com.co": "Colombia",
    # Peru
    ".pe": "Peru",
    # Chile
    ".cl": "Chile",
    # Costa Rica
    ".cr": "Costa Rica",
    # Israel
    ".il": "Israel",
    # Egypt
    ".eg": "Egypt",
    # Morocco
    ".ma": "Morocco",
    # Kenya
    ".ke": "Kenya",
    # Tanzania
    ".tz": "Tanzania",
    # Iceland
    ".is": "Iceland",
}

# Sort by length descending so multi-part TLDs match first
_TLD_KEYS_SORTED = sorted(TLD_TO_COUNTRY.keys(), key=len, reverse=True)

# Aggregator/booking site domains whose TLD does NOT indicate the hotel's country.
# e.g., m.yelp.ca serves US hotels, tripadvisor.com.au lists global hotels.
AGGREGATOR_DOMAINS = {
    "yelp.ca", "m.yelp.ca",
    "yelp.co.uk", "m.yelp.co.uk",
    "yelp.com.au", "m.yelp.com.au",
    "yelp.de", "m.yelp.de",
    "yelp.fr", "m.yelp.fr",
    "yelp.it", "m.yelp.it",
    "yelp.es", "m.yelp.es",
    "vacasa.ca",
    "tripadvisor.com.au",
    "tripadvisor.co.uk",
    "tripadvisor.co.nz",
    "tripadvisor.co.in",
    "tripadvisor.ca",
    "tripadvisor.de",
    "tripadvisor.fr",
    "tripadvisor.es",
    "tripadvisor.it",
    "stayz.com.au",
    "booking.com",
    "expedia.com.au",
    "expedia.ca",
    "expedia.co.uk",
    "expedia.co.in",
    "hotel.com.au",
    "hotels.com",
    "agoda.com",
    "airbnb.com.au",
    "airbnb.ca",
    "airbnb.co.uk",
    "airbnb.co.nz",
    "airbnb.co.in",
    "airbnb.de",
    "airbnb.fr",
    "airbnb.es",
    "airbnb.it",
    "rentbyowner.com.au",
    "wotif.com",
    "wotif.com.au",
    "lastminute.com.au",
    "trivago.com.au",
    "trivago.ca",
    "trivago.co.uk",
    "hostelworld.com",
}


# ============================================================================
# Phone prefix -> Country mapping
# ============================================================================

PHONE_PREFIX_TO_COUNTRY = {
    "+61": "Australia",
    "+64": "New Zealand",
    "+44": "United Kingdom",
    "+91": "India",
    "+971": "United Arab Emirates",
    "+27": "South Africa",
    "+52": "Mexico",
    "+55": "Brazil",
    "+54": "Argentina",
    "+81": "Japan",
    "+82": "South Korea",
    "+86": "China",
    "+66": "Thailand",
    "+84": "Vietnam",
    "+60": "Malaysia",
    "+63": "Philippines",
    "+65": "Singapore",
    "+62": "Indonesia",
    "+49": "Germany",
    "+33": "France",
    "+34": "Spain",
    "+39": "Italy",
    "+351": "Portugal",
    "+31": "Netherlands",
    "+32": "Belgium",
    "+41": "Switzerland",
    "+43": "Austria",
    "+46": "Sweden",
    "+47": "Norway",
    "+45": "Denmark",
    "+358": "Finland",
    "+353": "Ireland",
    "+30": "Greece",
    "+90": "Turkey",
    "+48": "Poland",
    "+420": "Czech Republic",
    "+36": "Hungary",
    "+40": "Romania",
    "+385": "Croatia",
    "+7": "Russia",
    "+94": "Sri Lanka",
    "+855": "Cambodia",
    "+679": "Fiji",
    "+57": "Colombia",
    "+51": "Peru",
    "+56": "Chile",
    "+506": "Costa Rica",
    "+972": "Israel",
    "+20": "Egypt",
    "+212": "Morocco",
    "+254": "Kenya",
    "+255": "Tanzania",
    "+354": "Iceland",
    "+966": "Saudi Arabia",
    "+965": "Kuwait",
    "+974": "Qatar",
    "+973": "Bahrain",
    "+968": "Oman",
    "+962": "Jordan",
    "+961": "Lebanon",
    "+230": "Mauritius",
    "+297": "Aruba",
    "+260": "Zambia",
    "+256": "Uganda",
    "+233": "Ghana",
    "+234": "Nigeria",
    "+682": "Cook Islands",
    "+672": "Australia",  # Norfolk Island (Australian territory)
    "+675": "Papua New Guinea",
    "+676": "Tonga",
    "+677": "Solomon Islands",
    "+678": "Vanuatu",
    "+92": "Pakistan",
    "+853": "Macau",
    "+599": "Curacao",
    "+880": "Bangladesh",
    "+977": "Nepal",
    "+95": "Myanmar",
    "+856": "Laos",
    "+689": "French Polynesia",
    "+687": "New Caledonia",
    "+1": "US/CA",  # ambiguous: US or Canada
}

# Sort by length descending so longer prefixes match first (+971 before +9)
_PHONE_PREFIX_SORTED = sorted(PHONE_PREFIX_TO_COUNTRY.keys(), key=len, reverse=True)


# ============================================================================
# Australian phone patterns (without + prefix)
# Australian numbers often appear as 02/03/04/07/08 XXXX XXXX
# ============================================================================

_AU_PHONE_PATTERN = re.compile(
    r'^(?:\(0[2-8]\)|0[2-8])[\s\-]?\d{4}[\s\-]?\d{4}$'
)

# Australian mobile: 04XX XXX XXX
_AU_MOBILE_PATTERN = re.compile(
    r'^04\d{2}[\s\-]?\d{3}[\s\-]?\d{3}$'
)


# ============================================================================
# Address patterns for country inference
# ============================================================================

# Australian postcodes: 4-digit, ranges by state
# NSW: 2000-2999, VIC: 3000-3999, QLD: 4000-4999, SA: 5000-5999
# WA: 6000-6999, TAS: 7000-7999, NT: 0800-0899
_AU_POSTCODE_STATE = {
    range(2000, 3000): "New South Wales",
    range(3000, 4000): "Victoria",
    range(4000, 5000): "Queensland",
    range(5000, 6000): "South Australia",
    range(6000, 7000): "Western Australia",
    range(7000, 8000): "Tasmania",
    range(800, 900): "Northern Territory",
    range(2600, 2620): "Australian Capital Territory",
    range(900, 1000): "Australian Capital Territory",
}

# Match ", <AU_STATE_ABBREV> <4-digit postcode>" pattern in address
_AU_ADDRESS_PATTERN = re.compile(
    r'(?:,\s*|\s+)(?:' + '|'.join(AU_STATES.keys()) + r')\s+(\d{4})\b',
    re.IGNORECASE
)

# Match "Australia" literally in address
_AUSTRALIA_IN_ADDRESS = re.compile(r'\bAustralia\b', re.IGNORECASE)

# Match ", India" or "India" in address
_INDIA_IN_ADDRESS = re.compile(r'\b(?:India|भारत)\b', re.IGNORECASE)

# Match country names and common abbreviations embedded in address
# Includes full names, abbreviations (UAE, UK, NZ), and RMS artifacts (UnitedStates, etc.)
_COUNTRY_IN_ADDRESS = {
    # UAE — common abbreviation in RMS addresses
    re.compile(r'\bUAE\b'): "United Arab Emirates",
    re.compile(r'\bUnited\s+Arab\s+Emirates\b', re.IGNORECASE): "United Arab Emirates",
    # UK
    re.compile(r',\s*UK\b'): "United Kingdom",
    re.compile(r'\bUnited\s*Kingdom\b', re.IGNORECASE): "United Kingdom",
    re.compile(r'\bEngland\b', re.IGNORECASE): "United Kingdom",
    re.compile(r'\bScotland\b', re.IGNORECASE): "United Kingdom",
    re.compile(r'\bWales\b', re.IGNORECASE): "United Kingdom",
    # Oceania
    re.compile(r'\bAustralia\b', re.IGNORECASE): "Australia",
    re.compile(r'\bNew\s+Zealand\b', re.IGNORECASE): "New Zealand",
    re.compile(r',\s*NZ\b'): "New Zealand",
    re.compile(r'\bFiji\b', re.IGNORECASE): "Fiji",
    # Asia
    re.compile(r'\bIndia\b', re.IGNORECASE): "India",
    re.compile(r'\bPhilippines\b', re.IGNORECASE): "Philippines",
    re.compile(r'\bThailand\b', re.IGNORECASE): "Thailand",
    re.compile(r'\bViet\s*Nam\b', re.IGNORECASE): "Vietnam",
    re.compile(r'\bVietnam\b', re.IGNORECASE): "Vietnam",
    re.compile(r'\bMalaysia\b', re.IGNORECASE): "Malaysia",
    re.compile(r'\bIndonesia\b', re.IGNORECASE): "Indonesia",
    re.compile(r'\bSingapore\b', re.IGNORECASE): "Singapore",
    re.compile(r'\bJapan\b', re.IGNORECASE): "Japan",
    re.compile(r'\bSouth\s+Korea\b', re.IGNORECASE): "South Korea",
    re.compile(r'\bSri\s+Lanka\b', re.IGNORECASE): "Sri Lanka",
    re.compile(r'\bCambodia\b', re.IGNORECASE): "Cambodia",
    re.compile(r'\bMyanmar\b', re.IGNORECASE): "Myanmar",
    re.compile(r'\bLaos\b', re.IGNORECASE): "Laos",
    re.compile(r'\bNepal\b', re.IGNORECASE): "Nepal",
    re.compile(r'\bBangladesh\b', re.IGNORECASE): "Bangladesh",
    re.compile(r'\bMaldives\b', re.IGNORECASE): "Maldives",
    re.compile(r'\bChina\b', re.IGNORECASE): "China",
    re.compile(r'\bTaiwan\b', re.IGNORECASE): "Taiwan",
    re.compile(r'\bHong\s+Kong\b', re.IGNORECASE): "Hong Kong",
    # Europe
    re.compile(r'\bNorway\b', re.IGNORECASE): "Norway",
    re.compile(r'\bGermany\b', re.IGNORECASE): "Germany",
    re.compile(r'\bFrance\b', re.IGNORECASE): "France",
    re.compile(r'\bSpain\b', re.IGNORECASE): "Spain",
    re.compile(r'\bItaly\b', re.IGNORECASE): "Italy",
    re.compile(r'\bPortugal\b', re.IGNORECASE): "Portugal",
    re.compile(r'\bSwitzerland\b', re.IGNORECASE): "Switzerland",
    re.compile(r'\bAustria\b', re.IGNORECASE): "Austria",
    re.compile(r'\bSweden\b', re.IGNORECASE): "Sweden",
    re.compile(r'\bDenmark\b', re.IGNORECASE): "Denmark",
    re.compile(r'\bFinland\b', re.IGNORECASE): "Finland",
    re.compile(r'\bIreland\b', re.IGNORECASE): "Ireland",
    re.compile(r'\bGreece\b', re.IGNORECASE): "Greece",
    re.compile(r'\bTurkey\b', re.IGNORECASE): "Turkey",
    re.compile(r'\bNetherlands\b', re.IGNORECASE): "Netherlands",
    re.compile(r'\bBelgium\b', re.IGNORECASE): "Belgium",
    re.compile(r'\bCroatia\b', re.IGNORECASE): "Croatia",
    re.compile(r'\bCzech\s+Republic\b', re.IGNORECASE): "Czech Republic",
    re.compile(r'\bPoland\b', re.IGNORECASE): "Poland",
    re.compile(r'\bIceland\b', re.IGNORECASE): "Iceland",
    # Americas
    re.compile(r'\bMexico\b', re.IGNORECASE): "Mexico",
    re.compile(r'\bBrazil\b', re.IGNORECASE): "Brazil",
    re.compile(r'\bColombia\b', re.IGNORECASE): "Colombia",
    re.compile(r'\bCosta\s+Rica\b', re.IGNORECASE): "Costa Rica",
    re.compile(r'\bCanada\b', re.IGNORECASE): "Canada",
    re.compile(r'\bPeru\b', re.IGNORECASE): "Peru",
    re.compile(r'\bChile\b', re.IGNORECASE): "Chile",
    re.compile(r'\bArgentina\b', re.IGNORECASE): "Argentina",
    # Africa / Middle East
    re.compile(r'\bSouth\s+Africa\b', re.IGNORECASE): "South Africa",
    re.compile(r'\bKenya\b', re.IGNORECASE): "Kenya",
    re.compile(r'\bTanzania\b', re.IGNORECASE): "Tanzania",
    re.compile(r'\bMorocco\b', re.IGNORECASE): "Morocco",
    re.compile(r'\bEgypt\b', re.IGNORECASE): "Egypt",
    re.compile(r'\bIsrael\b', re.IGNORECASE): "Israel",
    re.compile(r'\bJordan\b', re.IGNORECASE): "Jordan",
    re.compile(r'\bOman\b', re.IGNORECASE): "Oman",
    re.compile(r'\bQatar\b', re.IGNORECASE): "Qatar",
    re.compile(r'\bSaudi\s+Arabia\b', re.IGNORECASE): "Saudi Arabia",
    re.compile(r'\bBahrain\b', re.IGNORECASE): "Bahrain",
    re.compile(r'\bKuwait\b', re.IGNORECASE): "Kuwait",
    re.compile(r'\bGhana\b', re.IGNORECASE): "Ghana",
    re.compile(r'\bNigeria\b', re.IGNORECASE): "Nigeria",
    re.compile(r'\bUganda\b', re.IGNORECASE): "Uganda",
    re.compile(r'\bZambia\b', re.IGNORECASE): "Zambia",
    re.compile(r'\bMauritius\b', re.IGNORECASE): "Mauritius",
    # Lebanon — only match as country (end of address), NOT as US city (Lebanon, PA)
    re.compile(r',\s*Lebanon\s*$', re.IGNORECASE): "Lebanon",
    re.compile(r'\bDubai\b', re.IGNORECASE): "United Arab Emirates",
    re.compile(r'\bAbu\s+Dhabi\b', re.IGNORECASE): "United Arab Emirates",
    re.compile(r'\bSharjah\b', re.IGNORECASE): "United Arab Emirates",
    re.compile(r'\bPakistan\b', re.IGNORECASE): "Pakistan",
    re.compile(r'\bPapua\s+New\s+Guinea\b', re.IGNORECASE): "Papua New Guinea",
    # RMS artifacts: "UnitedStates", "United_States", "Saudi_Arabia", etc.
    re.compile(r'\bUnitedStates\b', re.IGNORECASE): "United States",
    re.compile(r'\bUnited_States\b', re.IGNORECASE): "United States",
    re.compile(r'\bUnitedKingdom\b', re.IGNORECASE): "United Kingdom",
    re.compile(r'\bUnited_Kingdom\b', re.IGNORECASE): "United Kingdom",
    re.compile(r'\bSaudi_Arabia\b', re.IGNORECASE): "Saudi Arabia",
    re.compile(r'\bKorea_South\b', re.IGNORECASE): "South Korea",
    re.compile(r'\bSouth_Africa\b', re.IGNORECASE): "South Africa",
    re.compile(r'\bNew_Zealand\b', re.IGNORECASE): "New Zealand",
    re.compile(r'\bSri_Lanka\b', re.IGNORECASE): "Sri Lanka",
    re.compile(r'\bCosta_Rica\b', re.IGNORECASE): "Costa Rica",
    re.compile(r'\bUnited_Arab_Emirates\b', re.IGNORECASE): "United Arab Emirates",
    re.compile(r'\bCzech_Republic\b', re.IGNORECASE): "Czech Republic",
    re.compile(r'\bPapua_New_Guinea\b', re.IGNORECASE): "Papua New Guinea",
    re.compile(r'\bMacau\b', re.IGNORECASE): "Macau",
}

# "NotSet" in address is a common RMS artifact
_NOTSET_PATTERN = re.compile(r',\s*NotSet\b', re.IGNORECASE)


def infer_country_from_tld(website: Optional[str]) -> Optional[str]:
    """Infer country from website domain TLD.

    Args:
        website: Hotel website URL

    Returns:
        Country name or None if no match / .com / .org
    """
    if not website:
        return None

    website = website.strip().lower()

    # Remove protocol and path
    if "://" in website:
        try:
            parsed = urlparse(website)
            domain = parsed.netloc or parsed.path.split("/")[0]
        except Exception:
            domain = website.split("://", 1)[-1].split("/")[0]
    else:
        domain = website.split("/")[0]

    # Remove port
    domain = domain.split(":")[0]

    if not domain:
        return None

    # Skip aggregator/booking domains — their TLD doesn't indicate hotel country
    for agg in AGGREGATOR_DOMAINS:
        if domain == agg or domain.endswith("." + agg):
            return None

    # Check against known TLDs (longest first)
    for tld in _TLD_KEYS_SORTED:
        if domain.endswith(tld):
            return TLD_TO_COUNTRY[tld]

    return None


def infer_country_from_phone(phone: Optional[str]) -> Optional[str]:
    """Infer country from phone number prefix.

    Args:
        phone: Phone number string

    Returns:
        Country name or None. Returns "US/CA" for +1 (ambiguous).
    """
    if not phone:
        return None

    phone = phone.strip()

    # Check international prefixes
    if phone.startswith("+"):
        for prefix in _PHONE_PREFIX_SORTED:
            if phone.startswith(prefix):
                return PHONE_PREFIX_TO_COUNTRY[prefix]

    # Check Australian local number patterns (no + prefix)
    if _AU_PHONE_PATTERN.match(phone) or _AU_MOBILE_PATTERN.match(phone):
        return "Australia"

    return None


def infer_country_from_address(address: Optional[str]) -> Optional[str]:
    """Infer country from address text patterns.

    Checks for:
    - Explicit country names in address
    - Australian postcode patterns (AU_STATE XXXX)
    - "NotSet" artifacts (RMS sets country to "UnitedStates" with "NotSet" marker)

    Args:
        address: Hotel address string

    Returns:
        Country name or None
    """
    if not address:
        return None

    # Check for explicit country names in address
    for pattern, country in _COUNTRY_IN_ADDRESS.items():
        if pattern.search(address):
            return country

    # Check for Australian address patterns: "WA 6537", "NSW 2000", etc.
    match = _AU_ADDRESS_PATTERN.search(address)
    if match:
        postcode = int(match.group(1))
        for pc_range in _AU_POSTCODE_STATE:
            if postcode in pc_range:
                return "Australia"

    return None


def infer_au_state_from_address(address: Optional[str]) -> Optional[str]:
    """Infer Australian state from address using state abbreviation and postcode.

    Args:
        address: Hotel address string

    Returns:
        Full Australian state name or None
    """
    if not address:
        return None

    match = _AU_ADDRESS_PATTERN.search(address)
    if match:
        postcode = int(match.group(1))
        for pc_range, state_name in _AU_POSTCODE_STATE.items():
            if postcode in pc_range:
                return state_name

    return None


def infer_state_from_au_state_in_address(address: Optional[str]) -> Optional[str]:
    """Extract Australian state abbreviation from address and return full name.

    Looks for patterns like 'WA 6537', 'NSW 2000', 'VIC 3227' in address.
    """
    if not address:
        return None

    for abbrev, full_name in AU_STATES.items():
        pattern = re.compile(r'\b' + abbrev + r'\s+\d{4}\b', re.IGNORECASE)
        if pattern.search(address):
            return full_name

    return None


def infer_location(
    website: Optional[str] = None,
    phone_google: Optional[str] = None,
    phone_website: Optional[str] = None,
    address: Optional[str] = None,
    current_country: Optional[str] = None,
    current_state: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], float]:
    """Infer country and state from all available signals.

    Priority order for country:
    1. Address (explicit country name — highest confidence)
    2. TLD (very reliable for ccTLDs)
    3. Phone prefix (reliable but +1 is ambiguous)

    Returns:
        (inferred_country, inferred_state, confidence)
        confidence: 0.0-1.0 score
    """
    signals = []

    # Collect country signals
    phone = phone_google or phone_website
    tld_country = infer_country_from_tld(website)
    phone_country = infer_country_from_phone(phone)
    addr_country = infer_country_from_address(address)

    if addr_country:
        signals.append(("address", addr_country, 0.9))
    if tld_country:
        signals.append(("tld", tld_country, 0.85))
    if phone_country and phone_country != "US/CA":
        signals.append(("phone", phone_country, 0.8))

    if not signals:
        return None, None, 0.0

    # Find consensus country
    country_votes = {}
    for source, country, weight in signals:
        country_votes[country] = country_votes.get(country, 0) + weight

    best_country = max(country_votes, key=country_votes.get)
    confidence = country_votes[best_country] / sum(country_votes.values())

    # Infer state if country is Australia
    inferred_state = None
    if best_country == "Australia" and address:
        inferred_state = infer_state_from_au_state_in_address(address)
        if not inferred_state:
            inferred_state = infer_au_state_from_address(address)

    return best_country, inferred_state, confidence


# ============================================================================
# UK postcode area -> county mapping
# ============================================================================

# UK postcode format: A9 9AA, A99 9AA, A9A 9AA, AA9 9AA, AA99 9AA, AA9A 9AA
_UK_POSTCODE = re.compile(
    r'\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b', re.IGNORECASE
)

UK_POSTCODE_TO_COUNTY = {
    # England
    'B': 'West Midlands', 'BA': 'Somerset', 'BB': 'Lancashire',
    'BD': 'West Yorkshire', 'BH': 'Dorset', 'BL': 'Greater Manchester',
    'BN': 'East Sussex', 'BR': 'Greater London', 'BS': 'Somerset',
    'CA': 'Cumbria', 'CB': 'Cambridgeshire', 'CH': 'Cheshire',
    'CM': 'Essex', 'CO': 'Essex', 'CR': 'Greater London',
    'CT': 'Kent', 'CV': 'Warwickshire', 'CW': 'Cheshire',
    'DA': 'Kent', 'DE': 'Derbyshire', 'DH': 'Durham',
    'DL': 'North Yorkshire', 'DN': 'South Yorkshire', 'DT': 'Dorset',
    'DY': 'West Midlands', 'E': 'Greater London', 'EC': 'Greater London',
    'EN': 'Hertfordshire', 'EX': 'Devon', 'FY': 'Lancashire',
    'GL': 'Gloucestershire', 'GU': 'Surrey', 'HA': 'Greater London',
    'HD': 'West Yorkshire', 'HG': 'North Yorkshire', 'HP': 'Buckinghamshire',
    'HR': 'Herefordshire', 'HU': 'East Yorkshire', 'HX': 'West Yorkshire',
    'IG': 'Greater London', 'IP': 'Suffolk', 'KT': 'Surrey',
    'L': 'Merseyside', 'LA': 'Lancashire', 'LE': 'Leicestershire',
    'LN': 'Lincolnshire', 'LS': 'West Yorkshire', 'LU': 'Bedfordshire',
    'M': 'Greater Manchester', 'ME': 'Kent', 'MK': 'Buckinghamshire',
    'N': 'Greater London', 'NE': 'Northumberland', 'NG': 'Nottinghamshire',
    'NN': 'Northamptonshire', 'NR': 'Norfolk', 'NW': 'Greater London',
    'OL': 'Greater Manchester', 'OX': 'Oxfordshire', 'PE': 'Cambridgeshire',
    'PL': 'Devon', 'PO': 'Hampshire', 'PR': 'Lancashire',
    'RG': 'Berkshire', 'RH': 'Surrey', 'RM': 'Greater London',
    'S': 'South Yorkshire', 'SE': 'Greater London', 'SG': 'Hertfordshire',
    'SK': 'Cheshire', 'SL': 'Berkshire', 'SM': 'Greater London',
    'SN': 'Wiltshire', 'SO': 'Hampshire', 'SP': 'Wiltshire',
    'SR': 'Tyne and Wear', 'SS': 'Essex', 'ST': 'Staffordshire',
    'SW': 'Greater London', 'SY': 'Shropshire', 'TA': 'Somerset',
    'TF': 'Shropshire', 'TN': 'Kent', 'TQ': 'Devon',
    'TR': 'Cornwall', 'TS': 'North Yorkshire', 'TW': 'Greater London',
    'UB': 'Greater London', 'W': 'Greater London', 'WA': 'Cheshire',
    'WC': 'Greater London', 'WD': 'Hertfordshire', 'WF': 'West Yorkshire',
    'WN': 'Greater Manchester', 'WR': 'Worcestershire', 'WS': 'Staffordshire',
    'WV': 'West Midlands', 'YO': 'North Yorkshire',
    # Bristol (technically its own county)
    'BS': 'Somerset',
    # Scotland
    'AB': 'Aberdeenshire', 'DD': 'Angus', 'DG': 'Dumfries and Galloway',
    'EH': 'Lothian', 'FK': 'Stirlingshire', 'G': 'Glasgow',
    'HS': 'Western Isles', 'IV': 'Highland', 'KA': 'Ayrshire',
    'KW': 'Highland', 'KY': 'Fife', 'ML': 'Lanarkshire',
    'PA': 'Argyll and Bute', 'PH': 'Perthshire', 'TD': 'Scottish Borders',
    'ZE': 'Shetland',
    # Wales
    'CF': 'South Glamorgan', 'LD': 'Powys', 'LL': 'Gwynedd',
    'NP': 'Gwent', 'SA': 'Carmarthenshire', 'SY': 'Shropshire',
    # Northern Ireland
    'BT': 'Northern Ireland',
    # Crown Dependencies
    'GY': 'Guernsey', 'JE': 'Jersey', 'IM': 'Isle of Man',
}

# Known UK counties for matching in address text (includes historic + current)
UK_COUNTIES = {
    # English counties
    'Bedfordshire', 'Berkshire', 'Bristol', 'Buckinghamshire', 'Cambridgeshire',
    'Cheshire', 'Cornwall', 'Cumbria', 'Derbyshire', 'Devon', 'Dorset',
    'Durham', 'East Sussex', 'East Yorkshire', 'Essex', 'Gloucestershire',
    'Greater London', 'Greater Manchester', 'Hampshire', 'Herefordshire',
    'Hertfordshire', 'Isle of Wight', 'Kent', 'Lancashire', 'Leicestershire',
    'Lincolnshire', 'Merseyside', 'Norfolk', 'North Yorkshire',
    'Northamptonshire', 'Northumberland', 'Nottinghamshire', 'Oxfordshire',
    'Rutland', 'Shropshire', 'Somerset', 'South Yorkshire', 'Staffordshire',
    'Suffolk', 'Surrey', 'Tyne and Wear', 'Warwickshire', 'West Midlands',
    'West Sussex', 'West Yorkshire', 'Wiltshire', 'Worcestershire',
    # Scottish regions
    'Aberdeenshire', 'Angus', 'Argyll and Bute', 'Ayrshire',
    'Dumfries and Galloway', 'Dumfries & Galloway',
    'Fife', 'Glasgow', 'Highland', 'Highlands',
    'Lanarkshire', 'Lothian', 'Moray', 'Perthshire',
    'Scottish Borders', 'Stirlingshire', 'Shetland', 'Western Isles',
    # Welsh counties
    'Carmarthenshire', 'Ceredigion', 'Conwy', 'Denbighshire', 'Flintshire',
    'Gwent', 'Gwynedd', 'Monmouthshire', 'Pembrokeshire', 'Powys',
    'South Glamorgan', 'Vale of Glamorgan', 'West Glamorgan',
    # Northern Ireland
    'Antrim', 'Armagh', 'Down', 'Fermanagh', 'Londonderry', 'Tyrone',
    'Northern Ireland',
    # Constituent countries (used as state too)
    'England', 'Scotland', 'Wales',
}

# Major UK cities for extraction from address
_UK_MAJOR_CITIES = {
    'London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 'Liverpool',
    'Edinburgh', 'Bristol', 'Sheffield', 'Newcastle', 'Nottingham',
    'Cardiff', 'Belfast', 'Leicester', 'Brighton', 'Plymouth', 'Southampton',
    'Portsmouth', 'Oxford', 'Cambridge', 'York', 'Bath', 'Canterbury',
    'Exeter', 'Chester', 'Durham', 'Aberdeen', 'Dundee', 'Inverness',
    'Swansea', 'Newport', 'Bournemouth', 'Reading', 'Norwich', 'Ipswich',
    'Derby', 'Coventry', 'Wolverhampton', 'Sunderland', 'Blackpool',
    'Harrogate', 'Scarborough', 'Whitby', 'Windermere', 'Keswick',
    'Torquay', 'Penzance', 'Stratford-upon-Avon', 'Stratford upon Avon',
}

# Build a regex that matches any UK county in text (longest first to match multi-word first)
_UK_COUNTY_PATTERN = re.compile(
    r'\b(' + '|'.join(
        re.escape(c) for c in sorted(UK_COUNTIES, key=len, reverse=True)
    ) + r')\b',
    re.IGNORECASE
)

# Strip country suffixes from RMS addresses
_COUNTRY_SUFFIX = re.compile(
    r',?\s*(?:United\s*Kingdom|UnitedKingdom|England|Scotland|Wales|UK)\s*$',
    re.IGNORECASE
)


def extract_uk_state_city_from_address(address: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract county (state) and city from a UK address.

    Strategy:
    1. Strip country from end of address
    2. Find UK postcode -> map area to county
    3. Look for explicit county name in text (overrides postcode)
    4. Try to extract city from address parts
    """
    if not address:
        return None, None

    # Strip country suffix
    cleaned = _COUNTRY_SUFFIX.sub('', address).strip().rstrip(',').strip()

    county = None
    city = None

    # 1. Try explicit county name in address
    county_match = _UK_COUNTY_PATTERN.search(cleaned)
    if county_match:
        # Normalize county name to title case from our set
        matched = county_match.group(1)
        for c in UK_COUNTIES:
            if c.lower() == matched.lower():
                county = c
                break

    # 2. Fallback: UK postcode area -> county
    postcode_match = _UK_POSTCODE.search(cleaned)
    if not county and postcode_match:
        area = postcode_match.group(1).upper()
        # Try 2-letter area first, then 1-letter
        county = UK_POSTCODE_TO_COUNTY.get(area[:2]) or UK_POSTCODE_TO_COUNTY.get(area[0])

    # 3. Extract city
    # Remove postcode from text for cleaner parsing
    text_for_city = cleaned
    if postcode_match:
        text_for_city = cleaned[:postcode_match.start()].strip().rstrip(',').strip()

    # Remove county name from text
    if county:
        text_for_city = re.sub(re.escape(county), '', text_for_city, flags=re.IGNORECASE).strip().rstrip(',').strip()

    # Check for major city names anywhere in original address
    for city_name in _UK_MAJOR_CITIES:
        if re.search(r'\b' + re.escape(city_name) + r'\b', cleaned, re.IGNORECASE):
            city = city_name
            break

    # If no major city found, take the last comma-separated part as city candidate
    if not city and text_for_city:
        parts = [p.strip() for p in text_for_city.split(',') if p.strip()]
        if parts:
            candidate = parts[-1].strip()
            # Skip if it looks like a street address (has numbers at start)
            if candidate and not re.match(r'^\d', candidate) and len(candidate) > 2:
                # Clean up common prefixes
                candidate = re.sub(r'^(?:Nr|Near)\s+', '', candidate, flags=re.IGNORECASE).strip()
                if candidate and len(candidate) > 2:
                    city = candidate

    # If county is Greater London, set city to London
    if county == 'Greater London' and not city:
        city = 'London'

    return county, city


def extract_au_city_from_address(address: str) -> Optional[str]:
    """Extract city/town from an Australian address.

    AU addresses: "<street>, <suburb/city>, <STATE> <postcode>"
    """
    if not address:
        return None

    # Strip country
    cleaned = re.sub(r',?\s*(?:Australia)\s*$', '', address, flags=re.IGNORECASE).strip()

    # Remove postcode and state abbreviation at end
    cleaned = re.sub(
        r',?\s*(?:' + '|'.join(AU_STATES.keys()) + r')\s+\d{4}\s*$',
        '', cleaned, flags=re.IGNORECASE
    ).strip().rstrip(',').strip()

    if not cleaned:
        return None

    # Take last comma-separated part as city
    parts = [p.strip() for p in cleaned.split(',') if p.strip()]
    if parts:
        candidate = parts[-1].strip()
        if candidate and not re.match(r'^\d', candidate) and len(candidate) > 2:
            return candidate

    return None


# ============================================================================
# US address state extraction
# ============================================================================

# Match ", <STATE_ABBREV> <ZIP>" pattern (e.g. "Seward, AK 99664" or "Seward AK 99664-1234")
_US_ADDR_STATE_ZIP = re.compile(r',?\s+([A-Z]{2})\s+\d{5}(?:-\d{4})?\b')

# Match ", <STATE_ABBREV>" at end of address (e.g. "Seward, AK")
_US_ADDR_STATE_END = re.compile(r',\s*([A-Z]{2})\s*$')


def extract_us_state_from_address(address: str) -> Optional[str]:
    """Extract US state from address using state abbreviation patterns.

    Looks for:
    - "City, ST 12345" or "City ST 12345" (state + zip)
    - "City, ST" at end of address (state at end)

    Validates extracted 2-letter code against US_STATES before returning.

    Returns:
        Full US state name or None
    """
    if not address:
        return None

    # Try state + zip pattern first (more specific)
    match = _US_ADDR_STATE_ZIP.search(address)
    if match:
        abbrev = match.group(1).upper()
        if abbrev in US_STATES:
            return US_STATES[abbrev]

    # Fallback: state abbreviation at end of address
    match = _US_ADDR_STATE_END.search(address)
    if match:
        abbrev = match.group(1).upper()
        if abbrev in US_STATES:
            return US_STATES[abbrev]

    return None


def extract_us_city_from_address(address: str) -> Optional[str]:
    """Extract city from a US address.

    US addresses: "<street>, <city>, <ST> <zip>"
    """
    if not address:
        return None

    # Strip country suffix (including RMS artifacts like "UnitedStates")
    cleaned = re.sub(
        r',?\s*(?:United\s*States|UnitedStates|USA|US)\s*$', '', address, flags=re.IGNORECASE
    ).strip()

    # Remove state + zip at end
    cleaned = re.sub(
        r',?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\s*$', '', cleaned
    ).strip().rstrip(',').strip()

    # Remove trailing state abbreviation
    cleaned = re.sub(r',\s*[A-Z]{2}\s*$', '', cleaned).strip().rstrip(',').strip()

    if not cleaned:
        return None

    # Take last comma-separated part as city
    parts = [p.strip() for p in cleaned.split(',') if p.strip()]
    if parts:
        candidate = parts[-1].strip()
        if candidate and not re.match(r'^\d', candidate) and len(candidate) > 2:
            return candidate

    return None


def extract_state_city_from_address(
    address: Optional[str], country: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Extract state and city from address based on country.

    Returns:
        (state, city) — either or both may be None
    """
    if not address or not country:
        return None, None

    if country == 'United Kingdom':
        return extract_uk_state_city_from_address(address)

    if country == 'Australia':
        state = infer_state_from_au_state_in_address(address)
        if not state:
            state = infer_au_state_from_address(address)
        city = extract_au_city_from_address(address)
        return state, city

    if country == 'United States':
        state = extract_us_state_from_address(address)
        city = extract_us_city_from_address(address)
        return state, city

    return None, None
