"""Normalize country names to standard English format.

Usage:
    python -m workflows.normalize_countries [--dry-run]
"""

import asyncio
import sys
from db.client import init_db, close_db, get_conn

# ISO 2-letter codes to full country names
COUNTRY_CODES = {
    "CA": "Canada",
    "CR": "Costa Rica",
    "KH": "Cambodia",
    "GT": "Guatemala",
    "LK": "Sri Lanka",
    "BZ": "Belize",
    "DO": "Dominican Republic",
    "UY": "Uruguay",
    "EC": "Ecuador",
    "SV": "El Salvador",
    "TZ": "Tanzania",
    "NI": "Nicaragua",
    "PA": "Panama",
    "HN": "Honduras",
    "MT": "Malta",
    "EE": "Estonia",
    "LA": "Laos",
    "MM": "Myanmar",
    "MV": "Maldives",
    "FJ": "Fiji",
    "MA": "Morocco",
    "GE": "Georgia",
    "AW": "Aruba",
    "BO": "Bolivia",
    "IS": "Iceland",
    "MU": "Mauritius",
    "LV": "Latvia",
    "TW": "Taiwan",
    "JO": "Jordan",
    "TC": "Turks and Caicos Islands",
    "CY": "Cyprus",
    "LT": "Lithuania",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "HR": "Croatia",
    "RS": "Serbia",
    "ME": "Montenegro",
    "BA": "Bosnia and Herzegovina",
    "MK": "North Macedonia",
    "AL": "Albania",
    "BG": "Bulgaria",
    "RO": "Romania",
    "MD": "Moldova",
    "UA": "Ukraine",
    "BY": "Belarus",
    "AM": "Armenia",
    "AZ": "Azerbaijan",
    "KZ": "Kazakhstan",
    "UZ": "Uzbekistan",
    "KG": "Kyrgyzstan",
    "TJ": "Tajikistan",
    "TM": "Turkmenistan",
    "MN": "Mongolia",
    "NP": "Nepal",
    "BD": "Bangladesh",
    "PK": "Pakistan",
    "AF": "Afghanistan",
    "LB": "Lebanon",
    "SY": "Syria",
    "IQ": "Iraq",
    "IR": "Iran",
    "SA": "Saudi Arabia",
    "AE": "United Arab Emirates",
    "QA": "Qatar",
    "BH": "Bahrain",
    "KW": "Kuwait",
    "OM": "Oman",
    "YE": "Yemen",
    "EG": "Egypt",
    "LY": "Libya",
    "TN": "Tunisia",
    "DZ": "Algeria",
    "SD": "Sudan",
    "ET": "Ethiopia",
    "KE": "Kenya",
    "UG": "Uganda",
    "RW": "Rwanda",
    "BI": "Burundi",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
    "BW": "Botswana",
    "NA": "Namibia",
    "MZ": "Mozambique",
    "MW": "Malawi",
    "ZA": "South Africa",
    "SZ": "Eswatini",
    "LS": "Lesotho",
    "MG": "Madagascar",
    "SC": "Seychelles",
    "RE": "Reunion",
    "YT": "Mayotte",
    "KM": "Comoros",
    "GH": "Ghana",
    "NG": "Nigeria",
    "SN": "Senegal",
    "ML": "Mali",
    "BF": "Burkina Faso",
    "CI": "Ivory Coast",
    "LR": "Liberia",
    "SL": "Sierra Leone",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "CV": "Cape Verde",
    "GM": "Gambia",
    "MR": "Mauritania",
    "NE": "Niger",
    "TD": "Chad",
    "CM": "Cameroon",
    "CF": "Central African Republic",
    "GA": "Gabon",
    "CG": "Republic of the Congo",
    "CD": "Democratic Republic of the Congo",
    "AO": "Angola",
    "GQ": "Equatorial Guinea",
    "ST": "Sao Tome and Principe",
    "DJ": "Djibouti",
    "ER": "Eritrea",
    "SO": "Somalia",
    "SS": "South Sudan",
    "PH": "Philippines",
    "MY": "Malaysia",
    "SG": "Singapore",
    "ID": "Indonesia",
    "TH": "Thailand",
    "VN": "Vietnam",
    "KR": "South Korea",
    "JP": "Japan",
    "CN": "China",
    "HK": "Hong Kong",
    "MO": "Macau",
    "BN": "Brunei",
    "TL": "Timor-Leste",
    "PG": "Papua New Guinea",
    "AU": "Australia",
    "NZ": "New Zealand",
    "WS": "Samoa",
    "TO": "Tonga",
    "VU": "Vanuatu",
    "NC": "New Caledonia",
    "PF": "French Polynesia",
    "GU": "Guam",
    "FM": "Micronesia",
    "PW": "Palau",
    "MH": "Marshall Islands",
    "KI": "Kiribati",
    "NR": "Nauru",
    "TV": "Tuvalu",
    "SB": "Solomon Islands",
    "CK": "Cook Islands",
    "NU": "Niue",
    "TK": "Tokelau",
    "AS": "American Samoa",
    "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico",
    "VI": "U.S. Virgin Islands",
    "VG": "British Virgin Islands",
    "AI": "Anguilla",
    "MS": "Montserrat",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucia",
    "VC": "Saint Vincent and the Grenadines",
    "GD": "Grenada",
    "BB": "Barbados",
    "TT": "Trinidad and Tobago",
    "JM": "Jamaica",
    "HT": "Haiti",
    "CU": "Cuba",
    "BS": "Bahamas",
    "KY": "Cayman Islands",
    "BM": "Bermuda",
    "GL": "Greenland",
    "FO": "Faroe Islands",
    "AX": "Aland Islands",
    "SJ": "Svalbard and Jan Mayen",
    "GI": "Gibraltar",
    "AD": "Andorra",
    "MC": "Monaco",
    "SM": "San Marino",
    "VA": "Vatican City",
    "LI": "Liechtenstein",
    "LU": "Luxembourg",
    "BE": "Belgium",
    "NL": "Netherlands",
    "CH": "Switzerland",
    "AT": "Austria",
    "DE": "Germany",
    "PL": "Poland",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "IE": "Ireland",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "FR": "France",
    "ES": "Spain",
    "PT": "Portugal",
    "IT": "Italy",
    "GR": "Greece",
    "TR": "Turkey",
    "IL": "Israel",
    "PS": "Palestine",
    "CL": "Chile",
    "AR": "Argentina",
    "BR": "Brazil",
    "PE": "Peru",
    "CO": "Colombia",
    "VE": "Venezuela",
    "GY": "Guyana",
    "SR": "Suriname",
    "GF": "French Guiana",
    "PY": "Paraguay",
    "MX": "Mexico",
    "US": "United States",
    "IN": "India",
    "RU": "Russia",
    "MF": "Saint Martin",
    "SX": "Sint Maarten",
    "BQ": "Caribbean Netherlands",
    "CW": "Curacao",
    "AG": "Antigua and Barbuda",
    "GG": "Guernsey",
    "JE": "Jersey",
    "IM": "Isle of Man",
    "AN": "Netherlands Antilles",
    "HU": "Hungary",
    "GP": "Guadeloupe",
    "MQ": "Martinique",
}

# Native language names to English
COUNTRY_VARIATIONS = {
    # European languages
    "Nederland": "Netherlands",
    "België / Belgique / Belgien": "Belgium",
    "Belgique": "Belgium",
    "Belgien": "Belgium",
    "België": "Belgium",
    "Österreich": "Austria",
    "Schweiz/Suisse/Svizzera/Svizra": "Switzerland",
    "Schweiz": "Switzerland",
    "Suisse": "Switzerland",
    "Sverige": "Sweden",
    "Norge": "Norway",
    "Deutschland": "Germany",
    "España": "Spain",
    "Danmark": "Denmark",
    "Suomi / Finland": "Finland",
    "Suomi": "Finland",
    "Magyarország": "Hungary",
    "Česko": "Czech Republic",
    "Česká republika": "Czech Republic",
    "Italia": "Italy",
    "Éire / Ireland": "Ireland",
    "Éire": "Ireland",
    "Ελλάς": "Greece",
    "Ελλάδα": "Greece",
    "Κύπρος - Kıbrıs": "Cyprus",
    "Κύπρος": "Cyprus",
    "Türkiye": "Turkey",
    "Hrvatska": "Croatia",
    "Србија": "Serbia",
    "Polska": "Poland",
    "România": "Romania",
    "България": "Bulgaria",
    "Slovensko": "Slovakia",
    "Slovenija": "Slovenia",
    "Eesti": "Estonia",
    "Latvija": "Latvia",
    "Lietuva": "Lithuania",
    "საქართველო": "Georgia",
    "Україна": "Ukraine",
    "Беларусь": "Belarus",
    "Россия": "Russia",
    
    # Asian languages
    "日本": "Japan",
    "中国": "China",
    "中國": "China",
    "대한민국": "South Korea",
    "한국": "South Korea",
    "臺灣": "Taiwan",
    "台灣": "Taiwan",
    "ประเทศไทย": "Thailand",
    "Việt Nam": "Vietnam",
    "ދިވެހިރާއްޖެ": "Maldives",
    "नेपाल": "Nepal",
    "বাংলাদেশ": "Bangladesh",
    "ශ්‍රී ලංකාව": "Sri Lanka",
    "भारत": "India",
    "پاکستان": "Pakistan",
    
    # Arabic/Middle East
    "السعودية": "Saudi Arabia",
    "المملكة العربية السعودية": "Saudi Arabia",
    "عمان": "Oman",
    "الإمارات": "United Arab Emirates",
    "قطر": "Qatar",
    "البحرين": "Bahrain",
    "الكويت": "Kuwait",
    "الأردن": "Jordan",
    "لبنان": "Lebanon",
    "فلسطين": "Palestine",
    "مصر": "Egypt",
    "المغرب": "Morocco",
    "Maroc ⵍⵎⵖⵔⵉⴱ المغرب": "Morocco",
    "تونس": "Tunisia",
    "الجزائر": "Algeria",
    
    # African
    "Soomaaliland أرض الصومال": "Somalia",
    "Madagasikara / Madagascar": "Madagascar",
    "Madagasikara": "Madagascar",
    
    # Pacific/Oceania
    "New Zealand / Aotearoa": "New Zealand",
    "Aotearoa": "New Zealand",
    "Belau": "Palau",
    
    # Americas
    "México": "Mexico",
    "Panamá": "Panama",
    "Perú": "Peru",
    "Brasil": "Brazil",
    
    # English variations of United States
    "USA": "United States",
    "U.S.A.": "United States",
    "U.S.": "United States",
    "United States of America": "United States",

    # Variations
    "The Bahamas": "Bahamas",
    "US Virgin Islands": "U.S. Virgin Islands",
    "Curaçao": "Curacao",
    "Turks & Caicos Islands": "Turks and Caicos Islands",
    "Mauritius / Maurice": "Mauritius",
    "Phan Thiet -Vietnam": "Vietnam",
}

# Garbage values to set to NULL
GARBAGE_COUNTRIES = [
    "Best Price guaranteed on our website",
    "-",
    ".",
    "N/A",
    "n/a",
    "NA",  # Be careful - this could be Namibia, but we handle NA in COUNTRY_CODES
    "Unknown",
    "unknown",
    "UNKNOWN",
    "None",
    "null",
    "NULL",
]


async def normalize_countries(dry_run: bool = False) -> int:
    """Normalize country names and codes to standard English format."""
    await init_db()
    
    async with get_conn() as conn:
        total = 0
        
        # First, normalize 2-letter codes
        print("Normalizing country codes...")
        for code, name in COUNTRY_CODES.items():
            if dry_run:
                result = await conn.fetchval(
                    "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country = $1 AND status != -1",
                    code
                )
                if result > 0:
                    print(f"  [DRY-RUN] {code} -> {name}: {result}")
                    total += result
            else:
                result = await conn.execute("""
                    UPDATE sadie_gtm.hotels 
                    SET country = $1, updated_at = NOW()
                    WHERE country = $2 AND status != -1
                """, name, code)
                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    print(f"  {code} -> {name}: {count}")
                    total += count
        
        # Then, normalize native language names
        print("\nNormalizing country name variations...")
        for old, new in COUNTRY_VARIATIONS.items():
            if dry_run:
                result = await conn.fetchval(
                    "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country = $1 AND status != -1",
                    old
                )
                if result > 0:
                    print(f"  [DRY-RUN] {old[:40]} -> {new}: {result}")
                    total += result
            else:
                result = await conn.execute("""
                    UPDATE sadie_gtm.hotels 
                    SET country = $1, updated_at = NOW()
                    WHERE country = $2 AND status != -1
                """, new, old)
                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    print(f"  {old[:40]} -> {new}: {count}")
                    total += count
        
        # Finally, set garbage to NULL
        print("\nRemoving garbage country values...")
        for g in GARBAGE_COUNTRIES:
            # Skip "NA" as it's already in COUNTRY_CODES -> Namibia
            if g == "NA":
                continue
            if dry_run:
                result = await conn.fetchval(
                    "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country = $1 AND status != -1",
                    g
                )
                if result > 0:
                    print(f"  [DRY-RUN] '{g}' -> NULL: {result}")
                    total += result
            else:
                result = await conn.execute("""
                    UPDATE sadie_gtm.hotels 
                    SET country = NULL, updated_at = NOW()
                    WHERE country = $1 AND status != -1
                """, g)
                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    print(f"  '{g}' -> NULL: {count}")
                    total += count
        
        print(f"\n{'[DRY-RUN] Would fix' if dry_run else 'Total fixed'}: {total}")
        
    await close_db()
    return total


async def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN MODE ===\n")
    
    await normalize_countries(dry_run=dry_run)


if __name__ == "__main__":
    asyncio.run(main())
