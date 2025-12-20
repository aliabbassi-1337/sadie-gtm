#!/usr/bin/env python3
"""
Sadie Scraper Serper - Hotel Scraper using Serper Google Maps API
=================================================================
Uses Serper.dev's Google Maps endpoint to find hotels.
Costs Serper credits but returns Google-quality data.

Usage:
    export SERPER_KEY=your_key
    python3 sadie_scraper_serper.py --city "Sydney, Australia" --radius-km 50
    python3 sadie_scraper_serper.py --query "hotels in Las Vegas"
"""

import csv
import os
import sys
import argparse
import time
import requests
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

SERPER_MAPS_URL = "https://google.serper.dev/maps"
SERPER_SEARCH_URL = "https://google.serper.dev/search"

DEFAULT_RADIUS_KM = 30

# Big chains to filter out
SKIP_CHAIN_NAMES = [
    "marriott", "hilton", "hyatt", "sheraton", "westin", "w hotel",
    "intercontinental", "holiday inn", "crowne plaza", "ihg",
    "best western", "choice hotels", "comfort inn", "quality inn",
    "radisson", "wyndham", "ramada", "days inn", "super 8", "motel 6",
    "la quinta", "travelodge", "ibis", "novotel", "mercure", "accor",
    "four seasons", "ritz-carlton", "st. regis", "fairmont",
]

# Stats
_stats = {"found": 0, "skipped_chains": 0, "api_calls": 0}


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


# ============================================================================
# SERPER GOOGLE MAPS API
# ============================================================================

_out_of_credits = False

def search_serper_maps(query: str, api_key: str, num_results: int = 100) -> list:
    """
    Search Google Maps via Serper.dev.
    Returns list of place results.
    """
    global _out_of_credits
    
    if _out_of_credits:
        return []
    
    _stats["api_calls"] += 1
    
    try:
        response = requests.post(
            SERPER_MAPS_URL,
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json"
            },
            json={
                "q": query,
                "num": num_results,
            },
            timeout=30
        )
        
        if response.status_code == 400 and "Not enough credits" in response.text:
            log("OUT OF CREDITS - stopping early")
            _out_of_credits = True
            return []
        
        if response.status_code != 200:
            log(f"API error: {response.status_code} - {response.text[:100]}")
            return []
        
        data = response.json()
        return data.get("places", [])
        
    except Exception as e:
        log(f"Error: {e}")
        return []


def search_hotels_in_area(location: str, api_key: str, neighborhoods: list = None) -> list:
    """
    Search for hotels in an area using multiple queries for better coverage.
    Searches main location + all neighborhoods/suburbs.
    """
    # Base search types
    search_types = [
        "hotels",
        "motels", 
        "resorts",
        "boutique hotels",
        "inns",
        "bed and breakfast",
        "guest house",
        "lodge",
    ]
    
    # Build list of areas to search
    areas = [location]
    if neighborhoods:
        areas.extend([f"{n}, {location}" for n in neighborhoods])
    
    all_places = []
    seen_names = set()
    
    total_queries = len(search_types) * len(areas)
    query_num = 0
    
    for area in areas:
        for search_type in search_types:
            query = f"{search_type} in {area}"
            query_num += 1
            log(f"[{query_num}/{total_queries}] {query}")
            
            places = search_serper_maps(query, api_key)
            new_count = 0
            
            for place in places:
                name = place.get("title", "").strip()
                if not name:
                    continue
                
                # Dedupe by name
                name_lower = name.lower()
                if name_lower in seen_names:
                    continue
                seen_names.add(name_lower)
                
                # Skip chains
                if any(chain in name_lower for chain in SKIP_CHAIN_NAMES):
                    _stats["skipped_chains"] += 1
                    continue
                
                all_places.append(place)
                _stats["found"] += 1
                new_count += 1
            
            log(f"  -> {new_count} new ({len(places)} raw)")
            
            if _out_of_credits:
                log("Stopping - out of credits")
                return all_places
            
            time.sleep(0.3)
    
    return all_places


# Neighborhood lists for major cities
CITY_NEIGHBORHOODS = {
    "sydney": [
        # CBD & Inner City
        "Sydney CBD", "The Rocks", "Circular Quay", "Darling Harbour", "Barangaroo",
        "Haymarket", "Chinatown Sydney", "Pyrmont", "Ultimo", "Chippendale",
        # Eastern Suburbs
        "Bondi", "Bondi Beach", "Bondi Junction", "Coogee", "Bronte", "Tamarama",
        "Randwick", "Kensington", "Maroubra", "Double Bay", "Rose Bay", "Vaucluse",
        "Paddington", "Woollahra", "Edgecliff", "Darling Point", "Elizabeth Bay",
        "Watsons Bay", "Dover Heights", "North Bondi", "South Coogee", "Clovelly",
        # Inner East
        "Surry Hills", "Darlinghurst", "Potts Point", "Kings Cross", "Woolloomooloo",
        "Rushcutters Bay", "Bellevue Hill", "Centennial Park", "Moore Park",
        # Inner West
        "Newtown", "Enmore", "Marrickville", "Erskineville", "St Peters",
        "Glebe", "Forest Lodge", "Annandale", "Leichhardt", "Balmain",
        "Rozelle", "Lilyfield", "Drummoyne", "Five Dock", "Burwood",
        "Strathfield", "Ashfield", "Summer Hill", "Stanmore", "Petersham",
        "Dulwich Hill", "Lewisham", "Haberfield", "Croydon", "Croydon Park",
        "Campsie", "Canterbury", "Lakemba", "Belmore", "Punchbowl",
        # North Shore
        "North Sydney", "Kirribilli", "Milsons Point", "Neutral Bay", "Cremorne",
        "Mosman", "Crows Nest", "St Leonards", "Artarmon", "Willoughby",
        "Chatswood", "Lane Cove", "Lindfield", "Roseville", "Gordon",
        "Pymble", "Turramurra", "Wahroonga", "Hornsby", "Macquarie Park",
        "North Ryde", "Ryde", "Gladesville", "Hunters Hill", "Epping",
        "Eastwood", "Denistone", "West Ryde", "Meadowbank", "Top Ryde",
        # Northern Beaches
        "Manly", "Manly Beach", "Dee Why", "Brookvale", "Freshwater",
        "Curl Curl", "Narrabeen", "Mona Vale", "Newport", "Avalon",
        "Palm Beach", "Collaroy", "Warriewood", "Whale Beach", "Bilgola",
        "Bayview", "Church Point", "Terrey Hills", "Belrose", "Frenchs Forest",
        # Hills District
        "Castle Hill", "Baulkham Hills", "Rouse Hill", "Norwest", "Bella Vista",
        "Kellyville", "The Ponds", "Stanhope Gardens", "Glenwood", "Parklea",
        "Cherrybrook", "West Pennant Hills", "Pennant Hills", "Beecroft",
        "Carlingford", "North Rocks", "Northmead", "Winston Hills", "Toongabbie",
        # Western Sydney - Parramatta Region
        "Parramatta", "Westmead", "Harris Park", "Granville", "Auburn",
        "Lidcombe", "Olympic Park", "Homebush", "Rhodes", "Concord",
        "Silverwater", "Newington", "Wentworth Point", "Sydney Olympic Park",
        "Merrylands", "Guildford", "Yennora", "Fairfield", "Cabramatta",
        "Canley Vale", "Canley Heights", "Villawood", "Carramar", "Lansdowne",
        # Western Sydney - Blacktown Region
        "Blacktown", "Seven Hills", "Toongabbie", "Wentworthville", "Pendle Hill",
        "Girraween", "Greystanes", "Prospect", "Pemulwuy", "Prairiewood",
        "Mt Druitt", "Rooty Hill", "Doonside", "Woodcroft", "Plumpton",
        "Quakers Hill", "Schofields", "Riverstone", "Marsden Park", "Box Hill NSW",
        # Western Sydney - Penrith Region
        "Penrith", "Kingswood", "Werrington", "St Marys", "Emu Plains",
        "Leonay", "Glenmore Park", "Jamisontown", "South Penrith", "Cranebrook",
        # South West Sydney
        "Liverpool", "Casula", "Moorebank", "Chipping Norton", "Warwick Farm",
        "Prestons", "Leppington", "Edmondson Park", "Ingleburn", "Minto",
        "Campbelltown", "Macarthur", "Camden", "Narellan", "Oran Park",
        "Gregory Hills", "Spring Farm", "Harrington Park", "Mount Annan",
        "Picton", "Tahmoor", "Thirlmere", "Appin", "Douglas Park",
        # Bankstown / Canterbury Region
        "Bankstown", "Bass Hill", "Chester Hill", "Sefton", "Birrong",
        "Regents Park", "Berala", "Yagoona", "Greenacre", "Mount Lewis",
        "Revesby", "Padstow", "Panania", "East Hills", "Picnic Point",
        # St George / Sutherland
        "Hurstville", "Kogarah", "Rockdale", "Brighton-Le-Sands", "Sans Souci",
        "Arncliffe", "Bardwell Park", "Bexley", "Kingsgrove", "Beverly Hills",
        "Penshurst", "Mortdale", "Oatley", "Lugarno", "Peakhurst",
        "Cronulla", "Sutherland", "Miranda", "Caringbah", "Gymea",
        "Kirrawee", "Sylvania", "Jannali", "Como", "Oyster Bay",
        "Engadine", "Heathcote", "Waterfall", "Bundeena", "Maianbar",
        # South Sydney
        "Mascot", "Sydney Airport", "Botany", "Rosebery", "Zetland",
        "Waterloo", "Alexandria", "Redfern", "Eveleigh", "Green Square",
        "Eastlakes", "Daceyville", "Kingsford", "Pagewood", "Hillsdale",
        # BLUE MOUNTAINS
        "Blue Mountains", "Katoomba", "Leura", "Wentworth Falls", "Blackheath",
        "Mount Victoria", "Springwood", "Faulconbridge", "Woodford", "Hazelbrook",
        "Lawson", "Bullaburra", "Glenbrook", "Blaxland", "Warrimoo",
        "Valley Heights", "Winmalee", "Yellow Rock", "Hawkesbury Heights",
        "Medlow Bath", "Megalong Valley",
        # CENTRAL COAST
        "Central Coast NSW", "Gosford", "Terrigal", "The Entrance", "Wyong",
        "Tuggerah", "Erina", "Woy Woy", "Ettalong Beach", "Umina Beach",
        "Avoca Beach", "Copacabana", "MacMasters Beach", "Toukley", "Norah Head",
        "Bateau Bay", "Long Jetty", "Killarney Vale", "Berkeley Vale", "Ourimbah",
        "Kariong", "Somersby", "Peats Ridge", "Kulnura", "Mangrove Mountain",
        # HAWKESBURY
        "Hawkesbury", "Windsor", "Richmond NSW", "Kurrajong", "Kurmond",
        "Wilberforce", "Pitt Town", "McGraths Hill", "Glossodia", "Freemans Reach",
        "Wisemans Ferry", "Spencer", "St Albans NSW",
        # WOLLONGONG REGION (close to Sydney)
        "Wollongong", "North Wollongong", "Fairy Meadow", "Corrimal", "Bulli",
        "Thirroul", "Austinmer", "Coledale", "Stanwell Park", "Helensburgh",
        "Otford", "Coalcliff", "Scarborough", "Wombarra",
        "Port Kembla", "Warrawong", "Dapto", "Albion Park", "Shellharbour",
        "Kiama", "Gerringong", "Berry", "Shoalhaven",
        # HUNTER REGION (day trip from Sydney)
        "Newcastle", "Newcastle CBD", "Darby Street Newcastle", "Honeysuckle",
        "The Junction", "Merewether", "Bar Beach", "Nobby's Beach",
        "Charlestown", "Kotara", "Lambton", "Hamilton NSW", "Mayfield",
        "Maitland", "Cessnock", "Pokolbin", "Hunter Valley", "Lovedale",
        "Broke", "Singleton", "Muswellbrook", "Scone",
        "Nelson Bay", "Port Stephens", "Shoal Bay", "Anna Bay", "Salamander Bay",
        "Raymond Terrace", "Tea Gardens", "Hawks Nest",
    ],
    "melbourne": [
        # CBD
        "Melbourne CBD", "Southbank", "Docklands", "Federation Square",
        # Inner Suburbs
        "South Yarra", "Prahran", "St Kilda", "St Kilda Beach", "Elwood",
        "Fitzroy", "Collingwood", "Carlton", "Parkville", "Brunswick",
        "Richmond", "Cremorne", "Abbotsford", "Hawthorn", "Kew",
        "South Melbourne", "Albert Park", "Port Melbourne", "Williamstown",
        "Footscray", "Yarraville", "Seddon", "Newport",
        # Eastern
        "Box Hill", "Glen Waverley", "Doncaster", "Ringwood", "Camberwell",
        "Malvern", "Caulfield", "Brighton", "Sandringham", "Moorabbin",
        # Northern
        "Preston", "Northcote", "Thornbury", "Coburg", "Heidelberg",
        "Bundoora", "Reservoir", "Epping", "Mill Park",
        # Western
        "Sunshine", "Maribyrnong", "Moonee Ponds", "Essendon", "Airport West",
        "Caroline Springs", "Werribee", "Point Cook",
        # Mornington Peninsula
        "Frankston", "Mornington", "Sorrento", "Portsea",
    ],
    "las vegas": [
        "Las Vegas Strip", "Downtown Las Vegas", "Fremont Street", "Arts District",
        "Paradise", "Winchester", "Sunrise Manor", "Spring Valley", "Enterprise",
        "Henderson", "Green Valley", "Summerlin", "Red Rock", "North Las Vegas",
        "Boulder City", "Lake Las Vegas", "Convention Center", "Chinatown Las Vegas",
    ],
    "miami": [
        "Miami Beach", "South Beach", "North Beach", "Mid-Beach", "Bal Harbour",
        "Downtown Miami", "Brickell", "Edgewater", "Wynwood", "Design District",
        "Little Havana", "Little Haiti", "Overtown", "Allapattah",
        "Coconut Grove", "Coral Gables", "Key Biscayne", "Virginia Key",
        "North Miami", "North Miami Beach", "Aventura", "Sunny Isles Beach",
        "Doral", "Sweetwater", "Hialeah", "Miami Springs", "Miami Airport",
        "Kendall", "Pinecrest", "Cutler Bay", "Homestead", "Florida City",
    ],
    "new york": [
        # Manhattan
        "Times Square", "Midtown Manhattan", "Midtown East", "Midtown West",
        "Herald Square", "Penn Station", "Grand Central", "Murray Hill",
        "Chelsea", "Flatiron", "Gramercy", "Union Square", "Greenwich Village",
        "West Village", "East Village", "SoHo", "NoHo", "Tribeca", "NoLita",
        "Lower East Side", "Chinatown Manhattan", "Little Italy", "Financial District",
        "Battery Park", "World Trade Center", "Upper East Side", "Upper West Side",
        "Harlem", "Washington Heights", "Morningside Heights", "Hell's Kitchen",
        # Brooklyn
        "Brooklyn Downtown", "Williamsburg", "Greenpoint", "DUMBO", "Brooklyn Heights",
        "Park Slope", "Prospect Heights", "Crown Heights", "Bed-Stuy",
        "Bushwick", "Cobble Hill", "Carroll Gardens", "Red Hook",
        # Queens
        "Long Island City", "Astoria", "Flushing", "Jamaica", "JFK Airport",
        "LaGuardia Airport", "Forest Hills", "Rockaway Beach",
        # Bronx & Staten Island
        "Bronx", "Fordham", "Staten Island", "St. George",
        # New Jersey (close)
        "Jersey City", "Hoboken", "Newark Airport",
    ],
    "london": [
        # Central
        "Westminster", "Mayfair", "Marylebone", "Fitzrovia", "Bloomsbury",
        "Soho", "Covent Garden", "Leicester Square", "Piccadilly", "St James",
        "Holborn", "Clerkenwell", "Farringdon", "Barbican", "City of London",
        # West End
        "Kensington", "South Kensington", "Earl's Court", "Chelsea", "Knightsbridge",
        "Belgravia", "Pimlico", "Victoria", "Notting Hill", "Bayswater",
        "Paddington", "Hyde Park", "Holland Park",
        # East
        "Shoreditch", "Hoxton", "Hackney", "Bethnal Green", "Whitechapel",
        "Canary Wharf", "Greenwich", "Stratford", "Docklands",
        # South
        "South Bank", "Southwark", "Borough", "London Bridge", "Waterloo",
        "Lambeth", "Vauxhall", "Brixton", "Clapham", "Battersea", "Peckham",
        # North
        "Camden", "Kings Cross", "Islington", "Angel", "Hampstead",
        "Highgate", "Finsbury Park", "Stoke Newington",
        # West
        "Hammersmith", "Fulham", "Shepherd's Bush", "Chiswick", "Ealing",
        "Richmond", "Wimbledon", "Putney",
        # Airports
        "Heathrow", "Gatwick", "Stansted", "Luton",
    ],
}


def parse_serper_place(place: dict) -> dict:
    """Convert Serper place result to our hotel format."""
    # Extract coordinates from position or address
    lat = place.get("latitude", "")
    lng = place.get("longitude", "")
    
    return {
        "hotel": place.get("title", ""),
        "website": place.get("website", ""),
        "phone": place.get("phoneNumber", ""),
        "lat": lat,
        "long": lng,
    }


# ============================================================================
# MAIN
# ============================================================================

def get_neighborhoods(location: str) -> list:
    """Get neighborhood list for a location if available."""
    location_lower = location.lower()
    for city, hoods in CITY_NEIGHBORHOODS.items():
        if city in location_lower:
            return hoods
    return []


def run_scraper(
    location: str,
    output_csv: str,
    api_key: str,
    custom_neighborhoods: list = None,
):
    """Main scraper function."""
    global _stats
    _stats = {"found": 0, "skipped_chains": 0, "api_calls": 0}
    
    log("Sadie Scraper Serper - Google Maps via Serper.dev")
    log(f"Location: {location}")
    
    # Get neighborhoods
    neighborhoods = custom_neighborhoods or get_neighborhoods(location)
    if neighborhoods:
        log(f"Searching {len(neighborhoods)} neighborhoods + main area")
    else:
        log("No neighborhoods defined - searching main area only")
        log("Tip: Add neighborhoods with --neighborhoods or add to CITY_NEIGHBORHOODS")
    
    start_time = time.time()
    
    # Search for hotels
    places = search_hotels_in_area(location, api_key, neighborhoods)
    
    if not places:
        log("No hotels found.")
        return
    
    # Convert to hotel format
    hotels = [parse_serper_place(p) for p in places]
    
    # Save to CSV
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    fieldnames = ["hotel", "website", "phone", "lat", "long"]
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hotels)
    
    elapsed = time.time() - start_time
    with_website = sum(1 for h in hotels if h.get("website"))
    
    log("")
    log("=" * 60)
    log("COMPLETE!")
    log(f"Hotels found:      {_stats['found']}")
    log(f"Skipped (chains):  {_stats['skipped_chains']}")
    log(f"With website:      {with_website}")
    log(f"Without website:   {_stats['found'] - with_website}")
    log(f"API calls:         {_stats['api_calls']}")
    log(f"Time:              {elapsed:.1f}s")
    log(f"Output:            {output_csv}")
    log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Sadie Scraper Serper - Google Maps scraper")
    
    parser.add_argument("--city", type=str, help="City/location to search")
    parser.add_argument("--query", type=str, help="Custom search query (overrides --city)")
    parser.add_argument("--neighborhoods", type=str, nargs="+", 
                        help="Custom neighborhoods to search (space-separated)")
    parser.add_argument("--no-neighborhoods", action="store_true",
                        help="Skip neighborhood searches, only search main area")
    parser.add_argument("--output", "-o", default="scraper_output/hotels_serper.csv")
    parser.add_argument("--api-key", type=str, help="Serper API key (overrides env var)")
    
    args = parser.parse_args()
    
    # Get API key: CLI arg > SERPER_KEY_2 env > SERPER_KEY env
    api_key = args.api_key or os.environ.get("SERPER_KEY_2", "") or os.environ.get("SERPER_KEY", "")
    if not api_key:
        log("ERROR: No API key provided")
        log("Use --api-key or set SERPER_KEY_2 environment variable")
        sys.exit(1)
    
    if not args.city and not args.query:
        parser.error("Either --city or --query is required")
    
    location = args.query or args.city
    
    # Handle neighborhoods
    if args.no_neighborhoods:
        neighborhoods = []
    else:
        neighborhoods = args.neighborhoods  # None means use defaults
    
    run_scraper(
        location=location,
        output_csv=args.output,
        api_key=api_key,
        custom_neighborhoods=neighborhoods if args.neighborhoods else None,
    )


if __name__ == "__main__":
    main()

