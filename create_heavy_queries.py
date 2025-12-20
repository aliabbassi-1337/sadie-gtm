#!/usr/bin/env python3
"""
Create queries focused on HEAVY hotel areas with maximum variations
Target: Extract all 12k Sydney hotels
"""

# HEAVY HOTEL AREAS - run with MANY variations
heavy_hotel_areas = [
    # CBD Core (highest density)
    "Sydney CBD", "Sydney city centre", "Sydney central", "Sydney downtown",
    "George Street Sydney", "Pitt Street Sydney", "Martin Place Sydney",
    "Town Hall Sydney", "Wynyard Sydney", "Museum Station Sydney",
    
    # Harbour/Tourist Core
    "Circular Quay", "The Rocks Sydney", "Darling Harbour", "Barangaroo",
    "Pyrmont", "Ultimo", "Haymarket", "Chinatown Sydney", "World Square Sydney",
    
    # Inner East (backpacker/boutique hotspot)
    "Surry Hills", "Darlinghurst", "Potts Point", "Kings Cross",
    "Woolloomooloo", "Elizabeth Bay", "Rushcutters Bay",
    
    # Eastern Beaches
    "Bondi", "Bondi Beach", "Bondi Junction", "Coogee", "Coogee Beach",
    "Bronte", "Tamarama", "Maroubra",
    
    # North Shore
    "North Sydney", "Kirribilli", "Milsons Point", "Neutral Bay",
    "Cremorne", "Mosman", "Chatswood", "St Leonards", "Crows Nest",
    
    # Northern Beaches  
    "Manly", "Manly Beach", "Dee Why", "Narrabeen", "Mona Vale",
    "Newport", "Avalon", "Palm Beach", "Brookvale", "Freshwater",
    
    # Airport/South
    "Sydney Airport", "Mascot", "Wolli Creek", "Rockdale", 
    "Brighton-Le-Sands", "Cronulla", "Sutherland",
    
    # Western Sydney
    "Parramatta", "Olympic Park Sydney", "Homebush", "Strathfield",
    "Burwood", "Auburn", "Lidcombe", "Rhodes",
    
    # South West
    "Liverpool Sydney", "Bankstown", "Campbelltown", "Hurstville",
    
    # North West
    "Castle Hill", "Bella Vista", "Rouse Hill", "Blacktown",
    "Penrith", "Seven Hills",
    
    # Blue Mountains
    "Katoomba", "Leura", "Blue Mountains", "Blackheath", "Springwood",
]

# ALL property type variations
property_types = [
    "hotels",
    "hotel",  # singular sometimes returns different results
    "motels",
    "motel",
    "accommodation", 
    "lodging",
    "bed and breakfast",
    "B&B",
    "boutique hotels",
    "luxury hotels",
    "budget hotels",
    "cheap hotels",
    "serviced apartments",
    "apartment hotels",
    "hostels",
    "backpackers",
    "guesthouse",
    "guest house",
    "inn",
    "lodge",
    "resort",
    "suites",
    "rooms",
    "stay",
]

# Generate queries
queries = set()

# Heavy areas × all property types
for area in heavy_hotel_areas:
    for prop in property_types:
        queries.add(f"{area} {prop}")
        queries.add(f"{prop} in {area}")  # alternate phrasing

# Landmark-based (these often return different results)
landmarks = [
    "Sydney Opera House", "Sydney Harbour Bridge", "Bondi Beach",
    "Manly Beach", "Darling Harbour", "Circular Quay", "Sydney Airport",
    "Central Station Sydney", "Town Hall Sydney", "Sydney Convention Centre",
    "Sydney Olympic Park", "Taronga Zoo", "Sydney University", "UNSW",
    "Royal Botanic Gardens Sydney", "Sydney Tower", "Queen Victoria Building",
    "Westfield Sydney", "Star Casino Sydney", "Luna Park Sydney",
    "Sydney Fish Market", "Paddy's Markets Sydney", "Hyde Park Sydney",
    "Sydney Cricket Ground", "Allianz Stadium", "Sydney Football Stadium",
]

for landmark in landmarks:
    queries.add(f"hotels near {landmark}")
    queries.add(f"accommodation near {landmark}")
    queries.add(f"stay near {landmark}")

# Rating-based searches (return different subsets)
for area in ["Sydney", "Sydney CBD", "Bondi", "Manly", "Parramatta"]:
    queries.add(f"{area} 5 star hotels")
    queries.add(f"{area} 4 star hotels")
    queries.add(f"{area} 3 star hotels")
    queries.add(f"best hotels in {area}")
    queries.add(f"top rated hotels {area}")
    queries.add(f"cheap hotels {area}")
    queries.add(f"affordable hotels {area}")

# Price-based
for area in ["Sydney", "Sydney CBD", "Bondi", "Darling Harbour"]:
    queries.add(f"{area} hotels under $100")
    queries.add(f"{area} hotels under $150")
    queries.add(f"{area} hotels under $200")
    queries.add(f"budget {area} hotels")

# Feature-based
features = ["pool", "gym", "spa", "rooftop", "ocean view", "harbour view", 
            "pet friendly", "family friendly", "romantic", "business"]
for feature in features:
    queries.add(f"Sydney hotels with {feature}")
    queries.add(f"{feature} hotels Sydney")

# Write to file
queries = sorted(queries)

with open("sydney_queries_heavy.txt", "w") as f:
    f.write(f"# Sydney Hotels - HEAVY FOCUS Query List\n")
    f.write(f"# Total queries: {len(queries)}\n")
    f.write(f"# Focused on hotel-dense areas with maximum variations\n\n")
    for q in queries:
        f.write(q + "\n")

print(f"Generated {len(queries)} queries focused on heavy hotel areas")
print(f"  - {len(heavy_hotel_areas)} heavy areas × {len(property_types)} types × 2 phrasings")
print(f"  - {len(landmarks)} landmark searches × 3 types")
print(f"  - Rating/price/feature variations")
