#!/usr/bin/env python3
"""
Optimize queries - prioritize high-value areas, skip tiny suburbs
"""

# HIGH VALUE - Major tourist/hotel areas (search with multiple property types)
high_value_suburbs = [
    "Sydney CBD", "The Rocks", "Circular Quay", "Darling Harbour", "Pyrmont",
    "Surry Hills", "Darlinghurst", "Potts Point", "Kings Cross", "Paddington",
    "Bondi", "Bondi Beach", "Bondi Junction", "Coogee", "Manly", "Manly Beach",
    "Newtown", "Glebe", "Chippendale", "Ultimo", "Haymarket", "Chinatown Sydney",
    "North Sydney", "Chatswood", "Parramatta", "Sydney Airport", "Mascot",
    "Double Bay", "Rose Bay", "Cronulla", "Wollongong", "Katoomba", "Leura",
    "Blue Mountains", "Central Coast", "Terrigal", "Gosford", "Liverpool",
    "Penrith", "Blacktown", "Campbelltown", "Hurstville", "Bankstown",
    "Olympic Park", "Homebush", "Strathfield", "Burwood", "Auburn",
    "Woolloomooloo", "Elizabeth Bay", "Rushcutters Bay", "Edgecliff",
    "Barangaroo", "Millers Point", "Kirribilli", "Neutral Bay", "Mosman",
    "Cremorne", "Lane Cove", "Hornsby", "Epping", "Macquarie Park",
    "Randwick", "Kensington", "Maroubra", "Brighton-Le-Sands", "Rockdale",
    "St Leonards", "Artarmon", "Willoughby", "Dee Why", "Brookvale",
    "Newport", "Avalon", "Palm Beach", "Mona Vale", "Narrabeen",
    "Castle Hill", "Bella Vista", "Rouse Hill", "Windsor", "Richmond",
    "Sutherland", "Miranda", "Caringbah", "Engadine", "Helensburgh",
]

# MEDIUM VALUE - Secondary areas (search with fewer property types)
medium_value_suburbs = [
    "Redfern", "Waterloo", "Zetland", "Alexandria", "Green Square", "Rosebery",
    "Erskineville", "Stanmore", "Marrickville", "Petersham", "Leichhardt",
    "Annandale", "Rozelle", "Balmain", "Lilyfield", "Drummoyne", "Gladesville",
    "Ryde", "West Ryde", "Eastwood", "Carlingford", "Beecroft", "Pennant Hills",
    "Thornleigh", "Waitara", "Turramurra", "Pymble", "Gordon", "Lindfield",
    "Roseville", "Killara", "Wahroonga", "Seaforth", "Balgowlah", "Freshwater",
    "Curl Curl", "Collaroy", "Narraweena", "Cromer", "Frenchs Forest",
    "Concord", "Rhodes", "Meadowbank", "Ermington", "Dundas", "Rydalmere",
    "Merrylands", "Granville", "Guildford", "Fairfield", "Cabramatta",
    "Lidcombe", "Berala", "Regents Park", "Bass Hill", "Yagoona",
    "Revesby", "Padstow", "Riverwood", "Peakhurst", "Mortdale", "Penshurst",
    "Kogarah", "Bexley", "Arncliffe", "Wolli Creek", "Tempe", "Sydenham",
    "Campsie", "Belmore", "Lakemba", "Punchbowl", "Greenacre",
    "Casula", "Moorebank", "Holsworthy", "Ingleburn", "Minto", "Leumeah",
    "Glenfield", "Macquarie Fields", "Seven Hills", "Rooty Hill", "Mt Druitt",
    "Quakers Hill", "Schofields", "Riverstone", "Marsden Park",
    "Jamisontown", "Kingswood", "Werrington", "St Marys", "St Clair",
    "Glenbrook", "Springwood", "Wentworth Falls", "Blackheath", "Mt Victoria",
    "Como", "Jannali", "Sylvania", "Gymea", "Kirrawee", "Woolooware",
]

# Property types
high_value_types = ["hotels", "motels", "accommodation", "serviced apartments", "bed and breakfast"]
medium_value_types = ["hotels", "accommodation"]

# Generate queries
queries = []

# High value suburbs with all property types
for suburb in high_value_suburbs:
    for prop_type in high_value_types:
        queries.append(f"{suburb} {prop_type}")

# Medium value suburbs with fewer types
for suburb in medium_value_suburbs:
    for prop_type in medium_value_types:
        queries.append(f"{suburb} {prop_type}")

# Add generic Sydney-wide queries
generic = [
    "Sydney boutique hotels",
    "Sydney luxury hotels",
    "Sydney budget hotels",
    "Sydney hostels",
    "Sydney backpackers",
    "Sydney waterfront hotels",
    "Sydney beachside hotels",
    "Sydney pet friendly hotels",
    "Sydney airport hotels",
    "hotels near Sydney Opera House",
    "hotels near Sydney Harbour Bridge",
    "hotels near Bondi Beach",
    "hotels near Darling Harbour",
]
queries.extend(generic)

# Deduplicate
queries = list(dict.fromkeys(queries))

# Write to file
with open("sydney_queries_optimized.txt", "w") as f:
    f.write(f"# Sydney Hotels - OPTIMIZED Query List\n")
    f.write(f"# Total queries: {len(queries)}\n")
    f.write(f"# High-value areas first, skip tiny suburbs\n\n")
    for q in queries:
        f.write(q + "\n")

print(f"Generated {len(queries)} optimized queries")
print(f"  - {len(high_value_suburbs)} high-value suburbs × {len(high_value_types)} types = {len(high_value_suburbs) * len(high_value_types)}")
print(f"  - {len(medium_value_suburbs)} medium-value suburbs × {len(medium_value_types)} types = {len(medium_value_suburbs) * len(medium_value_types)}")
print(f"  - {len(generic)} generic queries")
