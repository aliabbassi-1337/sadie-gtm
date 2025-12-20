#!/usr/bin/env python3
"""Generate expanded Sydney hotel queries with property type variations."""

# All suburbs from the original file
suburbs = """Sydney CBD
The Rocks Sydney
Circular Quay
Millers Point
Barangaroo
Dawes Point
Haymarket
Chinatown Sydney
Ultimo
Pyrmont
Darling Harbour
Chippendale
Surry Hills
Darlinghurst
Potts Point
Woolloomooloo
Kings Cross
Elizabeth Bay
Rushcutters Bay
Paddington Sydney
Woollahra
Edgecliff
Double Bay
Point Piper
Rose Bay
Vaucluse
Watsons Bay
Dover Heights
Bondi
Bondi Beach
Bondi Junction
Bronte
Tamarama
Clovelly
Coogee
South Coogee
Maroubra
Malabar
La Perouse
Little Bay
Chifley
Eastgardens
Pagewood
Daceyville
Kingsford
Kensington Sydney
Randwick
Centennial Park
Moore Park
Newtown Sydney
Enmore
Marrickville
St Peters Sydney
Sydenham
Tempe
Petersham
Lewisham
Summer Hill
Ashfield
Croydon Sydney
Burwood
Strathfield
Homebush
Concord
Rhodes
Wentworth Point
Sydney Olympic Park
Lidcombe
Auburn
Silverwater
Regents Park Sydney
Berala
Birrong
Chester Hill
Bass Hill
Yagoona
Bankstown
Revesby
Padstow
Riverwood
Peakhurst
Hurstville
Mortdale
Penshurst
Beverly Hills Sydney
Narwee
Kingsgrove
Bexley
Arncliffe
Wolli Creek
Earlwood
Campsie
Belmore
Lakemba
Punchbowl
Greenacre
Villawood
Cabramatta
Mount Pritchard
Bonnyrigg
Fairfield
Smithfield
Wetherill Park
Guildford
Granville
Merrylands
Greystanes
Prospect
Toongabbie
Wentworthville
Westmead
Parramatta
Harris Park
Rosehill
Rydalmere
Dundas
Ermington
Meadowbank
West Ryde
Ryde
Gladesville
Putney
Eastwood
Epping
Carlingford
Beecroft
Pennant Hills
Thornleigh
Hornsby
Waitara
Wahroonga
Turramurra
Pymble
Gordon
Killara
Lindfield
Roseville
Chatswood
Willoughby
Artarmon
St Leonards
Wollstonecraft
North Sydney
McMahons Point
Milsons Point
Kirribilli
Neutral Bay
Cremorne
Mosman
Balmoral
Seaforth
Clontarf
Balgowlah
Manly
Manly Vale
Queenscliff
Freshwater
Curl Curl
Dee Why
Brookvale
Narrabeen
Collaroy
Mona Vale
Bayview
Terrey Hills
Newport
Avalon
Palm Beach
Castle Hill
Bella Vista
Kellyville
Rouse Hill
The Ponds
Stanhope Gardens
Glenwood
Seven Hills
Blacktown
Eastern Creek
Rooty Hill
Mount Druitt
Quakers Hill
Schofields
Riverstone
Marsden Park
Penrith
Jamisontown
Kingswood Sydney
Werrington
St Clair
St Marys
Glenbrook
Springwood
Katoomba
Leura
Blackheath
Windsor
Richmond Sydney
Liverpool
Warwick Farm
Moorebank
Casula
Prestons
Ingleburn
Macquarie Fields
Glenfield
Campbelltown
Leumeah
Minto
Sutherland
Jannali
Como
Sylvania
Miranda
Gymea
Kirrawee
Engadine
Cronulla
Woolooware
Kurnell
Wollongong
Thirroul
Helensburgh
Gosford
Terrigal
The Entrance
Mascot
Rosebery
Zetland
Waterloo Sydney
Alexandria
Redfern
Green Square
Erskineville
Stanmore
Annandale
Leichhardt
Rozelle
Balmain
Glebe
Camperdown""".strip().split('\n')

# Property types to combine
property_types = [
    "hotels",
    "motels",
    "accommodation",
    "bed and breakfast",
    "serviced apartments",
]

# Generate all combinations
queries = set()

# Suburb + property type combinations
for suburb in suburbs:
    suburb = suburb.strip()
    if not suburb:
        continue
    for prop_type in property_types:
        queries.add(f"{suburb} {prop_type}")

# Add some generic Sydney-wide searches
generic = [
    "Sydney hotels",
    "Sydney boutique hotels",
    "Sydney luxury hotels", 
    "Sydney budget hotels",
    "Sydney cheap hotels",
    "Sydney hostels",
    "Sydney backpackers",
    "Sydney guesthouse",
    "Sydney inn",
    "Sydney lodge",
    "Sydney waterfront hotels",
    "Sydney beachside hotels",
    "Sydney pet friendly hotels",
    "Sydney family hotels",
    "Sydney airport hotels",
    "hotels near Sydney Opera House",
    "hotels near Sydney Harbour Bridge",
    "hotels near Bondi Beach",
    "hotels near Manly Beach",
    "hotels near Sydney Airport",
    "hotels near Darling Harbour",
    "hotels near Circular Quay",
]

for q in generic:
    queries.add(q)

# Write to file
with open("sydney_queries_expanded.txt", "w") as f:
    f.write("# Sydney Hotels - EXPANDED Query List\n")
    f.write(f"# Total queries: {len(queries)}\n")
    f.write("# Generated with property type variations\n\n")
    for q in sorted(queries):
        f.write(q + "\n")

print(f"Generated {len(queries)} queries -> sydney_queries_expanded.txt")
