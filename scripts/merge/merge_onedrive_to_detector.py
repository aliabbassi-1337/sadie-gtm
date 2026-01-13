#!/usr/bin/env python3
"""
Merge OneDrive Excel data back into detector_output/florida/.
Adds any leads from OneDrive that don't exist in detector output.
Does NOT overwrite existing data.
"""
import csv
import openpyxl
from pathlib import Path

onedrive = Path.home() / 'Library/CloudStorage/OneDrive-ValsoftCorporation/Sadie Lead Gen/USA/Florida'
detector = Path('detector_output/florida')

# Map OneDrive Excel names to detector CSV names
city_mappings = [
    ('Miami Beach.xlsx', 'miami_beach_leads.csv'),
    ('Kissimmee.xlsx', 'kissimmee_leads.csv'),
    ('Miami.xlsx', 'miami_leads.csv'),
    ('Pensacola.xlsx', 'pensacola_leads.csv'),
    ('Fort Lauderdale.xlsx', 'fort_lauderdale_leads.csv'),
    ('Tampa.xlsx', 'tampa_leads.csv'),
    ('St Augustine.xlsx', 'saint_augustine_leads.csv'),
    ('Key West.xlsx', 'key_west_leads.csv'),
    ('Windermere.xlsx', 'windermere_leads.csv'),
    ('Panama City Beach.xlsx', 'panama_city_beach_leads.csv'),
    ('Bay Pines.xlsx', 'bay_pines_leads.csv'),
    ('Orlando.xlsx', 'orlando_leads.csv'),
    ('Daytona Beach.xlsx', 'daytona_beach_leads.csv'),
    ('North Miami.xlsx', 'north_miami_beach_leads.csv'),
    ('Pompano Beach.xlsx', 'pompano_beach_leads.csv'),
    ('Homestead.xlsx', 'homestead_leads.csv'),
    ('Fort Myers Beach.xlsx', 'fort_myers_beach_leads.csv'),
    ('Hialeah.xlsx', 'hialeah_leads.csv'),
    ('St Petersburg.xlsx', 'saint_petersburg_leads.csv'),
    ('Clearwater Beach.xlsx', 'clearwater_beach_leads.csv'),
    ('Jacksonville.xlsx', 'jacksonville_leads.csv'),
    ('Sarasota.xlsx', 'sarasota_leads.csv'),
    ('Pembroke Pines.xlsx', 'pembroke_pines_leads.csv'),
    ('Fort Myers.xlsx', 'fort_myers_leads.csv'),
    ('High Springs.xlsx', 'high_springs_leads.csv'),
]

total_added = 0

for xlsx_name, csv_name in city_mappings:
    xlsx_path = onedrive / xlsx_name
    csv_path = detector / csv_name

    if not xlsx_path.exists():
        print(f'SKIP: {xlsx_name} not found in OneDrive')
        continue

    # Read existing detector data
    existing = {}
    fieldnames = None
    if csv_path.exists():
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
                existing[key] = row

    # Read OneDrive Excel
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        wb.close()
        continue

    excel_headers = [str(h).strip() if h else '' for h in rows[0]]

    # If no existing fieldnames, use excel headers
    if not fieldnames:
        fieldnames = excel_headers

    added = 0
    for row in rows[1:]:
        row_dict = dict(zip(excel_headers, [str(v) if v else '' for v in row]))
        key = (row_dict.get('name', '').strip().lower(), row_dict.get('website', '').strip().lower())

        if key not in existing and key[0]:  # Only add if not exists and has name
            # Map excel columns to detector columns
            new_row = {fn: row_dict.get(fn, '') for fn in fieldnames}
            existing[key] = new_row
            added += 1

    wb.close()

    # Write merged data
    if added > 0 or not csv_path.exists():
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing.values())

    total_added += added
    before = len(existing) - added
    print(f'{csv_name}: {before} existing + {added} from OneDrive = {len(existing)} total')

print(f'\nTotal new leads added from OneDrive: {total_added}')
