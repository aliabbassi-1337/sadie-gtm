#!/usr/bin/env python3
"""
Merge all sources into detector_output/florida/:
- Current detector files
- Backup files
- S3 files

Keeps the BEST version of each row (prefers enriched data).
Deduplicates by name+website.
"""
import csv
import subprocess
import tempfile
import shutil
from pathlib import Path

detector = Path('detector_output/florida')
backup = Path('detector_output/backup_florida_full_20260111_225100')

def is_enriched(row):
    """Check if row has enriched data."""
    room_count = row.get('room_count', '').strip()
    return bool(room_count and room_count != '0')

def enrichment_score(row):
    """Score how enriched a row is. Higher = more data."""
    score = 0
    if row.get('room_count', '').strip() and row.get('room_count', '').strip() != '0':
        score += 10
    if row.get('email', '').strip():
        score += 5
    if row.get('phone_website', '').strip():
        score += 3
    if row.get('booking_engine', '').strip() and row.get('booking_engine') not in ('unknown', 'unknown_third_party'):
        score += 2
    return score

def best_row(row1, row2):
    """Return the row with more enriched data."""
    if enrichment_score(row1) >= enrichment_score(row2):
        return row1
    return row2

# Download S3 to temp
print("Downloading from S3...")
temp_dir = Path(tempfile.mkdtemp())
s3_dir = temp_dir / 'florida'
s3_dir.mkdir()
subprocess.run(
    ['aws', 's3', 'sync', 's3://sadie-gtm/detector_output/florida/', str(s3_dir)],
    capture_output=True
)

# Get all unique city files
all_cities = set()
for f in detector.glob('*_leads.csv'):
    if 'backup' not in str(f) and 'funnel' not in f.name:
        all_cities.add(f.name)
for f in backup.glob('*_leads.csv'):
    if 'funnel' not in f.name:
        all_cities.add(f.name)
for f in s3_dir.glob('*_leads.csv'):
    if 'funnel' not in f.name:
        all_cities.add(f.name)

print(f"Processing {len(all_cities)} city files...")

total_leads = 0

for csv_name in sorted(all_cities):
    detector_file = detector / csv_name
    backup_file = backup / csv_name
    s3_file = s3_dir / csv_name

    merged = {}
    all_fieldnames = set()

    # Read from all sources
    sources = [
        ('detector', detector_file),
        ('backup', backup_file),
        ('s3', s3_file),
    ]

    # First pass: collect all fieldnames
    for source_name, source_file in sources:
        if not source_file.exists():
            continue
        with open(source_file) as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                all_fieldnames.update(reader.fieldnames)

    # Second pass: read and merge data
    for source_name, source_file in sources:
        if not source_file.exists():
            continue
        with open(source_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
                if not key[0]:
                    continue
                if key in merged:
                    merged[key] = best_row(merged[key], row)
                else:
                    merged[key] = row

    if not merged or not all_fieldnames:
        continue

    # Standard field order
    fieldnames = ['name', 'website', 'booking_url', 'booking_engine', 'booking_engine_domain',
                  'phone_google', 'phone_website', 'email',
                  'address', 'latitude', 'longitude', 'rating', 'review_count', 'room_count']
    # Add any extra fields from sources (except ones we don't care about)
    skip_fields = {'detection_method', 'error'}
    for fn in all_fieldnames:
        if fn not in fieldnames and fn not in skip_fields:
            fieldnames.append(fn)

    # Write merged (filter out skipped fields from rows)
    with open(detector_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(merged.values())

    total_leads += len(merged)
    print(f'{csv_name}: {len(merged)} leads')

# Cleanup
shutil.rmtree(temp_dir)

print(f'\nTotal: {total_leads} leads across {len(all_cities)} cities')
