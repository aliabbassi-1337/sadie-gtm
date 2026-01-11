#!/usr/bin/env python3
"""
Merge S3 detection results into detector_output/florida/.
Downloads from S3 to temp dir, then merges unique leads.
PREFERS enriched rows (with room_count) over raw rows.
"""
import csv
import subprocess
import tempfile
import shutil
from pathlib import Path

detector = Path('detector_output/florida')

# Download S3 data to temp dir
print("Downloading from S3...")
temp_dir = Path(tempfile.mkdtemp())
s3_dir = temp_dir / 'florida'
s3_dir.mkdir()

result = subprocess.run(
    ['aws', 's3', 'sync', 's3://sadie-gtm/detector_output/florida/', str(s3_dir)],
    capture_output=True, text=True
)
if result.returncode != 0:
    print(f"S3 sync failed: {result.stderr}")
    shutil.rmtree(temp_dir)
    exit(1)

# Get all S3 lead files
s3_files = list(s3_dir.glob('*_leads.csv'))
print(f"Found {len(s3_files)} files from S3")


def is_enriched(row):
    """Check if row has enriched data (room_count filled)."""
    room_count = row.get('room_count', '').strip()
    return bool(room_count and room_count != '0')


total_added = 0
total_kept_enriched = 0

for s3_file in s3_files:
    csv_name = s3_file.name
    detector_file = detector / csv_name

    # Read S3 data
    s3_rows = {}
    with open(s3_file) as f:
        reader = csv.DictReader(f)
        s3_fieldnames = reader.fieldnames
        for row in reader:
            key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
            if key[0]:  # Has name
                s3_rows[key] = row

    # Read existing detector data
    existing = {}
    fieldnames = s3_fieldnames
    if detector_file.exists():
        with open(detector_file) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
                existing[key] = row

    # Merge: prefer enriched rows, add new from S3
    merged = {}
    added = 0
    kept_enriched = 0

    # Start with all existing rows
    for key, row in existing.items():
        merged[key] = row

    # Add S3 rows, but don't overwrite enriched data
    for key, s3_row in s3_rows.items():
        if key not in merged:
            # New row from S3 - add it
            new_row = {fn: s3_row.get(fn, '') for fn in fieldnames}
            merged[key] = new_row
            added += 1
        else:
            # Row exists - keep enriched version
            existing_row = merged[key]
            if is_enriched(existing_row):
                kept_enriched += 1
            elif is_enriched(s3_row):
                # S3 has enriched, local doesn't - use S3
                merged[key] = {fn: s3_row.get(fn, '') for fn in fieldnames}

    if added > 0 or kept_enriched > 0:
        # Write merged
        with open(detector_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged.values())

        total_added += added
        total_kept_enriched += kept_enriched
        if added > 0:
            print(f'{csv_name}: +{added} new, kept {kept_enriched} enriched, total {len(merged)}')

# Cleanup temp
shutil.rmtree(temp_dir)

print(f'\nTotal new leads added: {total_added}')
print(f'Total enriched rows preserved: {total_kept_enriched}')
