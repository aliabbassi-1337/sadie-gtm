#!/usr/bin/env python3
"""
Merge backup files into detector_output/florida/.
PREFERS enriched rows (with room_count) over raw rows.

Usage:
    python merge_backup_to_detector.py                          # Uses latest backup
    python merge_backup_to_detector.py backup_20260111_225100   # Uses specific backup
"""
import csv
import sys
from pathlib import Path

detector = Path('detector_output/florida')

# Find backup dir
if len(sys.argv) > 1:
    backup_name = sys.argv[1]
else:
    # Find latest backup
    backups = sorted([d for d in detector.parent.glob('backup_florida_full_*') if d.is_dir()])
    if not backups:
        backups = sorted([d for d in detector.glob('backup_*') if d.is_dir()])
    if not backups:
        print("No backup found. Specify backup folder name as argument.")
        exit(1)
    backup_name = backups[-1].name

# Check both possible locations
backup_dir = detector.parent / backup_name
if not backup_dir.exists():
    backup_dir = detector / backup_name
if not backup_dir.exists():
    print(f"Backup not found: {backup_name}")
    exit(1)

print(f"Merging from: {backup_dir}")


def is_enriched(row):
    """Check if row has enriched data (room_count filled)."""
    room_count = row.get('room_count', '').strip()
    return bool(room_count and room_count != '0')


# Get all backup lead files
backup_files = list(backup_dir.glob('*_leads.csv'))
print(f"Found {len(backup_files)} files in backup")

total_added = 0
total_kept_enriched = 0

for backup_file in backup_files:
    csv_name = backup_file.name
    detector_file = detector / csv_name

    # Read backup data
    backup_rows = {}
    with open(backup_file) as f:
        reader = csv.DictReader(f)
        backup_fieldnames = reader.fieldnames
        for row in reader:
            key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
            if key[0]:  # Has name
                backup_rows[key] = row

    # Read existing detector data
    existing = {}
    fieldnames = backup_fieldnames
    if detector_file.exists():
        with open(detector_file) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                key = (row.get('name', '').strip().lower(), row.get('website', '').strip().lower())
                existing[key] = row

    # Merge: prefer enriched rows, add new from backup
    merged = {}
    added = 0
    kept_enriched = 0

    # Start with all existing rows
    for key, row in existing.items():
        merged[key] = row

    # Add backup rows, but prefer enriched data
    for key, backup_row in backup_rows.items():
        if key not in merged:
            # New row from backup - add it
            new_row = {fn: backup_row.get(fn, '') for fn in fieldnames}
            merged[key] = new_row
            added += 1
        else:
            # Row exists in both - keep whichever is enriched
            existing_row = merged[key]
            existing_enriched = is_enriched(existing_row)
            backup_enriched = is_enriched(backup_row)

            if existing_enriched:
                kept_enriched += 1
            elif backup_enriched:
                # Backup has enriched, current doesn't - use backup
                merged[key] = {fn: backup_row.get(fn, '') for fn in fieldnames}
                kept_enriched += 1

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

print(f'\nTotal new leads added: {total_added}')
print(f'Total enriched rows preserved: {total_kept_enriched}')
