#!/usr/bin/env python3
"""
Sadie Post-Processor
====================
Cleans up detector output:
- Deduplicates by (name, website)
- Removes entries with errors

Usage:
    python3 sadie_postprocess.py detector_output/gatlinburg_leads.csv
    python3 sadie_postprocess.py detector_output/*.csv
"""

import csv
import sys
import os
from datetime import datetime


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def postprocess(input_file: str):
    """Process a single CSV file."""
    # Generate output filename
    base, ext = os.path.splitext(input_file)
    output_file = f"{base}_post{ext}"
    
    log(f"Processing: {input_file}")
    
    # Read input
    rows = []
    fieldnames = None
    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    original_count = len(rows)
    log(f"  Input rows: {original_count}")
    
    # Remove dead/broken domains
    dead_errors = ["ERR_NAME_NOT_RESOLVED", "ERR_HTTP2_PROTOCOL_ERROR"]
    clean_rows = [r for r in rows if not any(e in (r.get("error") or "") for e in dead_errors)]
    dead_count = len(rows) - len(clean_rows)
    if dead_count:
        log(f"  Removed dead/broken domains: {dead_count}")
    
    # Check for contact info
    def has_contact(row):
        return (row.get("phone_google") or "").strip() or \
               (row.get("phone_website") or "").strip() or \
               (row.get("email") or "").strip()
    
    # Fix rows with junk booking URLs (facebook, OTAs, chains, etc)
    junk_booking_domains = [
        # Social media
        "facebook.com", "instagram.com", "twitter.com", "youtube.com",
        "linkedin.com", "tiktok.com", "pinterest.com",
        # Review sites
        "yelp.com", "tripadvisor.com", "google.com", "maps.google.com",
        # OTAs
        "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
        "kayak.com", "trivago.com", "priceline.com", "agoda.com", "orbitz.com",
        "travelocity.com", "hotwire.com", "cheaptickets.com", "trip.com",
        # Big chains
        "hilton.com", "marriott.com", "ihg.com", "hyatt.com", "wyndham.com",
        "choicehotels.com", "bestwestern.com", "radissonhotels.com", "accor.com",
        # Short links / maps
        "goo.gl", "bit.ly", "maps.app",
    ]
    
    def has_junk_booking_url(row):
        booking_url = (row.get("booking_url") or "").lower()
        return any(junk in booking_url for junk in junk_booking_domains)
    
    fixed_rows = []
    junk_booking_fixed = 0
    junk_booking_removed = 0
    
    for row in clean_rows:
        if has_junk_booking_url(row):
            if has_contact(row):
                # Clear junk booking URL, mark as contact only
                row["booking_url"] = ""
                row["booking_engine"] = "contact_only"
                row["booking_engine_domain"] = ""
                row["error"] = ""
                fixed_rows.append(row)
                junk_booking_fixed += 1
            else:
                # No contact info and junk booking URL - remove
                junk_booking_removed += 1
        else:
            fixed_rows.append(row)
    
    clean_rows = fixed_rows
    if junk_booking_fixed:
        log(f"  Fixed junk booking URLs: {junk_booking_fixed}")
    if junk_booking_removed:
        log(f"  Removed junk booking (no contact): {junk_booking_removed}")
    
    # Remove remaining rows with no contact info
    before_contact = len(clean_rows)
    clean_rows = [r for r in clean_rows if has_contact(r)]
    no_contact_count = before_contact - len(clean_rows)
    if no_contact_count:
        log(f"  Removed no contact info: {no_contact_count}")
    
    # Remove junk domains
    junk_domains = [
        # Social media
        "facebook.com", "instagram.com", "twitter.com", "youtube.com",
        "linkedin.com", "yelp.com", "tripadvisor.com", "google.com",
        # OTAs
        "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
        "kayak.com", "trivago.com", "priceline.com", "agoda.com", "orbitz.com",
        "travelocity.com", "hotwire.com",
        # Big chains
        "hilton.com", "marriott.com", "ihg.com", "hyatt.com", "wyndham.com",
        "choicehotels.com", "bestwestern.com", "radissonhotels.com", "accor.com",
        "sonesta.com", "omnihotels.com", "fourseasons.com", "ritzcarlton.com",
        # Government
        ".gov", ".edu", ".mil", "nps.gov", "usda.gov", "fs.usda.gov",
        # Short links
        "maps.app.goo.gl", "goo.gl",
    ]
    
    def is_junk_domain(row):
        website = (row.get("website") or "").lower()
        # Filter junk domains
        if any(junk in website for junk in junk_domains):
            return True
        # Filter file links
        if website.endswith(".pdf") or ".pdf?" in website:
            return True
        return False
    
    before_junk = len(clean_rows)
    clean_rows = [r for r in clean_rows if not is_junk_domain(r)]
    junk_count = before_junk - len(clean_rows)
    log(f"  Removed junk domains: {junk_count}")
    
    
    # Deduplicate by (name, website)
    seen = set()
    deduped_rows = []
    for row in clean_rows:
        name = (row.get("name") or "").strip().lower()
        website = (row.get("website") or "").strip().lower()
        key = (name, website)
        
        if key in seen:
            continue
        seen.add(key)
        deduped_rows.append(row)
    
    dupe_count = len(clean_rows) - len(deduped_rows)
    log(f"  Removed duplicates: {dupe_count}")
    log(f"  Final rows: {len(deduped_rows)}")
    
    # Write output
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped_rows)
    
    log(f"  Output: {output_file} ({len(deduped_rows)} rows)")
    
    return {
        "input": original_count,
        "output": len(deduped_rows),
        "junk_removed": junk_count,
        "dupes_removed": dupe_count,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 sadie_postprocess.py <input.csv> [input2.csv ...]")
        sys.exit(1)
    
    files = sys.argv[1:]
    
    total_stats = {"input": 0, "output": 0, "junk_removed": 0, "dupes_removed": 0}
    
    for f in files:
        if not os.path.exists(f):
            log(f"File not found: {f}")
            continue
        
        stats = postprocess(f)
        for k, v in stats.items():
            total_stats[k] += v
        print()
    
    if len(files) > 1:
        log("=" * 50)
        log("TOTAL SUMMARY")
        log(f"  Input rows:         {total_stats['input']}")
        log(f"  Output rows:        {total_stats['output']}")
        log(f"  Junk removed:       {total_stats['junk_removed']}")
        log(f"  Duplicates removed: {total_stats['dupes_removed']}")


if __name__ == "__main__":
    main()

