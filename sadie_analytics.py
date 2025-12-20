#!/usr/bin/env python3
"""
Sadie Analytics - Analyze Detector Output
==========================================
Prints comprehensive statistics from detector output CSV files.

Usage:
    python3 sadie_analytics.py detector_output/ocean_city_leads.csv
    python3 sadie_analytics.py detector_output/*.csv
"""

import csv
import sys
import os
from collections import Counter, defaultdict
from urllib.parse import urlparse

def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return ""

def analyze_csv(filepath: str):
    """Analyze a single CSV file and print stats."""
    
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print(f"No data in {filepath}")
        return
    
    # ========================================================================
    # BASIC COUNTS
    # ========================================================================
    total = len(rows)
    
    has_website = [r for r in rows if r.get("website", "").strip()]
    has_booking_url = [r for r in rows if r.get("booking_url", "").strip()]
    has_phone_google = [r for r in rows if r.get("phone_google", "").strip()]
    has_phone_website = [r for r in rows if r.get("phone_website", "").strip()]
    has_email = [r for r in rows if r.get("email", "").strip()]
    has_address = [r for r in rows if r.get("address", "").strip()]
    has_screenshot = [r for r in rows if r.get("screenshot_path", "").strip()]
    has_error = [r for r in rows if r.get("error", "").strip() and r.get("error") != "no_website"]
    
    # Contact info (any phone or email)
    has_any_contact = [r for r in rows if r.get("phone_google") or r.get("phone_website") or r.get("email")]
    
    # ========================================================================
    # BOOKING ENGINE BREAKDOWN
    # ========================================================================
    engine_counts = Counter(r.get("booking_engine", "") for r in rows if r.get("booking_engine", "").strip())
    
    # Known vs unknown engines
    known_engines = {k: v for k, v in engine_counts.items() 
                     if k and k not in ["unknown", "unknown_third_party", "contact_only", "proprietary_or_same_domain"]}
    unknown_engines = engine_counts.get("unknown", 0) + engine_counts.get("unknown_third_party", 0)
    contact_only = engine_counts.get("contact_only", 0)
    proprietary = engine_counts.get("proprietary_or_same_domain", 0)
    
    # ========================================================================
    # BOOKING ENGINE DOMAINS (for unknown_third_party)
    # ========================================================================
    # Filter out known junk domains
    JUNK_DOMAINS = [
        "facebook.com", "booking.com", "expedia.com", "hotels.com", 
        "tripadvisor.com", "yelp.com", "google.com", "airbnb.com",
        "vrbo.com", "twitter.com", "instagram.com", "youtube.com",
    ]
    third_party_domains = Counter()
    for r in rows:
        if r.get("booking_engine") == "unknown_third_party":
            domain = r.get("booking_engine_domain", "")
            if domain and not any(junk in domain for junk in JUNK_DOMAINS):
                third_party_domains[domain] += 1
    
    # ========================================================================
    # ERROR BREAKDOWN (exclude no_website - that's not an error)
    # ========================================================================
    error_counts = Counter(r.get("error", "") for r in rows 
                          if r.get("error", "").strip() and r.get("error") != "no_website")
    
    # ========================================================================
    # DETECTION METHOD BREAKDOWN
    # ========================================================================
    detection_methods = Counter()
    for r in rows:
        method = r.get("detection_method", "")
        if method:
            # Count each component of the method
            for part in method.split("+"):
                detection_methods[part.strip()] += 1
    
    # ========================================================================
    # WEBSITE DOMAIN ANALYSIS
    # ========================================================================
    website_domains = Counter(extract_domain(r.get("website", "")) for r in has_website)
    top_website_domains = website_domains.most_common(10)
    
    # ========================================================================
    # LEAD QUALITY TIERS
    # ========================================================================
    tier1_leads = []  # Has booking URL + known engine
    tier2_leads = []  # Has booking URL + unknown engine
    tier3_leads = []  # Contact only (phone/email, no booking)
    tier4_leads = []  # Has website but no booking/contact
    tier5_leads = []  # No website at all
    
    for r in rows:
        has_booking = bool(r.get("booking_url", "").strip())
        engine = r.get("booking_engine", "")
        is_known = engine and engine not in ["unknown", "unknown_third_party", "contact_only", "proprietary_or_same_domain"]
        has_contact = bool(r.get("phone_google") or r.get("phone_website") or r.get("email"))
        has_site = bool(r.get("website", "").strip())
        
        if has_booking and is_known:
            tier1_leads.append(r)
        elif has_booking:
            tier2_leads.append(r)
        elif engine == "contact_only" or (has_contact and not has_booking):
            tier3_leads.append(r)
        elif has_site:
            tier4_leads.append(r)
        else:
            tier5_leads.append(r)
    
    # ========================================================================
    # PRINT REPORT
    # ========================================================================
    filename = os.path.basename(filepath)
    
    print(f"\nANALYTICS: {filename}")
    print("=" * 50)
    
    # Overview
    print("\nOVERVIEW")
    print(f"Total hotels: {total:,}")
    print(f"With website: {len(has_website):,} ({len(has_website)/total*100:.1f}%)")
    print(f"With booking URL: {len(has_booking_url):,} ({len(has_booking_url)/total*100:.1f}%)")
    print(f"With contact info: {len(has_any_contact):,} ({len(has_any_contact)/total*100:.1f}%)")
    print(f"With errors: {len(has_error):,} ({len(has_error)/total*100:.1f}%)")
    
    # Lead Quality
    print("\nLEAD QUALITY TIERS")
    print(f"Tier 1 (Booking + Known Engine): {len(tier1_leads):,} ({len(tier1_leads)/total*100:.1f}%)")
    print(f"Tier 2 (Booking + Unknown Engine): {len(tier2_leads):,} ({len(tier2_leads)/total*100:.1f}%)")
    print(f"Tier 3 (Contact Only): {len(tier3_leads):,} ({len(tier3_leads)/total*100:.1f}%)")
    print(f"Tier 4 (Website, No Booking): {len(tier4_leads):,} ({len(tier4_leads)/total*100:.1f}%)")
    print(f"Tier 5 (No Website): {len(tier5_leads):,} ({len(tier5_leads)/total*100:.1f}%)")
    
    # Booking Engines
    print("\nBOOKING ENGINES")
    if known_engines:
        for engine, count in sorted(known_engines.items(), key=lambda x: -x[1]):
            print(f"{engine}: {count} ({count/total*100:.1f}%)")
    print(f"Unknown/Third Party: {unknown_engines} ({unknown_engines/total*100:.1f}%)")
    print(f"Proprietary/Same Domain: {proprietary} ({proprietary/total*100:.1f}%)")
    print(f"Contact Only: {contact_only} ({contact_only/total*100:.1f}%)")
    
    # Unknown Third Party Domains
    if third_party_domains:
        print("\nUNKNOWN THIRD-PARTY DOMAINS")
        for domain, count in third_party_domains.most_common(15):
            print(f"{domain}: {count}")
    
    # Errors
    if error_counts:
        print("\nERRORS")
        for error, count in error_counts.most_common(10):
            error_display = error[:50] if len(error) > 50 else error
            print(f"{error_display}: {count} ({count/total*100:.1f}%)")
    
    # Contact Info
    print("\nCONTACT INFO")
    print(f"Phone (Google): {len(has_phone_google):,} ({len(has_phone_google)/total*100:.1f}%)")
    print(f"Phone (Website): {len(has_phone_website):,} ({len(has_phone_website)/total*100:.1f}%)")
    print(f"Email: {len(has_email):,} ({len(has_email)/total*100:.1f}%)")
    
    # Detection Methods
    if detection_methods:
        print("\nDETECTION METHODS")
        for method, count in detection_methods.most_common(10):
            print(f"{method}: {count}")
    
    # Top Website Domains
    if top_website_domains:
        print("\nTOP WEBSITE DOMAINS")
        for domain, count in top_website_domains:
            if domain:
                print(f"{domain}: {count}")
    
    # Screenshots
    print("\nSCREENSHOTS")
    print(f"With screenshot: {len(has_screenshot):,} ({len(has_screenshot)/total*100:.1f}%)")
    
    # Summary
    actionable = len(tier1_leads) + len(tier2_leads) + len(tier3_leads)
    print("\n" + "=" * 50)
    print(f"ACTIONABLE LEADS (Tier 1-3): {actionable:,} ({actionable/total*100:.1f}%)")
    print("=" * 50 + "\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 sadie_analytics.py <csv_file> [csv_file2 ...]")
        print("Example: python3 sadie_analytics.py detector_output/ocean_city_leads.csv")
        sys.exit(1)
    
    for filepath in sys.argv[1:]:
        if os.path.exists(filepath):
            analyze_csv(filepath)
        else:
            print(f"File not found: {filepath}")


if __name__ == "__main__":
    main()

