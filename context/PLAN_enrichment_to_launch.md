# Plan: Enrich Hotels to Launch

**Date:** 2026-01-29
**Status:** In Progress

## Goal

Get 16,766 pending hotels fully enriched so they can be launched.

## Launch Requirements

All fields required:
- Name (not 'Unknown') ✅ 99% done
- Email OR Phone
- City
- State
- Country ✅ 93% done
- Booking engine ✅ 100% done

## Current Gaps

| Field | Have | Missing | % Missing |
|-------|------|---------|-----------|
| Contact (email/phone) | 7,314 | 15,312 | 68% |
| City | 8,599 | 14,027 | 62% |
| State | 6,130 | 16,496 | 73% |

---

## Phase 1: Scrape Booking Pages

Booking pages often contain hotel contact info and location.

### 1.1 Cloudbeds Enrichment
- **Target**: Hotels with Cloudbeds booking URLs missing contact/location
- **Method**: Playwright scraper extracts from booking page
- **Data available**: Name, city, state, country, phone, email

### 1.2 RMS Cloud Enrichment  
- **Target**: Hotels with RMS booking URLs missing contact/location
- **Method**: Playwright scraper with correct URL format
- **Data available**: Name, city, state, phone, email

### 1.3 Mews Enrichment
- **Target**: Hotels with Mews booking URLs missing contact/location
- **Method**: Playwright scraper
- **Data available**: Name, location

### 1.4 SiteMinder Enrichment
- **Target**: Hotels with SiteMinder booking URLs
- **Method**: Limited - SiteMinder pages are SPAs with minimal data
- **Data available**: Name only (already done)

---

## Phase 2: Geocoding

Use existing location data to fill city/state.

### 2.1 Reverse Geocode from Coordinates
- **Target**: 508 hotels with coordinates but missing city/state
- **Method**: Nominatim or Google Geocoding API
- **Output**: City, state, country

### 2.2 Parse from Address
- **Target**: 8,275 hotels with address but missing city/state
- **Method**: Address parsing + geocoding
- **Output**: City, state, country

---

## Phase 3: Serper Enrichment

Last resort for hotels still missing data.

### 3.1 Search for Hotel Info
- **Target**: Hotels with name + website but missing contact
- **Method**: Serper API search for hotel contact info
- **Output**: Phone, email, address

---

## Execution Order

1. [ ] Phase 1.1: Cloudbeds booking page scraping
2. [ ] Phase 1.2: RMS booking page scraping
3. [ ] Phase 1.3: Mews booking page scraping
4. [ ] Phase 2.1: Reverse geocode from coordinates
5. [ ] Phase 2.2: Parse/geocode from address
6. [ ] Phase 3.1: Serper enrichment for remaining
7. [ ] Final: Launch all fully enriched hotels

---

## Progress Tracking

Will update after each phase with:
- Hotels processed
- Fields filled
- New launch-ready count
