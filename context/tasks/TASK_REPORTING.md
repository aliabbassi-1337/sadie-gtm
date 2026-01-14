# Task: Reporting Service

## Overview

Implement the reporting service in `/services/reporting/`. The reporting service generates Excel exports, uploads to OneDrive, and sends notifications.

**Location:** `/services/reporting/`

## Service Functions

The service needs **5 functions**:

### 1. `export_city(city: str, state: str) -> str`

Generate Excel report for a specific city.

**Logic:**
1. Query hotels where `city = :city` and `state = :state` and `status = 5` (live)
2. Join with `hotel_booking_engines` to get booking engine info
3. Join with `hotel_room_count` to get room counts
4. Join with `hotel_customer_proximity` to get nearest customer
5. Generate Excel with 2 sheets: Leads + Stats

**Returns:** File path of generated Excel file

### 2. `export_state(state: str) -> str`

Generate Excel report for entire state.

**Logic:** Same as `export_city` but filter by state only.

**Returns:** File path of generated Excel file

### 3. `upload_to_onedrive(file_path: str) -> bool`

Upload generated report to OneDrive.

**Logic:**
1. Authenticate with Microsoft Graph API
2. Upload file to configured SharePoint folder
3. Return share link

**Returns:** True if successful

### 4. `send_slack_notification(message: str, channel: str = "#leads") -> bool`

Send notification to Slack channel.

**Logic:**
1. Use Slack webhook or API
2. Post message with export summary

**Returns:** True if successful

### 5. `get_exportable_hotels_count(city: str = None, state: str = None) -> int`

Count hotels ready for export.

**Query:**
```sql
SELECT COUNT(*) FROM hotels
WHERE status = 5  -- live
AND (:city IS NULL OR city = :city)
AND (:state IS NULL OR state = :state);
```

## Excel Export Format

### Sheet 1: Leads

Columns (in order):
| Column | Source |
|--------|--------|
| name | hotels.name |
| website | hotels.website |
| phone | hotels.phone_google OR phone_website |
| email | hotels.email |
| city | hotels.city |
| state | hotels.state |
| booking_engine | booking_engines.name |
| booking_url | hotel_booking_engines.booking_url |
| room_count | hotel_room_count.room_count |
| nearest_customer | existing_customers.name |
| distance_km | hotel_customer_proximity.distance_km |

**Exclude from export:**
- latitude, longitude (internal use)
- detection_method (technical detail)
- error columns

### Sheet 2: Stats (Dashboard)

Key metrics:
- Total leads
- Leads with booking URL (Tier 1 + 2)
- Leads with known engine (Tier 1)
- Leads with phone/email
- Top 10 booking engines breakdown
- Funnel conversion rates (if scraper data available)

## Database Queries

### Get Exportable Hotels
```sql
SELECT
    h.id, h.name, h.website,
    COALESCE(h.phone_google, h.phone_website) AS phone,
    h.email, h.city, h.state,
    be.name AS booking_engine,
    hbe.booking_url,
    hrc.room_count,
    ec.name AS nearest_customer,
    hcp.distance_km
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.status = 5
AND (:city IS NULL OR h.city = :city)
AND (:state IS NULL OR h.state = :state)
ORDER BY h.city, h.name;
```

### After Export
```sql
UPDATE hotels SET status = 6, updated_at = NOW()
WHERE id = ANY(:hotel_ids);
```

Status 6 = exported

## Implementation Notes

- Use openpyxl for Excel generation
- Excel styling: header row with green fill, auto-width columns, frozen header
- OneDrive uses Microsoft Graph API (need Azure AD app registration)
- Slack uses incoming webhook (simpler) or Bot API

## Existing Scripts

Reference implementations in `/scripts/pipeline/`:
- `export_excel.py` - Excel generation with stats sheet
- `export_hubspot.py` - HubSpot CRM format (alternative export)

## Files to Create/Modify

```
services/reporting/
├── service.py          # Implement the 5 functions
├── repo.py             # NEW - Database queries
├── excel_generator.py  # NEW - Excel generation logic
├── onedrive.py         # NEW - OneDrive upload (optional)
├── slack.py            # NEW - Slack notifications (optional)
└── service_test.py     # Add tests
```

## Example Usage

```python
from services.reporting import service

svc = service.Service()

# Check exportable count
count = await svc.get_exportable_hotels_count(state="florida")
print(f"{count} hotels ready for export")

# Generate Excel
file_path = await svc.export_state("florida")
print(f"Generated: {file_path}")

# Upload and notify
await svc.upload_to_onedrive(file_path)
await svc.send_slack_notification(
    f"New Florida export: {count} leads",
    channel="#sales-leads"
)
```

## Export Workflow

Typical end-to-end flow:

1. **Manual review** - Sales reviews hotels at status=3 (enriched), marks good ones as status=5 (live)
2. **Export** - Run `export_state("florida")` to generate Excel
3. **Upload** - Upload to OneDrive shared folder
4. **Notify** - Post to Slack with summary
5. **Mark exported** - Update status to 6

The service handles steps 2-5. Step 1 is manual or via a separate review UI.
