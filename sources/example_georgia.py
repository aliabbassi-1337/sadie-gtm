"""
Example: Georgia Hotels source configuration.

This is a TEMPLATE showing how to define a new data source.
Copy this file and modify for your actual data source.
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

# Define the source configuration
CONFIG = CSVIngestorConfig(
    # Unique identifier for this source
    name="georgia_hotels",
    external_id_type="ga_hotel_license",

    # Where the data lives
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="data/georgia/",
    s3_pattern="*.csv",

    # CSV parsing options
    has_header=True,
    encoding="utf-8",
    delimiter=",",

    # Map CSV columns to record fields
    columns=[
        ColumnMapping(column="LICENSE_NO", field="external_id"),
        ColumnMapping(column="BUSINESS_NAME", field="name"),
        ColumnMapping(column="STREET_ADDRESS", field="address"),
        ColumnMapping(column="CITY", field="city"),
        ColumnMapping(column="STATE", field="state"),
        ColumnMapping(column="ZIP", field="zip_code"),
        ColumnMapping(column="COUNTY", field="county"),
        ColumnMapping(column="PHONE", field="phone", transform="phone"),
        ColumnMapping(column="ROOM_COUNT", field="room_count", transform="int"),
    ],

    # How to build the external_id (for deduplication)
    external_id_columns=["external_id"],
    external_id_separator=":",

    # Defaults applied to all records
    default_category="hotel",
    default_country="United States",

    # Optional: only include records from this state
    state_filter="GA",
)
