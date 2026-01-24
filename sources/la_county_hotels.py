"""
LA County Hotels source configuration.

Source: LA County Environmental Health Housing Inspections
Coverage: Los Angeles County, California
Records: ~1,637 hotels
Room counts: Yes (ranges: 6-10, 11-20, 21-50, 51-100, 101+)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="la_county_hotels",
    external_id_type="la_county_facility",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/california/",
    s3_pattern="la_county_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="facility_id", field="external_id"),
        ColumnMapping(column="name", field="name"),
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="city", field="city"),
        ColumnMapping(column="zip", field="zip_code"),
        ColumnMapping(column="type", field="category", transform="lower"),
        # Use rooms_min as conservative estimate (actual count is between min and max)
        ColumnMapping(column="rooms_min", field="room_count", transform="int"),
    ],

    # External ID from facility_id
    external_id_columns=["external_id"],

    # Defaults - LA County is all California
    default_state="CA",
    default_county="Los Angeles",
    default_category="hotel",
    default_country="USA",
    default_source="la_county",
)
