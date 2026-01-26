"""
San Francisco Business Registry Hotels source configuration.

Source: SF Open Data - Registered Business Locations
URL: https://data.sfgov.org/api/views/g8m3-pdis
Coverage: San Francisco City/County
Records: ~564 hotels (filtered from NAICS 7210-7219 and Transient Occupancy Tax)
Room counts: No
Coordinates: Yes (98.9% coverage)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="sf_business",
    external_id_type="sf_business_id",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/california/",
    s3_pattern="sf_business_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="external_id", field="external_id"),
        ColumnMapping(column="name", field="name"),
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="city", field="city"),
        ColumnMapping(column="zip_code", field="zip_code"),
        ColumnMapping(column="neighborhood", field="county"),
        ColumnMapping(column="lat", field="lat", transform="float"),
        ColumnMapping(column="lon", field="lon", transform="float"),
    ],

    # External ID from business account number
    external_id_columns=["external_id"],

    # Defaults
    default_state="CA",
    default_category="hotel",
    default_country="USA",
    default_source="sf_business",
)
