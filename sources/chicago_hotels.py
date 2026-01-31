"""
Chicago Hotels source configuration.

Source: City of Chicago Business Licensing / Chicago Data Portal
Coverage: City of Chicago
Records: ~624 hotels with active licenses
Room counts: No
Coordinates: Yes
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="chicago_hotels",
    external_id_type="chicago_license",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/chicago/",
    s3_pattern="chicago_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="license_id", field="external_id"),
        ColumnMapping(column="doing_business_as_name", field="name"),
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="city", field="city"),
        ColumnMapping(column="zip_code", field="zip_code"),
        ColumnMapping(column="neighborhood", field="county"),
        ColumnMapping(column="latitude", field="lat", transform="float"),
        ColumnMapping(column="longitude", field="lon", transform="float"),
    ],

    # External ID from license_id
    external_id_columns=["external_id"],

    # Defaults
    default_state="IL",
    default_category="hotel",
    default_country="United States",
    default_source="chicago_license",
)
