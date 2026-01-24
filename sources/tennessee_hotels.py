"""
Tennessee Hotels source configuration.

Source: Google Maps scraper (archive)
Coverage: Statewide (85 cities)
Records: ~2,504 hotels
Room counts: No (pending TN ABC license data with room counts)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="tennessee_hotels",
    external_id_type="tn_hotel",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/tennessee/",
    s3_pattern="tennessee_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    # CSV format: hotel,city,address,phone,website,rating,lat,long
    columns=[
        ColumnMapping(column="hotel", field="name"),
        ColumnMapping(column="city", field="city"),
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="phone", field="phone", transform="phone"),
        ColumnMapping(column="website", field="website"),
    ],

    # External ID: name + city (no unique ID in source data)
    external_id_columns=["name", "city"],
    external_id_separator=":",

    # Defaults
    default_state="TN",
    default_category="hotel",
    default_country="USA",
    default_source="tennessee",
)
