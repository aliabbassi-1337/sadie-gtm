"""
New South Wales (Australia) Hotels source configuration.

Source: NSW Liquor & Gaming licensed premises list
Coverage: New South Wales statewide
Records: ~2,877 hotels/accommodation venues
Room counts: No
Coordinates: Yes
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="nsw_hotels",
    external_id_type="nsw_liquor",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/australia/nsw/",
    s3_pattern="nsw_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="Licence number", field="external_id"),
        ColumnMapping(column="Licence name", field="name"),
        ColumnMapping(column="Address", field="address"),
        ColumnMapping(column="Suburb", field="city"),
        ColumnMapping(column="Postcode", field="zip_code"),
        ColumnMapping(column="LGA", field="county"),  # Local Government Area
        ColumnMapping(column="Business type", field="category"),
        ColumnMapping(column="Latitude", field="lat", transform="float"),
        ColumnMapping(column="Longitude", field="lon", transform="float"),
    ],

    # External ID from licence number
    external_id_columns=["external_id"],

    # Defaults
    default_state="NSW",
    default_category="hotel",
    default_country="Australia",
    default_source="nsw_liquor",
)
