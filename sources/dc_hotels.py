"""
Washington DC Hotels source configuration.

Source: DC Office of the Chief Technology Officer (OCTO) / DC GIS
Coverage: District of Columbia
Records: 169 hotels
Room counts: Yes
Coordinates: Yes
Websites: Yes (already enriched)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="dc_hotels",
    external_id_type="dc_gis",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/dc/",
    s3_pattern="dc_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="gis_id", field="external_id"),
        ColumnMapping(column="name", field="name"),
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="zipcode", field="zip_code"),
        ColumnMapping(column="rooms", field="room_count", transform="int"),
        ColumnMapping(column="phone", field="phone"),
        ColumnMapping(column="website", field="website"),
        ColumnMapping(column="lat", field="lat", transform="float"),
        ColumnMapping(column="lon", field="lon", transform="float"),
    ],

    # External ID from gis_id
    external_id_columns=["external_id"],

    # Defaults
    default_state="DC",
    default_category="hotel",
    default_country="USA",
    default_source="dc_gis",
)
