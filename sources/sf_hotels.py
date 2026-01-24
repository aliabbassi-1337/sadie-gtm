"""
San Francisco Hotels source configuration.

Source: SF Assessor Secured Property Tax Roll 2023
Coverage: San Francisco City/County
Records: ~547 hotels/motels
Room counts: Yes (official assessor data)
Coordinates: Yes (from parcel centroids)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="sf_hotels",
    external_id_type="sf_parcel",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/california/",
    s3_pattern="sf_hotels_with_coords.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="parcel_number", field="external_id"),
        ColumnMapping(column="name", field="name"),  # Address as name until enriched
        ColumnMapping(column="address", field="address"),
        ColumnMapping(column="neighborhood", field="county"),  # Use neighborhood as county
        ColumnMapping(column="category", field="category"),
        ColumnMapping(column="room_count", field="room_count", transform="int"),
        ColumnMapping(column="lat", field="lat", transform="float"),
        ColumnMapping(column="lon", field="lon", transform="float"),
    ],

    # External ID from parcel_number
    external_id_columns=["external_id"],

    # Defaults
    default_state="CA",
    default_category="hotel",
    default_country="USA",
    default_source="sf_assessor",
)
