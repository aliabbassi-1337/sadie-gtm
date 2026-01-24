"""
Maryland Hotels source configuration.

Source: Maryland Department of Planning / SDAT CAMA Data
Coverage: Statewide (all 24 counties)
Records: ~1,274 hotels/motels/B&Bs
Room counts: Yes (from property assessment data)
Coordinates: Yes (lat/lon from parcel data)
Note: No hotel names - uses use_description as temporary name until enriched
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="maryland_hotels",
    external_id_type="md_parcel",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/maryland/",
    s3_pattern="maryland_hotels_statewide.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    columns=[
        ColumnMapping(column="account_id", field="external_id"),
        ColumnMapping(column="use_description", field="name"),  # Use description as name until enriched
        ColumnMapping(column="jurisdiction", field="county"),  # County code (e.g., "ALLE" = Allegany)
        ColumnMapping(column="rooms", field="room_count", transform="int"),
        ColumnMapping(column="lat", field="lat", transform="float"),
        ColumnMapping(column="lon", field="lon", transform="float"),
    ],

    # External ID from account_id (parcel ID)
    external_id_columns=["external_id"],

    # Defaults
    default_state="MD",
    default_category="hotel",
    default_country="USA",
    default_source="md_sdat_cama",
)
