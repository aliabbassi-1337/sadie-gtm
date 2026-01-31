"""
Hawaii Hotels source configuration.

Source: Hawaii Tourism Authority - Visitor Plant Inventory 2023
Coverage: Statewide (all islands)
Records: ~1,330 unique properties
Room counts: Yes (available units)
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="hawaii_hotels",
    external_id_type="hawaii_vpi",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/hawaii/",
    s3_pattern="hawaii_vpi_clean.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    # Columns: island,area,prop_id,name,category,room_count,...
    columns=[
        ColumnMapping(column="prop_id", field="external_id", transform="int"),
        ColumnMapping(column="name", field="name"),
        ColumnMapping(column="island", field="county"),  # Use island as county
        ColumnMapping(column="area", field="city"),  # Use area as city
        ColumnMapping(column="category", field="category", transform="lower"),
        ColumnMapping(column="room_count", field="room_count", transform="int"),
    ],

    # External ID from prop_id
    external_id_columns=["external_id"],

    # Defaults
    default_state="HI",
    default_category="hotel",
    default_country="United States",
    default_source="hawaii_vpi",
)
