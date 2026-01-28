"""
NYC Hotels source configuration.

Source: NYC Department of Finance / NYC OpenData
Coverage: All 5 boroughs (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
Records: ~14,010 hotel properties
Room counts: No
Coordinates: Yes
Notes: Has building class, owner names, BBL. Needs Serper enrichment for hotel names/websites.
"""

from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="nyc_hotels",
    external_id_type="nyc_bbl",

    # S3 source
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="hotel-sources/us/nyc/",
    s3_pattern="nyc_hotels.csv",

    # CSV parsing
    has_header=True,
    encoding="utf-8",

    # Column mappings
    # Note: OWNER_NAME used as temporary name until enriched via coordinates
    columns=[
        ColumnMapping(column="BBL", field="external_id"),
        ColumnMapping(column="OWNER_NAME", field="name"),  # Owner name until enriched
        ColumnMapping(column="STREET NAME", field="address"),  # Just street name
        ColumnMapping(column="Postcode", field="zip_code"),
        ColumnMapping(column="Borough", field="city"),  # Borough as city
        ColumnMapping(column="BLDG_CLASS", field="category"),  # H1, H2, H3, etc.
        ColumnMapping(column="NTA Name", field="county"),  # Neighborhood as county
        ColumnMapping(column="Latitude", field="lat", transform="float"),
        ColumnMapping(column="Longitude", field="lon", transform="float"),
    ],

    # External ID from BBL
    external_id_columns=["external_id"],

    # Defaults
    default_state="NY",
    default_category="hotel",
    default_country="USA",
    default_source="nyc_dof",
)
