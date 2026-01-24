"""
Data Source Configurations.

Each source is defined as a CSVIngestorConfig. To add a new source:

1. Create a new file in this directory (e.g., `georgia.py`)
2. Define a `CONFIG` variable with your CSVIngestorConfig
3. Create a workflow file or use the generic ingest workflow

Example (sources/georgia.py):
```python
from services.ingestor.config import CSVIngestorConfig, ColumnMapping

CONFIG = CSVIngestorConfig(
    name="georgia_hotels",
    external_id_type="ga_hotel_license",
    source_type="s3",
    s3_bucket="sadie-gtm",
    s3_prefix="data/georgia/",
    columns=[
        ColumnMapping(column="LICENSE_NO", field="external_id"),
        ColumnMapping(column="NAME", field="name"),
        ColumnMapping(column="CITY", field="city"),
        ColumnMapping(column="STATE", field="state"),
        ColumnMapping(column="PHONE", field="phone", transform="phone"),
        ColumnMapping(column="ROOMS", field="room_count", transform="int"),
    ],
    external_id_columns=["external_id"],
    default_category="hotel",
)
```

Then ingest:
```bash
uv run python -m workflows.ingest_csv --source georgia
```
"""

from typing import Optional
from services.ingestor.config import CSVIngestorConfig


def get_source_config(name: str) -> Optional[CSVIngestorConfig]:
    """
    Load a source config by name.

    Args:
        name: Source name (e.g., "georgia" loads from sources/georgia.py)

    Returns:
        CSVIngestorConfig or None if not found
    """
    try:
        module = __import__(f"sources.{name}", fromlist=["CONFIG"])
        return getattr(module, "CONFIG", None)
    except ImportError:
        return None


def list_sources() -> list:
    """List all available source configs."""
    import pkgutil
    import sources

    names = []
    for importer, modname, ispkg in pkgutil.iter_modules(sources.__path__):
        if not ispkg and modname != "__init__":
            names.append(modname)
    return sorted(names)
