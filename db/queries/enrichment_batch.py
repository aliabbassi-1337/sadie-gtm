"""
Batch SQL queries for enrichment operations.

Loads SQL from enrichment_batch.sql - kept separate from aiosql since
these use array parameters ($1::int[]) not supported by aiosql.
"""

from pathlib import Path

_sql_file = Path(__file__).parent / "enrichment_batch.sql"
_sql_content = _sql_file.read_text()

# Parse SQL sections from enrichment_batch.sql
_sections = _sql_content.split("-- BATCH_")
for section in _sections[1:]:
    lines = section.strip().split("\n")
    name = "BATCH_" + lines[0].strip()
    # Find the SQL (skip comment lines)
    sql_lines = [l for l in lines[1:] if not l.startswith("--")]
    sql = "\n".join(sql_lines).strip().rstrip(";")
    globals()[name] = sql
