"""
Batch SQL queries for executemany operations.

Loads SQL from batch.sql - kept separate from aiosql since executemany needs positional params.
"""

from pathlib import Path

_sql_file = Path(__file__).parent / "batch.sql"
_sql_content = _sql_file.read_text()

# Parse SQL sections from batch.sql
_sections = _sql_content.split("-- BATCH_")
for section in _sections[1:]:
    lines = section.strip().split("\n")
    name = "BATCH_" + lines[0].strip()
    # Find the SQL (skip comment lines)
    sql_lines = [l for l in lines[1:] if not l.startswith("--")]
    sql = "\n".join(sql_lines).strip().rstrip(";")
    globals()[name] = sql
