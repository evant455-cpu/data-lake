"""Lesson 3: clean -> warehouse.

Rule of the warehouse layer: curated tables you can ask questions of with SQL.
Like clean, it is re-creatable: if the code improves, reload from clean.
One warehouse file, one schema (named section) per domain, e.g. `ocean`, `astro`.
"""
from __future__ import annotations

import re
from pathlib import Path

import duckdb

DEFAULT_WAREHOUSE = Path("warehouse") / "warehouse.duckdb"

# SQL lets us pass *values* safely as parameters, but not *names* (schema and
# table names). So we check names ourselves: letters, digits, underscores only.
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_name(name: str) -> str:
    if not _NAME.match(name):
        raise ValueError(f"Invalid name (use letters, digits, underscores): {name!r}")
    return name


def load_table(
    parquet_file: Path,
    *,
    schema: str,
    table: str,
    warehouse_path: Path = DEFAULT_WAREHOUSE,
) -> int:
    """Load a clean Parquet file into <schema>.<table> in the warehouse.

    Creates the schema if needed and replaces the table if it already exists
    (safe, because the table can always be rebuilt from clean).
    Returns the number of rows loaded.
    """
    _check_name(schema)
    _check_name(table)
    if not parquet_file.exists():
        raise FileNotFoundError(f"Clean file not found: {parquet_file}")

    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM read_parquet(?)",
            [str(parquet_file)],
        )
        return con.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
    finally:
        con.close()


def query(sql: str, warehouse_path: Path = DEFAULT_WAREHOUSE) -> list[tuple]:
    """Run a SQL question against the warehouse and return the rows.

    Opens the warehouse read-only, so a question can never change the data.
    """
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
