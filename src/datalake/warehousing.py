"""Lesson 3: clean -> warehouse.

Rule of the warehouse layer: curated tables you can ask questions of with SQL.
Like clean, it is re-creatable: if the code improves, reload from clean.
One warehouse file, one schema (named section) per domain, e.g. `ocean`, `astro`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
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


@dataclass
class AppendResult:
    """What an append did, so nothing is silently lost."""

    new: int  # rows whose key was not in the table yet
    updated: int  # rows that replaced an older row with the same key
    skipped_no_key: int  # rows with an empty key column (cannot be matched, so not loaded)
    duplicates_in_file: int  # extra copies of a key inside the file itself (last one wins)
    total: int  # rows in the table afterwards


def append_table(
    parquet_file: Path,
    *,
    schema: str,
    table: str,
    key: tuple[str, ...],
    warehouse_path: Path = DEFAULT_WAREHOUSE,
) -> AppendResult:
    """Add a clean Parquet file to <schema>.<table> without losing earlier runs (an "upsert").

    `key` names the columns that identify one row, e.g. ("vessel_id", "start").
    A row whose key is already in the table replaces the old row (the newer fetch wins);
    any other row is added. Every row gets a `_source_file` column saying which clean file
    it came from. Runs as one transaction: if anything fails, the table is left untouched.
    Safe to repeat with the same file.
    """
    _check_name(schema)
    _check_name(table)
    if not key:
        raise ValueError("key needs at least one column")
    for k in key:
        _check_name(k)
    if not parquet_file.exists():
        raise FileNotFoundError(f"Clean file not found: {parquet_file}")

    has_key = " AND ".join(f"{k} IS NOT NULL" for k in key)
    same_key = " AND ".join(f"t.{k} = i.{k}" for k in key)
    cols = ", ".join(key)
    path, source = str(parquet_file), parquet_file.name

    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse_path))
    try:
        file_cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()]
        missing = [k for k in key if k not in file_cols]
        if missing:
            raise ValueError(f"Key columns not in the file: {missing}")

        con.execute("BEGIN")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        file_rows = con.execute("SELECT count(*) FROM read_parquet(?)", [path]).fetchone()[0]
        keyed_rows = con.execute(f"SELECT count(*) FROM read_parquet(?) WHERE {has_key}", [path]).fetchone()[0]
        # Keep one row per key: the last one in the file wins.
        con.execute(
            f"""CREATE TEMP TABLE incoming AS
                SELECT * EXCLUDE (file_row_number, rn), ? AS _source_file
                FROM (SELECT *, row_number() OVER (PARTITION BY {cols} ORDER BY file_row_number DESC) AS rn
                      FROM read_parquet(?, file_row_number = true) WHERE {has_key})
                WHERE rn = 1""",
            [source, path],
        )
        incoming = con.execute("SELECT count(*) FROM incoming").fetchone()[0]

        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()[0]
        if not exists:
            con.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM incoming")
            updated = 0
        else:
            existing = [
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchall()
            ]
            if "_source_file" not in existing and set(existing) | {"_source_file"} == set(file_cols) | {"_source_file"}:
                # Table was made by load_table (no lineage column yet): add it; old rows stay unknown (NULL).
                con.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN _source_file VARCHAR")
                existing.append("_source_file")
            if set(existing) != set(file_cols) | {"_source_file"}:
                raise ValueError(
                    f"Columns differ from {schema}.{table}. Table has {sorted(existing)}, "
                    f"file has {sorted(set(file_cols) | {'_source_file'})}. Rebuild the table from clean files."
                )
            updated = con.execute(
                f"SELECT count(*) FROM incoming i WHERE EXISTS (SELECT 1 FROM {schema}.{table} t WHERE {same_key})"
            ).fetchone()[0]
            con.execute(f"DELETE FROM {schema}.{table} AS t WHERE EXISTS (SELECT 1 FROM incoming i WHERE {same_key})")
            con.execute(f"INSERT INTO {schema}.{table} BY NAME SELECT * FROM incoming")

        total = con.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
        con.execute("COMMIT")
        return AppendResult(
            new=incoming - updated,
            updated=updated,
            skipped_no_key=file_rows - keyed_rows,
            duplicates_in_file=keyed_rows - incoming,
            total=total,
        )
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        con.close()
