"""Step 2 of the scheduling plan: rebuild clean + warehouse from raw alone.

Why this matters: raw is the only layer that cannot be re-created. Clean and the warehouse are always
re-creatable from it. A GitHub runner starts empty every run, so each run begins by replaying the saved
raw files into a fresh warehouse. This module does exactly that, and can prove it matches an existing one.

Rules:
  - Raw files are only READ, never changed.
  - Files replay in the order they were FETCHED (from each file's .meta.json), so "newer copy wins"
    gives the same answer it did live, whatever the files are called.
  - The new warehouse is built beside the old one and swapped in only if every file succeeded.
  - An existing warehouse is never overwritten unless you pass --replace.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from datalake.warehousing import DEFAULT_WAREHOUSE, _check_name


def _handlers() -> dict:
    """(source, dataset) -> the function that cleans one raw file and adds it to the warehouse.

    Imported here (not at the top) so a new connector only needs one more line in this table.
    """
    from datalake.connectors import gaia, gfw

    return {
        ("gfw", "gap-events"): gfw.process_raw,
        ("gaia", "nearby-stars"): gaia.process_raw,
        ("gaia", "star-errors"): gaia.process_star_errors_raw,
    }


def _fetched_at(raw_file: Path) -> datetime:
    """When this raw file was fetched: from its sidecar, else midnight UTC of its date folder."""
    sidecar = raw_file.with_name(raw_file.name + ".meta.json")
    try:
        return datetime.fromisoformat(json.loads(sidecar.read_text(encoding="utf-8"))["fetched_at_utc"])
    except (OSError, KeyError, ValueError):
        folder_date = raw_file.parent.name  # YYYY-MM-DD
        try:
            return datetime.strptime(folder_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)


def find_raw_files(lake_root: Path = Path("lake")) -> list[Path]:
    """Every raw data file under <lake>/raw/<source>/<dataset>/<date>/, oldest fetch first."""
    raw_root = lake_root / "raw"
    files = [
        p for p in raw_root.glob("*/*/*/*")
        if p.is_file() and not p.name.endswith(".meta.json") and p.name != ".gitkeep"
    ]
    return sorted(files, key=lambda p: (_fetched_at(p), str(p)))


@dataclass
class RebuildResult:
    target: Path
    files: int = 0  # raw files replayed
    tables: dict[str, int] = field(default_factory=dict)  # "schema.table" -> rows afterwards
    skipped: list[str] = field(default_factory=list)  # raw files nobody knows how to handle yet


def _remove_db(path: Path) -> None:
    for p in (path, path.with_name(path.name + ".wal")):
        p.unlink(missing_ok=True)


def rebuild(lake_root: Path = Path("lake"), warehouse_path: Path = DEFAULT_WAREHOUSE, *, replace: bool = False) -> RebuildResult:
    """Recreate clean files and the warehouse from every raw file.

    Builds into a temporary file next to `warehouse_path`; only when all files succeed is it moved
    into place. If `warehouse_path` already exists, raises FileExistsError unless `replace=True`.
    """
    result = RebuildResult(target=warehouse_path)
    files = find_raw_files(lake_root)
    if not files:
        return result  # nothing to replay; leave everything as it is
    if warehouse_path.exists() and not replace:
        raise FileExistsError(
            f"{warehouse_path} already exists and will not be overwritten. "
            "Pass --replace to swap it for the rebuilt one, or choose a different --warehouse path."
        )

    handlers = _handlers()
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = warehouse_path.with_name(warehouse_path.name + ".rebuilding")
    _remove_db(tmp)  # a leftover from a crashed run is ours to clear
    try:
        for raw_file in files:
            source, dataset = raw_file.parts[-4], raw_file.parts[-3]
            handler = handlers.get((source, dataset))
            if handler is None:
                result.skipped.append(f"{raw_file} (no handler for {source}/{dataset} yet)")
                continue
            try:
                summary = handler(raw_file, lake_root=lake_root, warehouse_path=tmp)
            except Exception as e:  # name the file, keep the real cause attached
                raise RuntimeError(f"Rebuild failed on {raw_file}: {e}") from e
            result.files += 1
            result.tables[summary.table] = summary.table_total
        os.replace(tmp, warehouse_path)
    except BaseException:
        _remove_db(tmp)
        raise
    return result


@dataclass
class TableComparison:
    table: str
    rows_a: int
    rows_b: int
    only_in_a: int  # rows A has that B lacks (or holds with different values)
    only_in_b: int

    @property
    def same(self) -> bool:
        return self.only_in_a == 0 and self.only_in_b == 0 and self.rows_a == self.rows_b


def compare_warehouses(a: Path, b: Path, tables: list[str]) -> list[TableComparison]:
    """Compare tables in two warehouses row by row (read-only). Same columns, same values = identical."""
    out = []
    con = duckdb.connect(str(a), read_only=True)
    try:
        quoted = str(b).replace("'", "''")  # ATTACH cannot take a parameter, so quote the path ourselves
        con.execute(f"ATTACH '{quoted}' AS other (READ_ONLY)")
        for table in tables:
            schema, name = table.split(".")
            _check_name(schema), _check_name(name)
            cols = ", ".join(f'"{r[0]}"' for r in con.execute(f"DESCRIBE {table}").fetchall())
            rows_a = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            rows_b = con.execute(f"SELECT count(*) FROM other.{table}").fetchone()[0]
            only_a = con.execute(
                f"SELECT count(*) FROM (SELECT {cols} FROM {table} EXCEPT SELECT {cols} FROM other.{table})"
            ).fetchone()[0]
            only_b = con.execute(
                f"SELECT count(*) FROM (SELECT {cols} FROM other.{table} EXCEPT SELECT {cols} FROM {table})"
            ).fetchone()[0]
            out.append(TableComparison(table, rows_a, rows_b, only_a, only_b))
    finally:
        con.close()
    return out


def main(argv: list[str] | None = None) -> int:
    """Command line.

    Safe check (writes a new file, touches nothing else):
      python -m datalake.rebuild --warehouse warehouse/rebuilt.duckdb --compare warehouse/warehouse.duckdb
    Replace the real warehouse with a rebuilt one:
      python -m datalake.rebuild --replace
    Exit code 1 if --compare finds any difference.
    """
    import argparse

    p = argparse.ArgumentParser(description="Rebuild clean files and the warehouse from raw files alone.")
    p.add_argument("--lake", default="lake", help="lake folder holding raw/ (default: lake)")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE), help="where to write the rebuilt warehouse")
    p.add_argument("--replace", action="store_true", help="allow replacing an existing warehouse file")
    p.add_argument("--compare", help="after rebuilding, compare against this existing warehouse")
    a = p.parse_args(argv)

    lake, target = Path(a.lake), Path(a.warehouse)
    try:
        result = rebuild(lake, target, replace=a.replace)
    except FileExistsError as e:
        raise SystemExit(str(e))
    if result.files == 0:
        print(f"No raw files found under {lake / 'raw'}. Nothing to rebuild.")
        return 0
    print(f"Replayed {result.files} raw file(s), oldest fetch first, into {target}")
    for table, rows in result.tables.items():
        print(f"  {table}: {rows} rows")
    for note in result.skipped:
        print(f"  skipped: {note}")

    code = 0
    if a.compare:
        print(f"Comparing with {a.compare}:")
        for d in compare_warehouses(Path(a.compare), target, list(result.tables)):
            if d.same:
                print(f"  {d.table}: identical ({d.rows_a} rows)")
            else:
                code = 1
                print(f"  {d.table}: DIFFERENT. existing has {d.rows_a} rows, rebuilt has {d.rows_b}; "
                      f"{d.only_in_a} only in existing, {d.only_in_b} only in rebuilt")
    return code


if __name__ == "__main__":
    import sys

    sys.exit(main())
