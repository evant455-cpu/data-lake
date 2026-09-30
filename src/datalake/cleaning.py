"""Lesson 2: raw -> clean.

Rule of the clean layer: tidy, typed, and always re-creatable from raw.
Unlike raw, clean files MAY be overwritten, because if our cleaning code
improves we simply re-run it from raw.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# The column types we allow. Each maps to a function that parses text -> value.
# A parser raises ValueError when the text is not valid for that type.


def _parse_timestamp(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _parse_bool(text: str) -> bool:
    lowered = text.lower()
    if lowered in ("true", "1"):
        return True
    if lowered in ("false", "0"):
        return False
    raise ValueError(f"not a boolean: {text!r}")


PARSERS = {
    "string": str,
    "bool": _parse_bool,
    "int": int,
    "float": float,
    "timestamp": _parse_timestamp,
}

ARROW_TYPES = {
    "string": pa.string(),
    "bool": pa.bool_(),
    "int": pa.int64(),
    "float": pa.float64(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}


@dataclass
class CleanReport:
    """What happened during cleaning, so nothing is silently lost."""

    path: Path
    rows_in: int
    rows_out: int
    bad_values: dict[str, int]  # column -> how many values could not be parsed


def _check_types(types: dict[str, str]) -> None:
    unknown = set(types.values()) - set(PARSERS)
    if unknown:
        raise ValueError(f"Unknown column types: {sorted(unknown)}")


def _raw_location(raw_file: Path, lake_root: Path) -> tuple[str, str, str]:
    """raw path looks like <lake_root>/raw/<source>/<dataset>/<date>/<file>"""
    parts = raw_file.relative_to(lake_root / "raw").parts
    return parts[0], parts[1], parts[2]


def _write_clean(
    raw_file: Path,
    lake_root: Path,
    columns: dict[str, list],
    types: dict[str, str],
    bad_values: dict[str, int],
    rows_in: int,
) -> CleanReport:
    """Turn collected columns into a typed table and save it as Parquet in lake/clean/."""
    source, dataset, date = _raw_location(raw_file, lake_root)
    table = pa.table({name: pa.array(values, type=ARROW_TYPES[types[name]]) for name, values in columns.items()})
    out_dir = lake_root / "clean" / source / dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date}.parquet"
    pq.write_table(table, out_path)
    return CleanReport(path=out_path, rows_in=rows_in, rows_out=table.num_rows, bad_values=bad_values)


def clean_csv(
    raw_file: Path,
    *,
    schema: dict[str, str],
    lake_root: Path = Path("lake"),
) -> CleanReport:
    """Read a raw CSV and write a typed Parquet file into lake/clean/.

    `schema` says which columns to keep and their types, e.g.
    {"name": "string", "depth_m": "float", "seen_at": "timestamp"}.

    Tidying rules:
      - whitespace around values is trimmed
      - empty values become null (missing), not empty text
      - a value that does not fit its type becomes null and is counted
        in the report (we never crash on one bad cell, and never hide it)
      - columns not in the schema are dropped
    """
    _check_types(schema)

    columns: dict[str, list] = {name: [] for name in schema}
    bad_values = {name: 0 for name in schema}
    rows_in = 0

    with raw_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(schema) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Columns missing from raw file: {sorted(missing)}")
        for row in reader:
            rows_in += 1
            for name, type_name in schema.items():
                text = (row[name] or "").strip()
                if text == "":
                    columns[name].append(None)
                    continue
                try:
                    columns[name].append(PARSERS[type_name](text))
                except ValueError:
                    columns[name].append(None)
                    bad_values[name] += 1

    return _write_clean(raw_file, lake_root, columns, schema, bad_values, rows_in)


_MISSING = object()


def _dig(record: dict, path: str):
    """Follow a dotted path like "vessel.id" into nested dicts. Returns _MISSING if absent."""
    value = record
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            return _MISSING
        value = value[key]
    return value


def clean_json(
    raw_file: Path,
    *,
    schema: dict[str, tuple[str, str]],
    lake_root: Path = Path("lake"),
) -> CleanReport:
    """Read a raw JSON file (a list of records) and write typed Parquet into lake/clean/.

    `schema` maps each output column to (type, path), where the path digs into
    nested records with dots, e.g.
    {"vessel_id": ("string", "vessel.id"), "hours": ("float", "gap.duration_hours")}.

    Same tidy rules as clean_csv: whitespace trimmed, empty/absent/null -> null,
    a value that does not fit its type -> null and counted in the report.
    A path that lands on a whole nested record (not a single value) counts as bad.
    """
    types = {name: type_name for name, (type_name, _) in schema.items()}
    _check_types(types)

    records = json.loads(raw_file.read_text(encoding="utf-8"))
    if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
        raise ValueError("Raw JSON must be a list of records (objects)")

    columns: dict[str, list] = {name: [] for name in schema}
    bad_values = {name: 0 for name in schema}

    for record in records:
        for name, (type_name, path) in schema.items():
            value = _dig(record, path)
            if value is _MISSING or value is None:
                columns[name].append(None)
                continue
            if isinstance(value, (dict, list)):
                columns[name].append(None)
                bad_values[name] += 1
                continue
            text = str(value).strip()
            if text == "":
                columns[name].append(None)
                continue
            try:
                columns[name].append(PARSERS[type_name](text))
            except ValueError:
                columns[name].append(None)
                bad_values[name] += 1

    return _write_clean(raw_file, lake_root, columns, types, bad_values, len(records))
