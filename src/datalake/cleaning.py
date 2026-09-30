"""Lesson 2: raw -> clean.

Rule of the clean layer: tidy, typed, and always re-creatable from raw.
Unlike raw, clean files MAY be overwritten, because if our cleaning code
improves we simply re-run it from raw.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# The column types we allow. Each maps to a function that parses text -> value.
# A parser raises ValueError when the text is not valid for that type.


def _parse_timestamp(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


PARSERS = {
    "string": str,
    "int": int,
    "float": float,
    "timestamp": _parse_timestamp,
}

ARROW_TYPES = {
    "string": pa.string(),
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
    unknown = set(schema.values()) - set(PARSERS)
    if unknown:
        raise ValueError(f"Unknown column types: {sorted(unknown)}")

    # raw path looks like <lake_root>/raw/<source>/<dataset>/<date>/<file>
    parts = raw_file.relative_to(lake_root / "raw").parts
    source, dataset, date = parts[0], parts[1], parts[2]

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

    table = pa.table(
        {name: pa.array(values, type=ARROW_TYPES[schema[name]]) for name, values in columns.items()}
    )

    out_dir = lake_root / "clean" / source / dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date}.parquet"
    pq.write_table(table, out_path)

    return CleanReport(path=out_path, rows_in=rows_in, rows_out=table.num_rows, bad_values=bad_values)
