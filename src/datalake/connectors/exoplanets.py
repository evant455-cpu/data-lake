"""Lesson 7: the third connector, for the NASA Exoplanet Archive (confirmed planets).

Same shape as the other two: fetch -> raw -> clean -> warehouse (`astro.exoplanets`). It asks the archive
a SQL-like question over HTTP (TAP, like Gaia) and gets CSV back, so the existing `clean_csv` does the
cleaning. No account or token. Nothing in the lake, cleaning or warehouse code had to change; the only
edits elsewhere are the two registration lines (rebuild and quality).

What is different from the stars, and why it matters:
  - The whole table is small (thousands of planets), so one fetch is a SNAPSHOT of everything. There is no
    slicing and no row limit that we know of. Instead of guessing a limit, we ASK the archive how many
    planets it has (a second, tiny question) and compare. Fewer rows than that = incomplete.
  - The archive revises planets as new papers appear, so every snapshot re-sends planets we already have.
    The upsert (key = planet name, newer copy wins) handles that.
  - The link to our Gaia stars is the column `gaia_dr3_id`, a piece of TEXT. We keep it exactly as the
    archive sent it and turn it into a number only when asking a question (GAIA_SOURCE_ID_SQL), so the
    measured data is never rewritten.

NOT yet run against the live archive (the sandbox cannot reach it). The column names come from the archive's
documentation; the real FORMAT of gaia_dr3_id (we expect text like "Gaia DR3 123...") is a guess to confirm
on the first live run. The `unreadable_gaia_id` check will say so if the guess is wrong.

Data: NASA Exoplanet Archive, operated by Caltech/IPAC under contract with NASA
(https://exoplanetarchive.ipac.caltech.edu). Please credit it.
"""
from __future__ import annotations

import csv
import io
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from datalake.quality import Check

EXOPLANET_SYNC_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
SOURCE = "exoplanets"
DATASET = "planets"
TABLE = "exoplanets"
ARCHIVE_TABLE = "pscomppars"  # "Planetary Systems Composite Parameters": one row per known planet

# Column -> type, for cleaning.clean_csv. One row = one planet; its name is unique in the archive.
EXOPLANET_COLUMNS = {
    "pl_name": "string",  # planet name, e.g. "Proxima Cen b"
    "hostname": "string",  # name of the star it orbits
    "gaia_dr3_id": "string",  # the host star's Gaia number, as TEXT exactly as sent (our link to astro.nearby_stars)
    "sy_dist": "float",  # distance to the system, parsecs
    "sy_plx": "float",  # parallax of the system, milliarcseconds
    "sy_gaiamag": "float",  # brightness of the star in Gaia's G band
    "sy_pnum": "int",  # how many known planets the system has
    "disc_year": "int",  # year of discovery
    "discoverymethod": "string",  # e.g. "Transit", "Radial Velocity"
    "pl_orbper": "float",  # orbital period, days
    "pl_bmasse": "float",  # planet mass in Earth masses (for Radial Velocity planets this is a MINIMUM mass, m sin i)
    "pl_rade": "float",  # planet radius in Earth radii
}
EXOPLANET_KEY = ("pl_name",)

# The text "Gaia DR3 4472832130942575872" -> the number 4472832130942575872 (null if there is no number at the end).
# This is SQL text to drop into a question, e.g.:  JOIN astro.nearby_stars s ON s.source_id = {GAIA_SOURCE_ID_SQL}
GAIA_SOURCE_ID_SQL = r"TRY_CAST(NULLIF(regexp_extract(gaia_dr3_id, '([0-9]+)\s*$', 1), '') AS BIGINT)"

# Data-quality rules (flag only, never remove). A planet matching the condition is flagged.
EXOPLANET_CHECKS = [
    Check("no_host_star", "astro.exoplanets", "hostname IS NULL", "error",
          "Every planet orbits a star, so the host name must be there."),
    Check("non_positive_orbit", "astro.exoplanets", "pl_orbper <= 0", "error",
          "An orbit cannot take zero or negative time."),
    Check("non_positive_size", "astro.exoplanets", "pl_rade <= 0 OR pl_bmasse <= 0", "error",
          "A planet's radius and mass must be above zero."),
    Check("no_gaia_id", "astro.exoplanets", "gaia_dr3_id IS NULL", "warning",
          "No Gaia number for the host star, so this planet cannot be matched to our star table."),
    Check("unreadable_gaia_id", "astro.exoplanets", f"gaia_dr3_id IS NOT NULL AND {GAIA_SOURCE_ID_SQL} IS NULL", "warning",
          "The Gaia number is not in the form we expect, so the link to our stars will miss this planet."),
]


def exoplanets_query() -> str:
    """The question: every planet, our columns. No row limit: we check the count afterwards instead."""
    return f"SELECT {', '.join(EXOPLANET_COLUMNS)} FROM {ARCHIVE_TABLE} ORDER BY pl_name"


def count_query() -> str:
    return f"SELECT count(*) FROM {ARCHIVE_TABLE}"


def fetch_tap_csv(query: str) -> bytes:
    """Ask the real archive one question and return the CSV reply as bytes."""
    url = EXOPLANET_SYNC_URL + "?" + urllib.parse.urlencode({"query": query, "format": "csv"})
    with urllib.request.urlopen(url, timeout=120) as reply:
        return reply.read()


@dataclass
class ExoplanetFetch:
    """What a fetch brought back, including whether we know it is everything."""

    path: Path  # the raw CSV file
    rows: int
    complete: bool
    warning: str | None = None
    expected_rows: int | None = None  # what the archive said it has (None = could not find out)


def _count_rows(payload: bytes) -> int:
    """Count data rows, and make sure the reply really is a CSV table with our columns.

    A failed query comes back as an XML error message, not a CSV. We must never land that as data.
    """
    rows = list(csv.reader(io.StringIO(payload.decode("utf-8-sig"))))
    if not rows or "pl_name" not in rows[0]:
        start = payload[:200].decode("utf-8", "replace").strip()
        raise RuntimeError(f"Archive reply is not a CSV table (query error?). It starts with: {start!r}")
    return len(rows) - 1


def _expected_count(payload: bytes) -> int | None:
    """The archive's own planet count from the reply to count_query(), or None if we cannot read it."""
    try:
        rows = list(csv.reader(io.StringIO(payload.decode("utf-8-sig"))))
        return int(rows[1][0])
    except (ValueError, IndexError, UnicodeDecodeError):
        return None


def land_exoplanets(fetch=fetch_tap_csv, *, lake_root: Path = Path("lake")) -> ExoplanetFetch:
    """Fetch every confirmed planet and land the CSV untouched in the raw layer.

    `fetch` takes the question text and returns bytes. It is passed in (like the other connectors) so tests
    can give a fake one and never touch the network.
    """
    from datalake.landing import land_raw

    payload = fetch(exoplanets_query())
    rows = _count_rows(payload)  # raises before anything is landed if the reply is not a table
    expected = _expected_count(fetch(count_query()))
    if expected is None:
        complete, warning = False, "Could not verify the planet count with the archive, so we cannot say this is everything."
    elif rows < expected:
        complete, warning = False, f"Got {rows} planets but the archive says it has {expected}."
    else:
        complete, warning = True, None  # more rows than the count = a planet was added in between: fine
    raw = land_raw(
        payload, source=SOURCE, dataset=DATASET, filename=f"{ARCHIVE_TABLE}.csv", lake_root=lake_root,
        extra_meta={"complete": complete, "expected_rows": expected},
    )
    return ExoplanetFetch(path=raw, rows=rows, complete=complete, warning=warning, expected_rows=expected)


@dataclass
class ExoplanetSummary:
    """What a full run did, printed at the end so nothing is hidden."""

    raw_path: Path
    clean_path: Path
    table: str
    bad_values: dict[str, int]
    new: int = 0
    updated: int = 0
    skipped_no_key: int = 0
    duplicates_in_file: int = 0
    table_total: int = 0
    rows_fetched: int = 0  # 0 for --raw
    complete: bool = True
    warning: str | None = None


def process_raw(
    raw_file: Path,
    *,
    lake_root: Path | None = None,
    warehouse_path: Path | None = None,
    schema: str = "astro",
    table: str = TABLE,
) -> ExoplanetSummary:
    """Clean an already-landed raw CSV and add it to the warehouse table. No network needed."""
    from datalake.cleaning import clean_csv
    from datalake.warehousing import DEFAULT_WAREHOUSE, append_table

    if lake_root is None:
        if len(raw_file.parents) < 5 or raw_file.parents[3].name != "raw":
            raise ValueError(f"Expected <lake>/raw/<source>/<dataset>/<date>/<file>, got: {raw_file}")
        lake_root = raw_file.parents[4]
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    report = clean_csv(raw_file, schema=EXOPLANET_COLUMNS, lake_root=lake_root)
    res = append_table(
        report.path, schema=schema, table=table, key=EXOPLANET_KEY,
        warehouse_path=warehouse_path or DEFAULT_WAREHOUSE,
    )
    return ExoplanetSummary(
        raw_path=raw_file, clean_path=report.path, table=f"{schema}.{table}", bad_values=report.bad_values,
        new=res.new, updated=res.updated, skipped_no_key=res.skipped_no_key,
        duplicates_in_file=res.duplicates_in_file, table_total=res.total,
    )


def run_exoplanet_pipeline(
    fetch=fetch_tap_csv,
    *,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
) -> ExoplanetSummary:
    """The whole journey in one call: fetch -> raw -> clean -> warehouse."""
    got = land_exoplanets(fetch, lake_root=lake_root)
    summary = process_raw(got.path, lake_root=lake_root, warehouse_path=warehouse_path)
    summary.rows_fetched, summary.complete, summary.warning = got.rows, got.complete, got.warning
    return summary


def summary_lines(r: ExoplanetSummary) -> list[str]:
    """The text the command line prints after a run (kept separate so it can be tested)."""
    lines = []
    if not r.complete:
        lines += [f"WARNING: INCOMPLETE FETCH. {r.warning}", ""]
    lines += [
        f"1. Raw:       {r.raw_path}",
        f"2. Clean:     {r.clean_path}",
        f"3. Warehouse: {r.table}: {r.new} new, {r.updated} updated, {r.table_total} total",
    ]
    if r.rows_fetched:
        lines.append(f"   Fetched {r.rows_fetched} planets.")
    if r.skipped_no_key or r.duplicates_in_file:
        lines.append(f"   Not loaded: {r.skipped_no_key} without a planet name, "
                     f"{r.duplicates_in_file} repeated inside the file")
    bad = {k: v for k, v in r.bad_values.items() if v}
    lines.append(f"Values that did not fit their type: {bad or 'none'}")
    lines.append('Try: python -m datalake.warehousing "SELECT count(*) FROM astro.exoplanets"')
    lines.append("Data: NASA Exoplanet Archive (Caltech/IPAC)")
    return lines


def main(argv: list[str] | None = None) -> int:
    """Command line.

    Fetch + clean + load:  python -m datalake.connectors.exoplanets
    Re-clean a raw file:   python -m datalake.connectors.exoplanets --raw PATH_TO_RAW_CSV
    Exit code 2 if the fetch could not be shown to be complete (so a scheduled run can flag it).
    """
    import argparse

    p = argparse.ArgumentParser(description="NASA Exoplanet Archive confirmed planets: raw -> clean -> warehouse.")
    p.add_argument("--raw", help="Skip fetching: clean and load this existing raw CSV (no network needed).")
    a = p.parse_args(argv)

    if a.raw:
        r = process_raw(Path(a.raw))
    else:
        try:
            r = run_exoplanet_pipeline()
        except FileExistsError as e:
            raise SystemExit(
                f"Already fetched today, raw data is never overwritten.\n{e}\n"
                "To re-clean that file without fetching again, run:\n"
                "  python -m datalake.connectors.exoplanets --raw <that path>"
            )
    print("\n".join(summary_lines(r)))
    return 0 if r.complete else 2


if __name__ == "__main__":
    import sys

    sys.exit(main())
