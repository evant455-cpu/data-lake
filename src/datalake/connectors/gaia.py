"""Lesson 5: the second connector, for the Gaia space telescope (ESA Gaia DR3).

Same shape as the GFW connector, new domain: fetch -> raw -> clean -> warehouse, this time into the
`astro` schema. Nothing in the lake, cleaning or warehouse code had to change.

Differences from GFW, and why they are nice:
  - No account or token: the Gaia archive is open.
  - It answers a SQL-like question (ADQL) over HTTP and can reply as CSV, so the existing
    `clean_csv` does the cleaning and we need no new cleaning code.
  - Needs no extra library: Python's built-in `urllib` is enough.

NOT yet run against the live archive (the sandbox cannot reach it); tests use a fake fetcher.

Data: ESA Gaia DR3 (https://www.cosmos.esa.int/gaia), credit "ESA/Gaia/DPAC".
"""
from __future__ import annotations

import csv
import io
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from datalake.quality import Check

GAIA_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"

# Anonymous synchronous queries are reported to stop silently at 2000 rows (several projects hit this),
# which is the same trap as GFW's 100-row pages. So we refuse limits above it.
MAX_SYNC_ROWS = 2000

# Column -> type, for cleaning.clean_csv. One row = one star; Gaia's source_id is its unique number.
NEARBY_STARS_SCHEMA = {
    "source_id": "int",
    "ra": "float",  # position on the sky: right ascension, degrees
    "dec": "float",  # position on the sky: declination, degrees
    "parallax": "float",  # tiny yearly wobble in position, milliarcseconds (mas); bigger = closer
    "parallax_error": "float",
    "pmra": "float",  # proper motion: drift across the sky, mas per year
    "pmdec": "float",
    "phot_g_mean_mag": "float",  # brightness in Gaia's G band (smaller number = brighter)
    "bp_rp": "float",  # colour: blue minus red brightness (about 0.8 for the Sun, bigger = redder)
    "radial_velocity": "float",  # km/s toward/away from us; many stars have none (null)
}
NEARBY_STARS_KEY = ("source_id",)

# A second, separate table of ERROR BARS for the same stars (how sure Gaia is of each measurement).
# Kept apart on purpose: new data = new dataset, so the first table and its raw files never change.
# It joins to astro.nearby_stars by source_id.
STAR_ERRORS_SCHEMA = {
    "source_id": "int",
    "radial_velocity_error": "float",  # km/s: the margin of error on radial_velocity (null if no radial velocity)
    "rv_nb_transits": "int",  # how many passes of the telescope the radial velocity is based on (more = steadier)
    "pmra_error": "float",  # margin of error on proper motion, mas per year
    "pmdec_error": "float",
}
STAR_ERRORS_KEY = ("source_id",)
STAR_ERRORS_CHECKS = [
    Check("negative_error", "astro.star_errors",
          "radial_velocity_error < 0 OR pmra_error < 0 OR pmdec_error < 0", "error",
          "A margin of error cannot be negative."),
]


@dataclass(frozen=True)
class GaiaDataset:
    """One kind of table we pull from Gaia: which columns to ask for and where the result goes."""

    name: str  # raw folder name: lake/raw/gaia/<name>/
    columns: tuple[str, ...]  # what to SELECT (always starts with source_id)
    schema: dict  # column -> type, for cleaning
    key: tuple[str, ...]
    table: str  # warehouse table inside the astro schema


NEARBY_STARS = GaiaDataset(
    "nearby-stars", tuple(NEARBY_STARS_SCHEMA), NEARBY_STARS_SCHEMA, NEARBY_STARS_KEY, "nearby_stars")
STAR_ERRORS = GaiaDataset(
    "star-errors", tuple(STAR_ERRORS_SCHEMA), STAR_ERRORS_SCHEMA, STAR_ERRORS_KEY, "star_errors")


# Data-quality rules (Lesson 6). A row matching the condition is flagged, never removed.
# NOTE: simple rules like these cannot catch a value that is merely WRONG but plausible, e.g. Sirius
# showing a brightness of 8.5. That needs a rule that compares columns (colour vs brightness vs distance),
# which is where the machine-learning anomaly detector comes in later.
NEARBY_STARS_CHECKS = [
    Check("impossible_position", "astro.nearby_stars", "ra < 0 OR ra >= 360 OR dec < -90 OR dec > 90", "error",
          "Right ascension must be 0-360 degrees and declination -90 to 90."),
    Check("no_valid_parallax", "astro.nearby_stars", "parallax IS NULL OR parallax <= 0", "error",
          "Without a positive parallax there is no distance."),
    Check("weak_parallax", "astro.nearby_stars", "parallax > 0 AND parallax_error > 0 AND parallax / parallax_error < 10", "warning",
          "Our question asked only for stars measured to better than 10%, so this means the fetch or the data changed."),
    Check("no_colour", "astro.nearby_stars", "bp_rp IS NULL", "warning",
          "No colour measured; colour-based analysis will skip this star."),
]


def nearby_stars_query(
    limit: int = 1000, min_parallax_mas: float = 50.0, max_parallax_mas: float | None = None,
    dataset: GaiaDataset | None = None,
) -> str:
    """The ADQL question: the closest well-measured stars, nearest first.

    parallax > 50 mas means closer than 20 parsecs (about 65 light-years). `parallax_over_error > 10`
    keeps only stars whose distance is measured to better than 10%.
    `max_parallax_mas` (optional) closes the far side of a "band": a shell of space between two distances.
    Fetching shell by shell keeps each answer under the row limit. A big parallax is a close star, so the
    band is "parallax above min and at most max".
    Values go into the text, so they must be plain numbers (checked here).
    """
    limit = int(limit)
    min_parallax_mas = float(min_parallax_mas)
    band = ""
    if max_parallax_mas is not None:
        max_parallax_mas = float(max_parallax_mas)
        if max_parallax_mas <= min_parallax_mas:
            raise ValueError(
                f"max parallax ({max_parallax_mas:g}) must be above min parallax ({min_parallax_mas:g})"
            )
        band = f" AND parallax <= {max_parallax_mas:g}"
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if limit > MAX_SYNC_ROWS:
        raise ValueError(
            f"limit {limit} is above {MAX_SYNC_ROWS}: the archive's quick (synchronous) queries are "
            f"reported to cut off silently there. Use a smaller limit."
        )
    return (
        f"SELECT TOP {limit} {', '.join((dataset or NEARBY_STARS).columns)} "
        "FROM gaiadr3.gaia_source "
        f"WHERE parallax > {min_parallax_mas:g}{band} AND parallax_over_error > 10 "
        "ORDER BY parallax DESC"
    )


def fetch_tap_csv(adql: str) -> bytes:
    """Ask the real Gaia archive one ADQL question and return the CSV reply as bytes."""
    body = urllib.parse.urlencode(
        {"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": adql}
    ).encode()
    request = urllib.request.Request(GAIA_SYNC_URL, data=body)
    with urllib.request.urlopen(request, timeout=120) as reply:
        return reply.read()


@dataclass
class GaiaFetch:
    """What a fetch brought back, including whether we know it is everything."""

    path: Path  # the raw CSV file
    rows: int
    complete: bool  # False = we got exactly `limit` rows, so there may be more
    warning: str | None = None


def _count_rows(payload: bytes) -> int:
    """Count data rows, and make sure the reply really is a CSV table with our columns.

    A failed query comes back as an XML error message, not a CSV. We must never land that as data.
    """
    rows = list(csv.reader(io.StringIO(payload.decode("utf-8-sig"))))
    if not rows or "source_id" not in rows[0]:
        start = payload[:200].decode("utf-8", "replace").strip()
        raise RuntimeError(f"Gaia reply is not a CSV table (query error?). It starts with: {start!r}")
    return len(rows) - 1


def land_nearby_stars(
    fetch=fetch_tap_csv,
    *,
    limit: int = 1000,
    min_parallax_mas: float = 50.0,
    max_parallax_mas: float | None = None,
    lake_root: Path = Path("lake"),
    dataset: GaiaDataset | None = None,
) -> GaiaFetch:
    """Fetch the nearest stars and land the CSV untouched in the raw layer.

    `fetch` takes the ADQL text and returns bytes. It is passed in (like the client for GFW)
    so tests can give a fake one and never touch the network.
    """
    from datalake.landing import land_raw

    ds = dataset or NEARBY_STARS
    adql = nearby_stars_query(limit, min_parallax_mas, max_parallax_mas, ds)
    # The file name says which slice it holds, so each band is its own raw file (and never collides).
    band_tag = "" if max_parallax_mas is None else f"_le-{float(max_parallax_mas):g}"
    payload = fetch(adql)
    rows = _count_rows(payload)  # raises before anything is landed if the reply is not a table
    complete = rows < limit
    warning = None if complete else (
        f"Got exactly the limit ({limit} rows), so there may be more stars. "
        "Raise the limit (max 2000) or narrow the question, for example a thinner band (--max-parallax)."
    )
    raw = land_raw(
        payload,
        source="gaia",
        dataset=ds.name,
        filename=f"parallax-gt-{min_parallax_mas:g}{band_tag}_top-{limit}.csv",
        lake_root=lake_root,
    )
    return GaiaFetch(path=raw, rows=rows, complete=complete, warning=warning)


@dataclass
class GaiaSummary:
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
    table: str | None = None,
    dataset: GaiaDataset | None = None,
) -> GaiaSummary:
    """Clean an already-landed raw CSV and add it to the warehouse table. No network needed."""
    ds = dataset or NEARBY_STARS
    table = table or ds.table
    from datalake.cleaning import clean_csv
    from datalake.warehousing import DEFAULT_WAREHOUSE, append_table

    if lake_root is None:
        if len(raw_file.parents) < 5 or raw_file.parents[3].name != "raw":
            raise ValueError(f"Expected <lake>/raw/<source>/<dataset>/<date>/<file>, got: {raw_file}")
        lake_root = raw_file.parents[4]
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file}")

    report = clean_csv(raw_file, schema=ds.schema, lake_root=lake_root)
    res = append_table(
        report.path, schema=schema, table=table, key=ds.key,
        warehouse_path=warehouse_path or DEFAULT_WAREHOUSE,
    )
    return GaiaSummary(
        raw_path=raw_file, clean_path=report.path, table=f"{schema}.{table}", bad_values=report.bad_values,
        new=res.new, updated=res.updated, skipped_no_key=res.skipped_no_key,
        duplicates_in_file=res.duplicates_in_file, table_total=res.total,
    )


def run_nearby_stars_pipeline(
    fetch=fetch_tap_csv,
    *,
    limit: int = 1000,
    min_parallax_mas: float = 50.0,
    max_parallax_mas: float | None = None,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
    schema: str = "astro",
    table: str | None = None,
    dataset: GaiaDataset | None = None,
) -> GaiaSummary:
    """The whole journey in one call: fetch -> raw -> clean -> warehouse."""
    got = land_nearby_stars(
        fetch, limit=limit, min_parallax_mas=min_parallax_mas, max_parallax_mas=max_parallax_mas,
        lake_root=lake_root, dataset=dataset,
    )
    summary = process_raw(
        got.path, lake_root=lake_root, warehouse_path=warehouse_path, schema=schema, table=table, dataset=dataset
    )
    summary.rows_fetched, summary.complete, summary.warning = got.rows, got.complete, got.warning
    return summary


def summary_lines(r: GaiaSummary) -> list[str]:
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
        lines.append(f"   Fetched {r.rows_fetched} stars.")
    if r.skipped_no_key or r.duplicates_in_file:
        lines.append(f"   Not loaded: {r.skipped_no_key} without a source_id, "
                     f"{r.duplicates_in_file} repeated inside the file")
    bad = {k: v for k, v in r.bad_values.items() if v}
    lines.append(f"Values that did not fit their type: {bad or 'none'}")
    lines.append('Try: python -m datalake.warehousing "SELECT count(*) FROM astro.nearby_stars"')
    lines.append("Data: ESA Gaia DR3 (ESA/Gaia/DPAC)")
    return lines


def process_star_errors_raw(raw_file: Path, **kwargs) -> GaiaSummary:
    """Replay one raw error-bar file into astro.star_errors (this is what `rebuild` calls)."""
    return process_raw(raw_file, dataset=STAR_ERRORS, **kwargs)


def shell_edges_mas(near_pc: float, far_pc: float, step_pc: float) -> list[tuple[float, float]]:
    """Cut the distance range near_pc..far_pc into shells of step_pc, as (near_mas, far_mas) parallax pairs.

    Parallax in mas is 1000 / distance in parsecs, so a NEARER edge is a BIGGER parallax. Every edge is
    computed once and rounded the same way, so one shell's far edge is exactly the next shell's near edge:
    no star falls in a gap and none is counted twice.
    """
    if not (0 < near_pc < far_pc) or step_pc <= 0:
        raise ValueError("Need 0 < near_pc < far_pc and step_pc > 0.")
    pcs = [near_pc]
    while pcs[-1] + step_pc < far_pc - 1e-9:
        pcs.append(pcs[-1] + step_pc)
    pcs.append(far_pc)
    mas = [round(1000.0 / pc, 4) for pc in pcs]
    return list(zip(mas[:-1], mas[1:]))


def _shell_slices(pcs, *, dataset, limit, fetch, lake_root, warehouse_path) -> list:
    """One backfill slice per shell between consecutive distances in `pcs` (parsecs)."""
    from datalake.backfill import Slice, SliceOutcome

    slices = []
    edges = [(round(1000.0 / a, 4), round(1000.0 / b, 4)) for a, b in zip(pcs[:-1], pcs[1:])]
    for (near_mas, far_mas), a, b in zip(edges, pcs[:-1], pcs[1:]):
        def run(near_mas=near_mas, far_mas=far_mas):
            r = run_nearby_stars_pipeline(
                fetch, limit=limit, min_parallax_mas=far_mas, max_parallax_mas=near_mas,
                lake_root=lake_root, warehouse_path=warehouse_path, dataset=dataset,
            )
            note = f"{r.rows_fetched} stars: {r.new} new, {r.updated} updated, {r.table_total} total"
            return SliceOutcome(complete=r.complete, message=note if r.complete else f"{note}. {r.warning}")

        slices.append(Slice(id=f"gaia/{dataset.name}/{a:g}-{b:g}pc", run=run))
    return slices


def band_slices(
    *,
    near_pc: float = 20,
    far_pc: float = 30,
    step_pc: float = 2,
    limit: int = MAX_SYNC_ROWS,
    fetch=fetch_tap_csv,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
) -> list:
    """One backfill slice per distance shell (see datalake.backfill). 20 pc is where our complete sphere ends.

    A shell that returns exactly `limit` stars is reported incomplete and stays pending: make it thinner.
    """
    pcs = [near_pc]
    while pcs[-1] + step_pc < far_pc - 1e-9:
        pcs.append(pcs[-1] + step_pc)
    pcs.append(far_pc)
    return _shell_slices(pcs, dataset=NEARBY_STARS, limit=limit, fetch=fetch,
                         lake_root=lake_root, warehouse_path=warehouse_path)


# Shell edges (parsecs) for the error-bar table. We never fetched the inner sphere shell by shell, so the
# inner edges are chosen so every shell holds well under 2000 stars (from our star counts: ~315, ~760, ~1550).
STAR_ERROR_SHELLS_PC = (1, 10, 15, 20, 22, 24, 26, 28, 30)


def star_error_slices(
    *,
    edges_pc=STAR_ERROR_SHELLS_PC,
    limit: int = MAX_SYNC_ROWS,
    fetch=fetch_tap_csv,
    lake_root: Path = Path("lake"),
    warehouse_path: Path | None = None,
) -> list:
    """Backfill slices that fetch the error bars for the SAME stars as the nearby-stars table."""
    return _shell_slices(list(edges_pc), dataset=STAR_ERRORS, limit=limit, fetch=fetch,
                         lake_root=lake_root, warehouse_path=warehouse_path)


def main() -> None:
    """Command line.

    Fetch + clean + load:  python -m datalake.connectors.gaia [--limit N] [--min-parallax MAS] [--max-parallax MAS]
    Re-clean a raw file:   python -m datalake.connectors.gaia --raw PATH_TO_RAW_CSV
    """
    import argparse

    p = argparse.ArgumentParser(description="Gaia DR3 nearest stars: raw -> clean -> warehouse.")
    p.add_argument("--limit", type=int, default=1000, help="most stars to fetch (max 2000)")
    p.add_argument("--min-parallax", type=float, default=50.0, help="parallax in mas; 50 = closer than 20 parsecs")
    p.add_argument("--max-parallax", type=float, default=None,
                   help="far edge of a band in mas (use with --min-parallax): fetches only the shell between them")
    p.add_argument("--raw", help="Skip fetching: clean and load this existing raw CSV (no network needed).")
    a = p.parse_args()

    if a.raw:
        r = process_raw(Path(a.raw))
    else:
        try:
            r = run_nearby_stars_pipeline(
                limit=a.limit, min_parallax_mas=a.min_parallax, max_parallax_mas=a.max_parallax
            )
        except FileExistsError as e:
            raise SystemExit(
                f"Already fetched today with these settings, raw data is never overwritten.\n{e}\n"
                "To re-clean that file without fetching again, run:\n"
                "  python -m datalake.connectors.gaia --raw <that path>"
            )
    print("\n".join(summary_lines(r)))


if __name__ == "__main__":
    main()
