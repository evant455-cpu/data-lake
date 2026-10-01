"""Which nearby stars do not belong where they sit? (the first "stands out" detector)

Plot every star by COLOUR (bp_rp, how red or blue) against TRUE brightness (absolute magnitude, how bright it
would look from 10 parsecs; formula: G + 5*log10(parallax in mas) - 10). Real stars do not land anywhere:
they gather in a few well-known groups (the main sequence, the white dwarfs, the red giants). A star sitting
alone, far from every other star, is worth a second look. Either it is a rare kind of star, or one of its
numbers is wrong.

How it decides: for each star, count the OTHER stars within a small box around it (colour within +-0.15 and
absolute magnitude within +-1.0). Fewer than 5 neighbours = flagged. We count neighbours rather than compare
with a "typical brightness for this colour" because the blue half of the diagram holds TWO groups at once
(main sequence and white dwarfs), and a single typical value would call one of them odd.

What it cannot do (by design, honest limits):
  - A wrong value that lands inside a crowd looks normal. Sirius's G of 8.5 puts it among the white dwarfs.
  - Stars with no colour or no brightness cannot be judged. They are counted and reported, never flagged.
  - Very faint stars (G above about 19) have unreliable colours, so many flags there are bad photometry.
It only reads: nothing in the warehouse is changed or deleted. Flags are leads, not conclusions.

Run:  python -m datalake.misfits [--top N]
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from datalake.warehousing import DEFAULT_WAREHOUSE, ask, query

COLOUR_WINDOW = 0.15  # neighbours must be this close in colour (bp_rp, magnitudes)
MAG_WINDOW = 1.0  # ... and this close in absolute magnitude
MIN_NEIGHBOURS = 5  # fewer other stars than this nearby = flagged
FAINT_G = 19.0  # above this, Gaia colours are unreliable

ABS_MAG = "(phot_g_mean_mag + 5 * log10(parallax) - 10)"
_JUDGEABLE = "bp_rp IS NOT NULL AND phot_g_mean_mag IS NOT NULL AND parallax > 0"


def misfit_sql(colour_window: float, mag_window: float, min_neighbours: int, limit: int | None = None) -> str:
    """SQL listing flagged stars: loneliest first. Columns: source_id, pc, g_mag, bp_rp, abs_g, neighbours."""
    if colour_window <= 0 or mag_window <= 0:
        raise ValueError("the colour and magnitude windows must be positive")
    if min_neighbours < 1:
        raise ValueError("min_neighbours must be at least 1")
    sql = f"""
        WITH s AS (
            SELECT source_id, parallax, phot_g_mean_mag AS g, bp_rp, {ABS_MAG} AS abs_g
            FROM astro.nearby_stars WHERE {_JUDGEABLE}
        ), counted AS (
            SELECT a.source_id, a.parallax, a.g, a.bp_rp, a.abs_g, count(b.source_id) - 1 AS neighbours
            FROM s a JOIN s b
              ON abs(a.bp_rp - b.bp_rp) <= {float(colour_window)!r} AND abs(a.abs_g - b.abs_g) <= {float(mag_window)!r}
            GROUP BY a.source_id, a.parallax, a.g, a.bp_rp, a.abs_g
        )
        SELECT source_id, round(1000 / parallax, 1) AS pc, round(g, 1) AS g_mag, round(bp_rp, 2) AS bp_rp,
               round(abs_g, 2) AS abs_g, neighbours
        FROM counted WHERE neighbours < {int(min_neighbours)}
        ORDER BY neighbours, source_id"""
    return sql + (f"\n        LIMIT {int(limit)}" if limit is not None else "")


def find_misfits(
    warehouse_path: Path = DEFAULT_WAREHOUSE,
    *,
    colour_window: float = COLOUR_WINDOW,
    mag_window: float = MAG_WINDOW,
    min_neighbours: int = MIN_NEIGHBOURS,
) -> list[tuple]:
    """Every flagged star as (source_id, pc, g_mag, bp_rp, abs_g, neighbours). Read-only."""
    return query(misfit_sql(colour_window, mag_window, min_neighbours), warehouse_path)


def misfit_report(warehouse_path: Path = DEFAULT_WAREHOUSE, top: int = 15) -> str:
    """Text report: how many stars were judged, how many flagged, then the `top` loneliest. Read-only."""
    if top < 1:
        raise ValueError("top must be at least 1")
    total, judged = query(
        f"SELECT count(*), count(*) FILTER (WHERE {_JUDGEABLE}) FROM astro.nearby_stars", warehouse_path
    )[0]
    head = f"{judged} stars judged. "
    if total > judged:
        head += f"{total - judged} stars have no colour or brightness, so they were not judged. "
    flagged = len(find_misfits(warehouse_path))
    if not flagged:
        return head + "Nothing stands out: every judged star has company."
    head += (
        f"{flagged} stars sit where fewer than {MIN_NEIGHBOURS} other stars do "
        f"(colour within {COLOUR_WINDOW}, absolute magnitude within {MAG_WINDOW}). The {min(top, flagged)} loneliest:\n"
    )
    table = ask(misfit_sql(COLOUR_WINDOW, MAG_WINDOW, MIN_NEIGHBOURS, limit=top), warehouse_path, max_rows=top)
    note = (
        f"\nCheck g_mag first: above G {FAINT_G:g} the colour itself is unreliable, so a flag there is often "
        "bad photometry, not a strange star."
    )
    return head + table + note


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Which nearby stars sit alone on the colour-brightness diagram?")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--top", type=int, default=15, help="how many of the loneliest to list")
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    try:
        print(misfit_report(wh, a.top))
    except duckdb.CatalogException:
        raise SystemExit("There is no astro.nearby_stars table yet. Run: python -m datalake.connectors.gaia")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
