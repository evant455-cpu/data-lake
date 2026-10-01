"""Which nearby stars move unusually fast? (the first question the owner asked of the star data)

A star's proper motion (pmra, pmdec, in milliarcseconds per year) is how far it drifts ACROSS the sky.
That is an angle, not a speed: a slow star that is very close drifts a lot, a fast star far away barely
seems to move. To get a real speed we multiply by distance. With parallax in mas this works out to

    tangential speed (km/s) = 4.74047 * proper motion (mas/yr) / parallax (mas)

4.74047 is just the unit conversion: how many km/s one arcsecond-per-year is, at a distance of one parsec.
"Tangential" = the sideways part of the motion; the part toward or away from us is the radial velocity,
which many of our stars lack. The speed is relative to the Sun, so it includes the Sun's own motion.

Run:  python -m datalake.motion [--top N]
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from datalake.warehousing import DEFAULT_WAREHOUSE, ask, query

KM_PER_S_PER_ARCSEC_PER_YR_AT_1PC = 4.74047
TOTAL_PM_MAS_YR = "sqrt(pmra * pmra + pmdec * pmdec)"  # Gaia's pmra already includes the cos(dec) factor
SPEED_KM_S = f"{KM_PER_S_PER_ARCSEC_PER_YR_AT_1PC} * {TOTAL_PM_MAS_YR} / parallax"
_HAS_MOTION = "pmra IS NOT NULL AND pmdec IS NOT NULL AND parallax > 0"


def fastest_report(warehouse_path: Path = DEFAULT_WAREHOUSE, top: int = 15) -> str:
    """Text report: the median speed for context, then the `top` fastest stars. Read-only."""
    if top < 1:
        raise ValueError("top must be at least 1")
    n, median = query(
        f"SELECT count(*), median({SPEED_KM_S}) FROM astro.nearby_stars WHERE {_HAS_MOTION}", warehouse_path
    )[0]
    head = f"Median tangential speed of {n} stars: {median:.1f} km/s (relative to the Sun). The {top} fastest:\n" if n else (
        "No stars with a measured motion yet.\n")
    table = ask(
        f"SELECT source_id, round(1000 / parallax, 1) AS pc, round({SPEED_KM_S}, 1) AS km_s, "
        f"round({TOTAL_PM_MAS_YR}) AS pm_mas_yr, round(phot_g_mean_mag, 1) AS g_mag "
        f"FROM astro.nearby_stars WHERE {_HAS_MOTION} ORDER BY km_s DESC LIMIT {int(top)}",
        warehouse_path, max_rows=top,
    )
    return head + table


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Which nearby stars move fastest across the sky (real speed, km/s)?")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--top", type=int, default=15, help="how many of the fastest to list")
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    try:
        print(fastest_report(wh, a.top))
    except duckdb.CatalogException:
        raise SystemExit("There is no astro.nearby_stars table yet. Run: python -m datalake.connectors.gaia")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
