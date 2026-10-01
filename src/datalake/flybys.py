"""Which nearby stars will pass close to the Sun? (the owner's second science question)

For each star we know its distance d (from parallax), its sideways speed v_t (from proper motion, see
datalake.motion) and its speed toward or away from us v_r (radial velocity; negative = approaching).
If a star keeps moving in a straight line at constant speed, the geometry boils down to two lines:

    time of closest approach = -d * v_r / (v_r^2 + v_t^2)        (negative = it already passed)
    miss distance            =  d * v_t / sqrt(v_r^2 + v_t^2)

So the sideways speed sets how far it misses, and the toward/away speed sets when. We never need the
star's sky position. The speeds are relative to the Sun, which is exactly what a flyby needs.

Limits to keep in mind (they are printed in the report):
  - Straight-line motion. The galaxy's gravity bends paths, so be wary beyond a few million years (Myr).
  - No uncertainties yet. A small error in radial velocity can move the result a lot.
  - Only stars with a radial velocity, and only stars that are within 30 parsecs TODAY (fast stars now
    farther away can still arrive sooner, and we do not have them).

Run:  python -m datalake.flybys [--top N] [--max-myr M] [--past]
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from datalake.motion import SPEED_KM_S
from datalake.warehousing import DEFAULT_WAREHOUSE, ask, query

PARSEC_KM = 3.0856775814913673e13
MYR_SECONDS = 3.15576e13  # a million Julian years
MYR_PER_PC_PER_KM_S = PARSEC_KM / MYR_SECONDS  # one parsec covered at 1 km/s takes this many Myr (~0.9778)
AU_PER_PC = 206264.806

_DIST_PC = "1000 / parallax"
_V_T = f"({SPEED_KM_S})"  # sideways speed, km/s
_V_R = "radial_velocity"  # km/s, negative = approaching
_V_SQ = f"({_V_R} * {_V_R} + {_V_T} * {_V_T})"
_WHEN_MYR = f"(-CAST({MYR_PER_PC_PER_KM_S!r} AS DOUBLE) * {_DIST_PC} * {_V_R} / NULLIF({_V_SQ}, 0))"
_MISS_PC = f"({_DIST_PC} * {_V_T} / sqrt(NULLIF({_V_SQ}, 0)))"
_USABLE = "radial_velocity IS NOT NULL AND pmra IS NOT NULL AND pmdec IS NOT NULL AND parallax > 0"


def _all_flybys_sql() -> str:
    return (
        f"SELECT source_id, {_WHEN_MYR} AS when_myr, {_MISS_PC} AS miss_pc, {_DIST_PC} AS pc_now, "
        f"{_V_R} AS v_toward_kms, {_V_T} AS v_sideways_kms, phot_g_mean_mag AS g_mag "
        f"FROM astro.nearby_stars WHERE {_USABLE}"
    )


def _window(max_myr: float, past: bool) -> str:
    return f"when_myr < 0 AND when_myr >= -{float(max_myr)}" if past else f"when_myr > 0 AND when_myr <= {float(max_myr)}"


def flyby_sql(top: int = 15, max_myr: float = 5.0, past: bool = False) -> str:
    """SQL for the closest flybys inside the time window, closest miss first (numbers not rounded)."""
    if top < 1 or max_myr <= 0:
        raise ValueError("top must be at least 1 and max_myr above 0")
    return (
        f"SELECT * FROM ({_all_flybys_sql()}) WHERE {_window(max_myr, past)} "
        f"ORDER BY miss_pc LIMIT {int(top)}"
    )


def flyby_report(warehouse_path: Path = DEFAULT_WAREHOUSE, top: int = 15, max_myr: float = 5.0, past: bool = False) -> str:
    """Text report of the closest flybys (read-only)."""
    sql = flyby_sql(top, max_myr, past)  # validates top and max_myr before touching the warehouse
    total = query("SELECT count(*) FROM astro.nearby_stars", warehouse_path)[0][0]
    usable, in_window = query(
        f"SELECT count(*), count(*) FILTER (WHERE {_window(max_myr, past)}) FROM ({_all_flybys_sql()})",
        warehouse_path,
    )[0]
    verb = "passed the Sun within" if past else "will pass the Sun within"
    head = (
        f"{usable} of {total} stars have a radial velocity and a motion, so we can place them. "
        f"{in_window} {verb} {max_myr:g} Myr.\n"
        f"Closest {top} {'past' if past else 'future'} flybys. Straight-line motion, no uncertainties yet "
        "(treat results beyond a few Myr, or with a small miss, as leads, not facts):\n"
    )
    table = ask(
        "SELECT source_id, round(when_myr, 3) AS when_myr, round(miss_pc, 3) AS miss_pc, "
        f"round(miss_pc * {AU_PER_PC})::BIGINT AS miss_au, round(pc_now, 1) AS pc_now, "
        "round(v_toward_kms, 1) AS v_toward_kms, round(v_sideways_kms, 1) AS v_sideways_kms, "
        f"round(g_mag, 1) AS g_mag FROM ({sql})",
        warehouse_path, max_rows=top,
    )
    return head + table


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Which nearby stars pass closest to the Sun (straight-line motion)?")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--top", type=int, default=15, help="how many of the closest flybys to list")
    p.add_argument("--max-myr", type=float, default=5.0, help="only flybys within this many million years")
    p.add_argument("--past", action="store_true", help="list stars that already passed, instead of future flybys")
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    try:
        print(flyby_report(wh, a.top, a.max_myr, a.past))
    except duckdb.CatalogException:
        raise SystemExit("There is no astro.nearby_stars table yet. Run: python -m datalake.connectors.gaia")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
