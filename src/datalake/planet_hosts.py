"""Which of our stars do the known planets orbit? (fixing the labels before any machine learning)

The planet table links each planet to its host star by the star's Gaia number. Some hosts have NO Gaia number
in the archive, even very close ones (Lalande 21185 and Luyten's Star, both with known planets). Matched by
number alone, those stars look planet-free, and a model trained on that would learn a wrong lesson.

So we match in two ways, most trusted first:
  1. Gaia number: exact, when the archive gives one.
  2. Sky position, only for planets with no Gaia number: the nearest of our stars within a small radius
     (default 10 arcseconds).

Stars move. Gaia's positions are for the year 2016, and the archive does not document which year its positions
are for (often 2000). A fast star like Lalande 21185 moves about 77 arcseconds in those 16 years, so for each star
we compute where it was in 2000 as well as 2016 (using its proper motion) and keep the closer of the two.

Double stars sit a few arcseconds apart, so a position match can be ambiguous. We keep the nearest and report how
many candidates there were, and every position match is listed so a person can check it.
Read-only: nothing in the warehouse is changed.

Run:  python -m datalake.planet_hosts [--radius ARCSEC]
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datalake.connectors.exoplanets import GAIA_SOURCE_ID_SQL
from datalake.warehousing import DEFAULT_WAREHOUSE, query

RADIUS_ARCSEC = 10.0
GAIA_EPOCH = 2016.0  # Gaia DR3 positions are for this year
OTHER_EPOCH = 2000.0  # the usual catalogue year; the archive does not say which it uses


@dataclass
class HostMatch:
    pl_name: str
    hostname: str
    source_id: int
    method: str  # "gaia_id" or "position"
    sep_arcsec: float | None  # how far apart (position matches only)
    candidates: int  # how many of our stars were inside the radius (1 = no doubt)


def _sep_arcsec_sql(year: float) -> str:
    """Separation in arcseconds between the archive position (p) and our star (s) moved to `year`.

    Small-angle formula: fine for distances of seconds to minutes of arc. RA difference is wrapped into
    -180..180 degrees so 359.99 and 0.01 count as close, and shrunk by cos(dec) because RA lines meet at the poles.
    """
    dt = float(year) - GAIA_EPOCH
    star_dec = f'(s."dec" + coalesce(s.pmdec, 0) * {dt!r} / 3600000.0)'
    star_ra = f'(s.ra + coalesce(s.pmra, 0) * {dt!r} / 3600000.0 / cos(radians(s."dec")))'
    d_ra = f"((((p.ra - {star_ra}) + 540.0) % 360.0) - 180.0) * cos(radians(p.\"dec\"))"
    d_dec = f'(p."dec" - {star_dec})'
    return f"3600.0 * sqrt(({d_ra}) * ({d_ra}) + {d_dec} * {d_dec})"


def _has_table(name: str, warehouse_path: Path) -> bool:
    return bool(query(
        f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'astro' AND table_name = '{name}'",
        warehouse_path,
    ))


def host_matches_sql(radius_arcsec: float = RADIUS_ARCSEC, with_positions: bool = True) -> str:
    """SQL listing one matched star per planet: pl_name, hostname, source_id, method, sep_arcsec, candidates."""
    if radius_arcsec <= 0:
        raise ValueError("radius_arcsec must be positive")
    by_id = f"""
        SELECT x.pl_name, x.hostname, s.source_id, 'gaia_id' AS method, CAST(NULL AS DOUBLE) AS sep_arcsec, 1 AS candidates
        FROM astro.exoplanets x JOIN astro.nearby_stars s ON s.source_id = {GAIA_SOURCE_ID_SQL.replace('gaia_dr3_id', 'x.gaia_dr3_id')}"""
    if not with_positions:
        return by_id + "\n        ORDER BY pl_name"
    sep = f"least({_sep_arcsec_sql(GAIA_EPOCH)}, {_sep_arcsec_sql(OTHER_EPOCH)})"
    return f"""
        WITH near AS (
            SELECT x.pl_name, x.hostname, s.source_id, {sep} AS sep_arcsec
            FROM astro.exoplanets x
            JOIN astro.exoplanet_positions p ON p.pl_name = x.pl_name
            CROSS JOIN astro.nearby_stars s
            WHERE x.gaia_dr3_id IS NULL AND p.ra IS NOT NULL AND p."dec" IS NOT NULL
              AND s.ra IS NOT NULL AND s."dec" IS NOT NULL
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY pl_name ORDER BY sep_arcsec, source_id) AS rn,
                   count(*) OVER (PARTITION BY pl_name) AS candidates
            FROM near WHERE sep_arcsec <= {float(radius_arcsec)!r}
        )
        {by_id}
        UNION ALL
        SELECT pl_name, hostname, source_id, 'position', sep_arcsec, candidates FROM ranked WHERE rn = 1
        ORDER BY pl_name"""


def find_host_matches(warehouse_path: Path = DEFAULT_WAREHOUSE, *, radius_arcsec: float = RADIUS_ARCSEC) -> list[HostMatch]:
    """Every planet we could place on one of our stars. Planets around stars outside our sample are simply absent."""
    with_positions = _has_table("exoplanet_positions", warehouse_path)
    rows = query(host_matches_sql(radius_arcsec, with_positions), warehouse_path)
    return [HostMatch(*r) for r in rows]


def match_report(warehouse_path: Path = DEFAULT_WAREHOUSE, *, radius_arcsec: float = RADIUS_ARCSEC) -> str:
    """Text report: how many planets matched by each route, and the position matches for a person to check."""
    with_positions = _has_table("exoplanet_positions", warehouse_path)
    matches = find_host_matches(warehouse_path, radius_arcsec=radius_arcsec)
    [(total,)] = query("SELECT count(*) FROM astro.exoplanets", warehouse_path)
    by_id = [m for m in matches if m.method == "gaia_id"]
    by_pos = [m for m in matches if m.method == "position"]
    stars = len({m.source_id for m in matches})
    lines = [
        f"{total} planets: {len(by_id)} by Gaia number, {len(by_pos)} by sky position, "
        f"{total - len(matches)} not matched (mostly planets around stars outside our sample).",
        f"Matched planets orbit {stars} of our stars.",
    ]
    if not with_positions:
        lines.append("No host positions loaded yet, so hosts without a Gaia number could not be placed. "
                     "Run: python -m datalake.connectors.exoplanets --positions")
    if by_pos:
        lines.append(f"\nMatched by position (within {radius_arcsec:g} arcsec; check these):")
        for m in by_pos:
            doubt = "" if m.candidates == 1 else f"  ({m.candidates} candidates, nearest kept)"
            lines.append(f"  {m.pl_name:28s} host {m.hostname:22s} -> {m.source_id}  {m.sep_arcsec:.1f} arcsec{doubt}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    import argparse

    import duckdb

    p = argparse.ArgumentParser(description="Match known planets to our Gaia stars (Gaia number first, then position).")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--radius", type=float, default=RADIUS_ARCSEC, help="position-match radius in arcseconds")
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    try:
        print(match_report(wh, radius_arcsec=a.radius))
    except duckdb.CatalogException:
        raise SystemExit("Needs astro.exoplanets and astro.nearby_stars. Run: python -m datalake.connectors.exoplanets "
                         "and python -m datalake.connectors.gaia")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
