import math

import duckdb
import pytest

from datalake import planet_hosts, rebuild
from datalake.connectors import exoplanets, gaia
from datalake.landing import land_raw
from datalake.warehousing import query

STAR_HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"
PLANET_HEADER = ",".join(exoplanets.EXOPLANET_COLUMNS)
POS_HEADER = ",".join(exoplanets.HOST_POSITION_COLUMNS)

# Lalande 21185 (Gaia DR3, epoch 2016): a fast star, about 4.8 arcseconds a year.
LALANDE = dict(sid=762815470562110464, ra=165.8341, dec=35.9488, pmra=-580.0, pmdec=-4777.0)


def _at_year(star, year):
    """Where the star was (or will be) in `year`, moving it along its proper motion from Gaia's 2016 position."""
    dt = year - 2016.0
    return (star["ra"] + star["pmra"] * dt / 3.6e6 / math.cos(math.radians(star["dec"])),
            star["dec"] + star["pmdec"] * dt / 3.6e6)


def _build(tmp_path, stars, planets, positions=None):
    """stars: dicts (sid, ra, dec, pmra, pmdec); planets: (name, host, gaia_text); positions: (name, host, ra, dec)."""
    wh = tmp_path / "w.duckdb"
    lines = [STAR_HEADER] + [f"{s['sid']},{s['ra']},{s['dec']},100.0,0.1,{s['pmra']},{s['pmdec']},9.5,2.0," for s in stars]
    gaia.process_raw(land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                              filename="s.csv", lake_root=tmp_path), lake_root=tmp_path, warehouse_path=wh)
    lines = [PLANET_HEADER] + [f"{n},{h},{g},5.0,200.0,9.5,1,2019,Radial Velocity,10.0,3.0," for n, h, g in planets]
    exoplanets.process_raw(land_raw(("\n".join(lines) + "\n").encode(), source="exoplanets", dataset="planets",
                                    filename="p.csv", lake_root=tmp_path), lake_root=tmp_path, warehouse_path=wh)
    if positions is not None:
        lines = [POS_HEADER] + [f"{n},{h},{ra!r},{dec!r}" for n, h, ra, dec in positions]
        exoplanets.process_positions_raw(land_raw(("\n".join(lines) + "\n").encode(), source="exoplanets",
                                                  dataset="host-positions", filename="q.csv", lake_root=tmp_path),
                                         lake_root=tmp_path, warehouse_path=wh)
    return wh


def _matches(wh, **kw):
    """pl_name -> (source_id, method)"""
    return {m.pl_name: (m.source_id, m.method) for m in planet_hosts.find_host_matches(wh, **kw)}


def _star(sid, ra, dec, pmra=0.0, pmdec=0.0):
    return dict(sid=sid, ra=ra, dec=dec, pmra=pmra, pmdec=pmdec)


def test_the_gaia_number_is_used_first(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0), _star(12, 50.0, 20.0)], [("P b", "P", "Gaia DR3 11")],
                positions=[("P b", "P", 50.0, 20.0)])  # position points at star 12, but the Gaia number wins
    assert _matches(wh) == {"P b": (11, "gaia_id")}


def test_a_host_without_a_gaia_number_is_found_by_position(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0)], [("P b", "P", "")], positions=[("P b", "P", 10.0, 20.0)])
    assert _matches(wh) == {"P b": (11, "position")}


def test_a_fast_star_is_found_where_it_was_in_2000(tmp_path):
    """Lalande 21185 moved about 77 arcseconds between 2000 and 2016. A catalogue position from 2000 must still match."""
    ra2000, dec2000 = _at_year(LALANDE, 2000)
    wh = _build(tmp_path, [LALANDE], [("GJ 411 b", "GJ 411", "")], positions=[("GJ 411 b", "GJ 411", ra2000, dec2000)])
    [m] = planet_hosts.find_host_matches(wh)
    assert (m.source_id, m.method) == (LALANDE["sid"], "position") and m.sep_arcsec < 0.5
    # without the motion correction it would be far outside the radius: proves the 2000 epoch is what matched
    sep_2016 = 3600 * math.hypot((ra2000 - LALANDE["ra"]) * math.cos(math.radians(LALANDE["dec"])), dec2000 - LALANDE["dec"])
    assert sep_2016 > 60


def test_too_far_away_is_no_match(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0)], [("P b", "P", "")], positions=[("P b", "P", 10.0, 20.0 + 11 / 3600)])
    assert _matches(wh) == {}
    assert _matches(wh, radius_arcsec=12) == {"P b": (11, "position")}


def test_the_nearest_of_two_candidates_wins_and_the_doubt_is_reported(tmp_path):
    """Double stars sit a few arcseconds apart, so more than one candidate is possible."""
    wh = _build(tmp_path, [_star(11, 10.0, 20.0 + 6 / 3600), _star(12, 10.0, 20.0 + 2 / 3600)], [("P b", "P", "")],
                positions=[("P b", "P", 10.0, 20.0)])
    [m] = planet_hosts.find_host_matches(wh)
    assert m.source_id == 12 and m.candidates == 2


def test_right_ascension_wraps_around_at_360_degrees(tmp_path):
    wh = _build(tmp_path, [_star(11, 0.0002, 20.0)], [("P b", "P", "")], positions=[("P b", "P", 359.9999, 20.0)])
    assert _matches(wh) == {"P b": (11, "position")}


def test_without_the_positions_table_gaia_numbers_still_work(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0)], [("P b", "P", "Gaia DR3 11"), ("Q b", "Q", "")])
    assert _matches(wh) == {"P b": (11, "gaia_id")}
    assert "positions" in planet_hosts.match_report(wh)


def test_report_counts_every_route(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0), _star(12, 50.0, 20.0)],
                [("A b", "A", "Gaia DR3 11"), ("B b", "B", ""), ("C b", "C", "Gaia DR3 999"), ("D b", "D", "")],
                positions=[("B b", "B", 50.0, 20.0), ("D b", "D", 200.0, -40.0)])
    text = planet_hosts.match_report(wh)
    assert "4 planets" in text
    assert "1 by Gaia number" in text and "1 by sky position" in text
    assert "2 not matched" in text
    assert "B b" in text  # positional matches are listed so a person can check them


def test_matching_changes_nothing(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0)], [("P b", "P", "")], positions=[("P b", "P", 10.0, 20.0)])
    before = [query(f"SELECT * FROM astro.{t} ORDER BY 1", wh) for t in ("exoplanets", "exoplanet_positions", "nearby_stars")]
    planet_hosts.match_report(wh)
    assert [query(f"SELECT * FROM astro.{t} ORDER BY 1", wh) for t in ("exoplanets", "exoplanet_positions", "nearby_stars")] == before


def test_bad_radius_is_refused(tmp_path):
    wh = _build(tmp_path, [_star(11, 10.0, 20.0)], [("P b", "P", "")])
    with pytest.raises(ValueError):
        planet_hosts.find_host_matches(wh, radius_arcsec=0)


def test_command_line_says_what_is_missing(tmp_path):
    wh = tmp_path / "empty.duckdb"
    duckdb.connect(str(wh)).close()
    with pytest.raises(SystemExit, match="exoplanets"):
        planet_hosts.main(["--warehouse", str(wh)])


# ---- the positions dataset itself ----

def test_positions_dataset_asks_for_positions_and_lands_in_its_own_folder(tmp_path):
    assert "ra, dec" in exoplanets.exoplanets_query(exoplanets.HOST_POSITIONS)
    payload = f"{POS_HEADER}\nP b,P,10.5,-20.25\n".encode()
    got = exoplanets.land_exoplanets(lambda q: b"count\n1\n" if "count(*)" in q else payload,
                                     lake_root=tmp_path, dataset=exoplanets.HOST_POSITIONS)
    assert got.path.relative_to(tmp_path).parts[:3] == ("raw", "exoplanets", "host-positions") and got.complete
    s = exoplanets.process_raw(got.path, lake_root=tmp_path, warehouse_path=tmp_path / "w.duckdb", dataset=exoplanets.HOST_POSITIONS)
    assert s.table == "astro.exoplanet_positions"
    assert query("SELECT ra, \"dec\" FROM astro.exoplanet_positions", tmp_path / "w.duckdb") == [(10.5, -20.25)]


def test_positions_are_registered_with_rebuild():
    assert rebuild._handlers()[("exoplanets", "host-positions")] is exoplanets.process_positions_raw


def test_command_line_positions_raw(tmp_path, monkeypatch, capsys):
    raw = land_raw(f"{POS_HEADER}\nP b,P,10.5,-20.25\n".encode(), source="exoplanets", dataset="host-positions",
                   filename="q.csv", lake_root=tmp_path / "lake")
    monkeypatch.chdir(tmp_path)
    assert exoplanets.main(["--positions", "--raw", str(raw)]) == 0
    assert "astro.exoplanet_positions: 1 new" in capsys.readouterr().out


def test_near_the_pole_a_big_ra_difference_is_a_small_distance(tmp_path):
    """At declination 80, lines of RA bunch together: 40 arcsec of RA is only about 7 arcsec on the sky."""
    wh = _build(tmp_path, [_star(11, 10.0, 80.0)], [("P b", "P", "")], positions=[("P b", "P", 10.0 + 40 / 3600, 80.0)])
    [m] = planet_hosts.find_host_matches(wh)
    assert m.source_id == 11 and m.sep_arcsec == pytest.approx(40 * math.cos(math.radians(80)), abs=0.05)
