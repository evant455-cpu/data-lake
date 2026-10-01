import pytest

from datalake import motion
from datalake.connectors import gaia
from datalake.landing import land_raw
from datalake.warehousing import query

HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"


def _warehouse(tmp_path, rows):
    """Load fake stars (source_id, parallax, pmra, pmdec) into a real warehouse the normal way."""
    lines = [HEADER] + [f"{s},1.0,2.0,{p},0.1,{a},{d},9.5,1.2," for s, p, a, d in rows]
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                   filename="t.csv", lake_root=tmp_path)
    wh = tmp_path / "w.duckdb"
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    return wh


# Barnard's Star (Gaia DR3 values): its tangential speed is known to be about 90 km/s.
BARNARD = (1, 546.976, -802.803, 10362.394)


def test_barnards_star_comes_out_at_its_known_speed(tmp_path):
    wh = _warehouse(tmp_path, [BARNARD])
    [(speed,)] = query(f"SELECT {motion.SPEED_KM_S} FROM astro.nearby_stars", wh)
    assert speed == pytest.approx(90.1, abs=0.2)


def test_same_sky_motion_is_faster_when_the_star_is_farther(tmp_path):
    """The whole point: angular motion alone is not speed. Twice as far, same drift = twice as fast."""
    wh = _warehouse(tmp_path, [(1, 100.0, 300.0, 400.0), (2, 50.0, 300.0, 400.0)])
    near, far = (r[0] for r in query(f"SELECT {motion.SPEED_KM_S} FROM astro.nearby_stars ORDER BY source_id", wh))
    assert far == pytest.approx(2 * near)


def test_report_lists_fastest_first_and_gives_the_median_for_context(tmp_path):
    wh = _warehouse(tmp_path, [BARNARD, (2, 100.0, 30.0, 40.0), (3, 100.0, 60.0, 80.0), (4, 100.0, 90.0, 120.0)])
    text = motion.fastest_report(wh, top=2)
    lines = text.splitlines()
    assert "Median" in lines[0] and "4 stars" in lines[0]
    body = text.split("\n", 1)[1]
    assert body.index("\n1 ") < body.index("\n4 ") and "\n3 " not in body  # top 2 only: stars 1 and 4
    assert "(2 rows)" in text


def test_stars_without_a_proper_motion_are_left_out_not_counted_as_zero(tmp_path):
    wh = _warehouse(tmp_path, [BARNARD, (2, 100.0, "", "")])
    assert "1 stars" in motion.fastest_report(wh, top=5).splitlines()[0]


def test_no_table_gives_a_plain_message(tmp_path):
    import duckdb
    wh = tmp_path / "empty.duckdb"
    duckdb.connect(str(wh)).close()
    with pytest.raises(SystemExit, match="nearby_stars"):
        motion.main(["--warehouse", str(wh)])


def test_top_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        motion.fastest_report(tmp_path / "w.duckdb", top=0)
