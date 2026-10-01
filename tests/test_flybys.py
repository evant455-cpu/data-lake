import duckdb
import pytest

from datalake import flybys
from datalake.connectors import gaia
from datalake.landing import land_raw

HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"
MYR_PER_PC_PER_KMS = 0.9778  # 1 parsec at 1 km/s takes about 0.9778 million years


def _pm(v_tangential_kms, parallax_mas):
    """The proper motion (mas/yr) that gives a sideways speed in km/s for a star at this parallax."""
    return v_tangential_kms * parallax_mas / 4.74047


def _warehouse(tmp_path, stars):
    """stars: (source_id, parallax, v_tangential_kms, radial_velocity or None, ra, dec)."""
    lines = [HEADER]
    for sid, plx, vt, rv, ra, dec in stars:
        lines.append(f"{sid},{ra},{dec},{plx},0.1,{_pm(vt, plx)},0.0,9.5,1.2,{'' if rv is None else rv}")
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                   filename="t.csv", lake_root=tmp_path)
    wh = tmp_path / "w.duckdb"
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    return wh


def _rows(wh, **kw):
    """The flyby table as plain Python rows: {source_id: (when_myr, miss_pc)}."""
    con = duckdb.connect(str(wh), read_only=True)
    try:
        sql = flybys.flyby_sql(**kw)
        return {r[0]: (r[1], r[2]) for r in con.execute(sql).fetchall()}
    finally:
        con.close()


def test_head_on_star_arrives_in_distance_over_speed_and_misses_by_nothing(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 0.0, -10.0, 0, 0)])  # 10 pc away, closing at 10 km/s
    when, miss = _rows(wh)[1]
    assert when == pytest.approx(10 / 10 * MYR_PER_PC_PER_KMS, rel=1e-3)
    assert miss == pytest.approx(0.0, abs=1e-6)


def test_sideways_motion_sets_the_miss_distance(tmp_path):
    """r = 10 pc, v_toward = 10, v_sideways = 1: miss = 10 * 1 / sqrt(101); time = 10 * 10 / 101 (x 0.9778 Myr)."""
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0)])
    when, miss = _rows(wh)[1]
    assert miss == pytest.approx(10 / 101 ** 0.5, rel=1e-4)
    assert when == pytest.approx(100 / 101 * MYR_PER_PC_PER_KMS, rel=1e-3)


def test_the_answer_does_not_depend_on_where_in_the_sky_the_star_is(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0), (2, 100.0, 1.0, -10.0, 200.0, -70.0)])
    rows = _rows(wh)
    assert rows[1] == pytest.approx(rows[2])


def test_a_receding_star_is_a_past_flyby_not_a_future_one(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, 10.0, 0, 0)])
    assert _rows(wh) == {}
    when, miss = _rows(wh, past=True)[1]
    assert when < 0 and miss == pytest.approx(10 / 101 ** 0.5, rel=1e-4)


def test_stars_beyond_the_time_window_are_left_out(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 0.0, -10.0, 0, 0), (2, 100.0, 0.0, -0.4, 0, 0)])  # arrives in ~24 Myr
    assert set(_rows(wh, max_myr=5)) == {1}
    assert set(_rows(wh, max_myr=50)) == {1, 2}


def test_closest_misses_come_first(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 3.0, -10.0, 0, 0), (2, 100.0, 0.5, -10.0, 0, 0), (3, 100.0, 1.5, -10.0, 0, 0)])
    con = duckdb.connect(str(wh), read_only=True)
    order = [r[0] for r in con.execute(flybys.flyby_sql()).fetchall()]
    assert order == [2, 3, 1]


def test_stars_without_a_radial_velocity_or_without_motion_are_skipped(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0), (2, 100.0, 1.0, None, 0, 0), (3, 100.0, 0.0, 0.0, 0, 0)])
    assert set(_rows(wh)) == {1}  # star 2 has no radial velocity, star 3 does not move at all (no crash)


def test_report_says_how_many_stars_could_be_placed(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0), (2, 100.0, 1.0, None, 0, 0), (3, 100.0, 1.0, 10.0, 0, 0)])
    text = flybys.flyby_report(wh, top=5)
    head = text.splitlines()[0]
    assert "2 of 3 stars" in head and "1 will pass" in head
    assert "miss_au" in text and "(1 row)" in text


def test_miss_distance_is_also_given_in_au(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0)])
    text = flybys.flyby_report(wh)
    assert str(round(10 / 101 ** 0.5 * 206264.806)) in text.replace(",", "")


def test_bad_settings_and_missing_table(tmp_path):
    with pytest.raises(ValueError):
        flybys.flyby_report(tmp_path / "w.duckdb", top=0)
    with pytest.raises(ValueError):
        flybys.flyby_report(tmp_path / "w.duckdb", max_myr=0)
    wh = tmp_path / "empty.duckdb"
    duckdb.connect(str(wh)).close()
    with pytest.raises(SystemExit, match="nearby_stars"):
        flybys.main(["--warehouse", str(wh)])


def test_past_flybys_also_respect_the_time_window(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 0.0, 10.0, 0, 0), (2, 100.0, 0.0, 0.4, 0, 0)])  # passed ~1 Myr and ~24 Myr ago
    assert set(_rows(wh, past=True, max_myr=5)) == {1}
    assert set(_rows(wh, past=True, max_myr=50)) == {1, 2}


# ---- error bars: shown beside each flyby once the star_errors table exists ----

def _with_errors(tmp_path, stars, errors):
    """stars as in _warehouse; errors = {source_id: (rv_error, transits)}."""
    from datalake.connectors import gaia as g
    wh = _warehouse(tmp_path, stars)
    lines = ["source_id,radial_velocity_error,rv_nb_transits,pmra_error,pmdec_error"]
    for sid, (err, n) in errors.items():
        lines.append(f"{sid},{err},{n},0.05,0.04")
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="star-errors",
                   filename="e.csv", lake_root=tmp_path)
    g.process_star_errors_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    return wh


def test_the_report_shows_the_radial_velocity_error_when_we_have_it(tmp_path):
    wh = _with_errors(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0), (2, 100.0, 1.0, -10.0, 0, 0)],
                      {1: (0.4, 20)})  # star 2 has no error row at all
    text = flybys.flyby_report(wh)
    assert "rv_err_kms" in text and "rv_transits" in text
    row1 = next(line for line in text.splitlines() if line.startswith("1 "))
    assert "0.4" in row1 and "20" in row1
    assert "(2 rows)" in text  # star 2 is still listed, just with blanks


def test_the_report_still_works_without_the_error_table(tmp_path):
    wh = _warehouse(tmp_path, [(1, 100.0, 1.0, -10.0, 0, 0)])
    text = flybys.flyby_report(wh)
    assert "rv_err_kms" not in text and "(1 row)" in text
