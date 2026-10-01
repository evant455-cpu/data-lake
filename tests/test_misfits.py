import pytest

from datalake import misfits
from datalake.connectors import gaia
from datalake.landing import land_raw

HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"


def _warehouse(tmp_path, rows):
    """Fake stars as (source_id, g_mag, bp_rp), all at parallax 100 mas (10 pc), so absolute magnitude == g_mag."""
    lines = [HEADER] + [f"{s},1.0,2.0,100.0,0.1,3.0,4.0,{g},{c}," for s, g, c in rows]
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                   filename="t.csv", lake_root=tmp_path)
    wh = tmp_path / "w.duckdb"
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    return wh


def _crowd(first_id, g, colour, n):
    """n stars sitting almost on the same spot of the colour-brightness diagram."""
    return [(first_id + i, g + 0.01 * i, colour + 0.001 * i) for i in range(n)]


def _flagged_ids(wh, **kw):
    return {r[0] for r in misfits.find_misfits(wh, **kw)}


def test_a_star_far_from_every_other_is_flagged_and_the_crowd_is_not(tmp_path):
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 20) + [(99, 14.0, 0.7)])
    assert _flagged_ids(wh) == {99}


def test_two_separate_crowds_are_both_fine(tmp_path):
    """Real stars form two groups (main sequence and white dwarfs). A single 'typical brightness for this colour'
    would call one of them odd. Looking only at near neighbours does not."""
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 0.8, 20) + _crowd(100, 13.0, 0.8, 20))
    assert _flagged_ids(wh) == set()


def test_a_star_does_not_count_itself_as_a_neighbour(tmp_path):
    wh = _warehouse(tmp_path, [(1, 5.0, 1.0)])
    [row] = misfits.find_misfits(wh)
    assert row[0] == 1 and row[-1] == 0


def test_exactly_the_minimum_number_of_neighbours_is_enough(tmp_path):
    five = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 6))  # each star has 5 neighbours
    assert _flagged_ids(five, min_neighbours=5) == set()
    assert _flagged_ids(five, min_neighbours=6) == {1, 2, 3, 4, 5, 6}


def test_a_neighbour_exactly_on_the_window_edge_counts(tmp_path):
    wh = _warehouse(tmp_path, [(1, 5.0, 1.0), (2, 6.0, 1.15)])  # 1.0 mag and 0.15 colour apart
    assert _flagged_ids(wh, min_neighbours=1) == set()
    narrower = _warehouse(tmp_path / "n", [(1, 5.0, 1.0), (2, 6.01, 1.15)])
    assert _flagged_ids(narrower, min_neighbours=1) == {1, 2}


def test_colour_and_brightness_windows_both_have_to_fit(tmp_path):
    wh = _warehouse(tmp_path, [(1, 5.0, 1.0), (2, 5.0, 1.5), (3, 9.0, 1.0)])  # 2 is too red, 3 too faint
    assert _flagged_ids(wh, min_neighbours=1) == {1, 2, 3}


def test_absolute_magnitude_uses_distance(tmp_path):
    """Same look from Earth, different distance: the nearer one is intrinsically far fainter, so it is not a neighbour."""
    lines = [HEADER,
             "1,1.0,2.0,100.0,0.1,3.0,4.0,5.0,1.0,",   # 10 pc: absolute G = 5.0
             "2,1.0,2.0,1000.0,0.1,3.0,4.0,5.0,1.0,"]  # 1 pc: looks equally bright but is far fainter: absolute G = 10.0
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                   filename="t.csv", lake_root=tmp_path)
    wh = tmp_path / "w.duckdb"
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    rows = misfits.find_misfits(wh, min_neighbours=1)
    assert {r[0] for r in rows} == {1, 2}
    assert {round(r[4], 1) for r in rows} == {5.0, 10.0}


def test_stars_without_colour_or_brightness_are_not_judged(tmp_path):
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 10) + [(50, 14.0, ""), (51, "", 0.7)])
    assert _flagged_ids(wh) == set()
    text = misfits.misfit_report(wh)
    assert "10 stars judged" in text and "2 stars have no colour or brightness" in text


def test_report_lists_the_loneliest_first_and_respects_top(tmp_path):
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 20) + [(98, 14.0, 0.7), (99, 14.0, 0.7), (97, 18.0, 3.0)])
    text = misfits.misfit_report(wh, top=2)
    assert "3 stars sit where fewer than 5 other stars do" in text
    body = text.split("\n", 1)[1]
    assert body.index("\n97 ") < body.index("\n98 ") and "\n99 " not in body  # 97 has 0 neighbours, 98 and 99 have 1
    assert "(2 rows)" in text


def test_report_says_when_nothing_stands_out(tmp_path):
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 10))
    assert "Nothing" in misfits.misfit_report(wh)


def test_bad_settings_are_refused(tmp_path):
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 3))
    for bad in ({"colour_window": 0}, {"mag_window": -1}, {"min_neighbours": 0}):
        with pytest.raises(ValueError):
            misfits.find_misfits(wh, **bad)
    with pytest.raises(ValueError):
        misfits.misfit_report(wh, top=0)


def test_no_table_gives_a_plain_message(tmp_path):
    import duckdb
    wh = tmp_path / "empty.duckdb"
    duckdb.connect(str(wh)).close()
    with pytest.raises(SystemExit, match="nearby_stars"):
        misfits.main(["--warehouse", str(wh)])


def test_the_real_catalogue_does_not_change(tmp_path):
    """Read-only: running the detector must leave the table exactly as it was."""
    from datalake.warehousing import query
    wh = _warehouse(tmp_path, _crowd(1, 5.0, 1.0, 10) + [(99, 14.0, 0.7)])
    before = query("SELECT * FROM astro.nearby_stars ORDER BY source_id", wh)
    misfits.misfit_report(wh)
    assert query("SELECT * FROM astro.nearby_stars ORDER BY source_id", wh) == before


def test_windows_include_their_exact_edge_in_colour_too(tmp_path):
    """Colours 1.0 and 1.5 are exactly 0.5 apart (no rounding noise), so a 0.5 window must include the edge."""
    wh = _warehouse(tmp_path, [(1, 5.0, 1.0), (2, 5.0, 1.5)])
    assert _flagged_ids(wh, colour_window=0.5, min_neighbours=1) == set()
    assert _flagged_ids(wh, colour_window=0.25, min_neighbours=1) == {1, 2}


def test_a_star_with_a_zero_or_negative_parallax_is_skipped_not_a_crash(tmp_path):
    """Gaia can report a negative parallax for a poorly measured star; log10 of that is undefined."""
    lines = [HEADER, "1,1.0,2.0,100.0,0.1,3.0,4.0,5.0,1.0,", "2,1.0,2.0,-3.0,0.1,3.0,4.0,5.0,1.0,"]
    raw = land_raw(("\n".join(lines) + "\n").encode(), source="gaia", dataset="nearby-stars",
                   filename="t.csv", lake_root=tmp_path)
    wh = tmp_path / "w.duckdb"
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    assert "1 stars judged" in misfits.misfit_report(wh)
