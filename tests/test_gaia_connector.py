import asyncio
import json

import pytest

from datalake.connectors.gaia import (
    NEARBY_STARS_SCHEMA,
    land_nearby_stars,
    nearby_stars_query,
    process_raw,
    run_nearby_stars_pipeline,
    summary_lines,
)

HEADER = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"


def _csv(*rows):
    """Fake Gaia CSV from (source_id, parallax, radial_velocity) tuples; the rest is filler."""
    lines = [HEADER]
    for sid, plx, rv in rows:
        lines.append(f"{sid},10.5,-20.25,{plx},0.02,100.5,-50.5,9.5,1.2,{rv}")
    return ("\n".join(lines) + "\n").encode()


class FakeTap:
    """Stands in for the Gaia archive: remembers the question asked, returns canned bytes."""

    def __init__(self, payload):
        self.payload, self.queries = payload, []

    def __call__(self, adql):
        self.queries.append(adql)
        return self.payload


def _land(tmp_path, payload, **kw):
    tap = FakeTap(payload)
    return tap, land_nearby_stars(tap, lake_root=tmp_path, **kw)


def test_lands_the_csv_untouched_and_asks_the_right_question(tmp_path):
    payload = _csv((1, 768.5, 10.1), (2, 546.9, ""))
    tap, fetch = _land(tmp_path, payload)
    assert fetch.path.relative_to(tmp_path).parts[:3] == ("raw", "gaia", "nearby-stars")
    assert fetch.path.read_bytes() == payload  # exactly as fetched
    assert (fetch.path.parent / f"{fetch.path.name}.meta.json").exists()
    adql = tap.queries[0]
    assert "gaiadr3.gaia_source" in adql and "TOP 1000" in adql and "parallax > 50" in adql


def test_short_result_is_complete_and_full_result_is_flagged(tmp_path):
    _, ok = _land(tmp_path, _csv((1, 60, 1), (2, 70, 2)), limit=3)
    assert (ok.rows, ok.complete, ok.warning) == (2, True, None)
    _, cut = _land(tmp_path, _csv((1, 60, 1), (2, 70, 2), (3, 80, 3)), limit=3, min_parallax_mas=40)
    assert cut.rows == 3 and cut.complete is False and "limit" in cut.warning


def test_header_only_result_is_zero_rows_and_complete(tmp_path):
    _, fetch = _land(tmp_path, (HEADER + "\n").encode())
    assert (fetch.rows, fetch.complete) == (0, True)


def test_refuses_limits_the_archive_would_silently_cut(tmp_path):
    with pytest.raises(ValueError, match="2000"):
        _land(tmp_path, _csv(), limit=5000)


def test_query_values_must_be_plain_numbers():
    with pytest.raises(ValueError):
        nearby_stars_query(limit=10, min_parallax_mas="1; DROP TABLE x")
    with pytest.raises(ValueError):
        nearby_stars_query(limit=0)


def test_an_error_reply_is_not_landed_as_data(tmp_path):
    error = b'<?xml version="1.0"?><VOTABLE><INFO name="QUERY_STATUS" value="ERROR">bad</INFO></VOTABLE>'
    with pytest.raises(RuntimeError, match="not a CSV"):
        _land(tmp_path, error)
    assert not (tmp_path / "raw").exists()  # nothing landed


def _run(tmp_path, payload, wh, **kw):
    return run_nearby_stars_pipeline(FakeTap(payload), lake_root=tmp_path, warehouse_path=wh, **kw)


def test_pipeline_cleans_types_and_loads_astro_schema(tmp_path):
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    s = _run(tmp_path, _csv((1, 768.5, 10.1), (2, 546.9, "")), wh)
    assert s.table == "astro.nearby_stars" and (s.new, s.updated, s.table_total) == (2, 0, 2)
    assert sum(s.bad_values.values()) == 0
    # A star with no measured radial velocity keeps a null, it is not dropped or zeroed.
    assert query("SELECT source_id, radial_velocity FROM astro.nearby_stars ORDER BY source_id", wh) == [
        (1, 10.1), (2, None)]


def test_overlapping_fetches_update_by_source_id(tmp_path):
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    _run(tmp_path, _csv((1, 768.5, 10.1), (2, 546.9, 5.0)), wh)
    # Second fetch (different cut, so a different raw file): star 2 again with a better parallax, plus star 3.
    s = _run(tmp_path, _csv((2, 547.1, 5.0), (3, 400.0, 1.0)), wh, min_parallax_mas=40)
    assert (s.new, s.updated, s.table_total) == (1, 1, 3)
    assert query("SELECT parallax FROM astro.nearby_stars WHERE source_id = 2", wh) == [(547.1,)]


def test_one_warehouse_holds_both_domains(tmp_path):
    """A new source = new connector + new schema. The ocean code is not touched."""
    import pandas as pd

    from datalake.connectors.gfw import process_raw as gfw_process_raw
    from datalake.landing import land_raw
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    _run(tmp_path, _csv((1, 768.5, 10.1)), wh)
    events = pd.DataFrame({
        "start": pd.to_datetime(["2022-01-01"], utc=True), "end": pd.to_datetime(["2022-01-02"], utc=True),
        "vessel": [{"id": "A", "name": "N", "type": "fishing", "flag": "RUS"}],
        "gap": [{"duration_hours": "20", "intentional_disabling": True}],
    })
    raw = land_raw(events.to_json(orient="records", date_format="iso").encode(), source="gfw",
                   dataset="gap-events", filename="e.json", lake_root=tmp_path)
    gfw_process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    assert query("SELECT count(*) FROM ocean.gap_events", wh) == [(1,)]
    assert query("SELECT count(*) FROM astro.nearby_stars", wh) == [(1,)]


def test_process_raw_rebuilds_without_the_network(tmp_path):
    wh = tmp_path / "wh.duckdb"
    first = _run(tmp_path, _csv((1, 60, 1), (2, 70, 2)), wh)
    first.clean_path.unlink()
    wh.unlink()
    again = process_raw(first.raw_path, warehouse_path=wh)
    assert again.clean_path.exists() and again.new == 2


def test_summary_warns_loudly_when_incomplete(tmp_path):
    wh = tmp_path / "wh.duckdb"
    cut = _run(tmp_path, _csv((1, 60, 1), (2, 70, 2)), wh, limit=2)
    lines = summary_lines(cut)
    assert lines[0].startswith("WARNING: INCOMPLETE FETCH")
    ok = _run(tmp_path, _csv((9, 60, 1)), wh, limit=5)
    assert not any("WARNING" in line for line in summary_lines(ok))


def test_schema_covers_expected_columns_and_key():
    assert "source_id" in NEARBY_STARS_SCHEMA and NEARBY_STARS_SCHEMA["source_id"] == "int"
    assert json.dumps(sorted(NEARBY_STARS_SCHEMA)).count("radial_velocity") == 1


# ---- distance bands: fetch a slice of the sky shell by shell, each under the row limit ----

def test_band_adds_an_upper_parallax_bound_and_no_band_means_no_bound():
    plain = nearby_stars_query(limit=10)
    band = nearby_stars_query(limit=10, min_parallax_mas=50, max_parallax_mas=54.74)
    assert "parallax <=" not in plain
    assert "parallax > 50" in band and "parallax <= 54.74" in band


def test_band_must_have_max_above_min_and_plain_numbers():
    with pytest.raises(ValueError, match="above"):
        nearby_stars_query(limit=10, min_parallax_mas=50, max_parallax_mas=50)
    with pytest.raises(ValueError, match="above"):
        nearby_stars_query(limit=10, min_parallax_mas=50, max_parallax_mas=40)
    with pytest.raises(ValueError):
        nearby_stars_query(limit=10, max_parallax_mas="55; DROP TABLE x")


def test_each_band_lands_in_its_own_raw_file_even_on_the_same_day(tmp_path):
    _, whole = _land(tmp_path, _csv((1, 60, 1)))
    _, band = _land(tmp_path, _csv((2, 52, 1)), max_parallax_mas=54.74)
    _, other = _land(tmp_path, _csv((3, 56, 1)), min_parallax_mas=54.74, max_parallax_mas=60)
    assert len({whole.path.name, band.path.name, other.path.name}) == 3
    assert "54.74" in band.path.name  # the file name says which slice it holds


def test_a_band_under_the_limit_completes_and_overlap_is_harmless(tmp_path):
    from datalake.warehousing import query

    wh = tmp_path / "wh.duckdb"
    _run(tmp_path, _csv((1, 768.5, 10.1), (2, 54.7359, 5.0)), wh, limit=2)  # hit its limit: incomplete
    tap = FakeTap(_csv((2, 54.7359, 5.0), (3, 52.0, 1.0), (4, 50.5, 2.0)))  # band 50..54.74, overlaps star 2
    s = run_nearby_stars_pipeline(tap, limit=1000, min_parallax_mas=50, max_parallax_mas=54.74,
                                  lake_root=tmp_path, warehouse_path=wh)
    assert s.complete and s.warning is None
    assert (s.new, s.updated, s.table_total) == (2, 1, 4)
    assert "parallax <= 54.74" in tap.queries[0]
    assert query("SELECT count(*) FROM astro.nearby_stars", wh) == [(4,)]
