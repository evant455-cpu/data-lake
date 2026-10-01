import asyncio

import duckdb
import pytest

from datalake.connectors.gaia import NEARBY_STARS_CHECKS
from datalake.connectors.gfw import GAP_EVENT_CHECKS
from datalake.quality import Check, main, report_lines, run_checks


def _warehouse(tmp_path, gaps=None, stars=None):
    """A tiny warehouse. gaps: (vessel_id, start, end, hours); stars: (id, ra, dec, parallax, parallax_error, bp_rp)."""
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    if gaps is not None:
        con.execute("CREATE SCHEMA ocean")
        con.execute('CREATE TABLE ocean.gap_events (vessel_id VARCHAR, start TIMESTAMPTZ, "end" TIMESTAMPTZ, duration_hours DOUBLE)')
        con.executemany("INSERT INTO ocean.gap_events VALUES (?, ?, ?, ?)", gaps)
    if stars is not None:
        con.execute("CREATE SCHEMA astro")
        con.execute("CREATE TABLE astro.nearby_stars (source_id BIGINT, ra DOUBLE, dec DOUBLE, parallax DOUBLE, "
                    "parallax_error DOUBLE, bp_rp DOUBLE)")
        con.executemany("INSERT INTO astro.nearby_stars VALUES (?, ?, ?, ?, ?, ?)", stars)
    con.close()
    return wh


GOOD_GAP = ("A", "2022-01-01 00:00:00+00", "2022-01-02 00:00:00+00", 24.0)
GOOD_STAR = (1, 10.0, -20.0, 100.0, 0.1, 1.2)


def _by_name(results):
    return {r.check.name: r for r in results}


def test_gap_rules_flag_each_kind_of_bad_row(tmp_path):
    wh = _warehouse(tmp_path, gaps=[
        GOOD_GAP,
        ("B", "2022-01-05 00:00:00+00", "2022-01-04 00:00:00+00", 5.0),   # ends before it starts
        ("C", "2022-02-01 00:00:00+00", "2022-02-02 00:00:00+00", 0.0),   # zero length
        ("D", "2021-09-30 00:00:00+00", "2025-06-22 00:00:00+00", 32651.9),  # the real 3.7-year outlier
        ("E", "2022-03-01 00:00:00+00", "2022-03-02 00:00:00+00", None),  # duration missing
    ])
    r = _by_name(run_checks(GAP_EVENT_CHECKS, wh))
    assert {n: x.flagged for n, x in r.items()} == {
        "end_before_start": 1, "non_positive_duration": 1, "missing_duration": 1, "extreme_duration": 1}
    assert all(x.total == 5 for x in r.values())
    assert r["extreme_duration"].examples[0]["vessel_id"] == "D"
    assert (r["end_before_start"].check.severity, r["extreme_duration"].check.severity) == ("error", "warning")


def test_clean_data_flags_nothing(tmp_path):
    wh = _warehouse(tmp_path, gaps=[GOOD_GAP], stars=[GOOD_STAR])
    results = run_checks(GAP_EVENT_CHECKS + NEARBY_STARS_CHECKS, wh)
    assert results and all(r.flagged == 0 and r.skipped is None for r in results)


def test_star_rules_flag_each_kind_of_bad_row(tmp_path):
    wh = _warehouse(tmp_path, stars=[
        GOOD_STAR,
        (2, 400.0, 10.0, 100.0, 0.1, 1.0),   # ra outside 0-360
        (3, 10.0, 95.0, 100.0, 0.1, 1.0),    # dec outside -90..90
        (4, 10.0, 10.0, -5.0, 0.1, 1.0),     # negative parallax
        (5, 10.0, 10.0, 60.0, 30.0, 1.0),    # parallax only 2x its error: weak
        (6, 10.0, 10.0, 100.0, 0.1, None),   # no colour
    ])
    r = _by_name(run_checks(NEARBY_STARS_CHECKS, wh))
    assert {n: x.flagged for n, x in r.items()} == {
        "impossible_position": 2, "no_valid_parallax": 1, "weak_parallax": 1, "no_colour": 1}


def test_a_missing_table_is_skipped_not_a_crash(tmp_path):
    wh = _warehouse(tmp_path, gaps=[GOOD_GAP])  # no astro schema at all
    r = run_checks(NEARBY_STARS_CHECKS, wh)
    assert all(x.skipped and "not found" in x.skipped for x in r)


def test_checks_only_flag_and_never_change_the_data(tmp_path):
    wh = _warehouse(tmp_path, gaps=[GOOD_GAP, ("D", "2021-09-30 00:00:00+00", "2025-06-22 00:00:00+00", 32651.9)])
    run_checks(GAP_EVENT_CHECKS, wh)
    con = duckdb.connect(str(wh), read_only=True)
    assert con.execute("SELECT count(*) FROM ocean.gap_events").fetchone() == (2,)


def test_rule_text_is_validated_for_table_names_and_severity():
    with pytest.raises(ValueError):
        Check("x", "ocean.gap_events; DROP TABLE y", "true", "error", "why")
    with pytest.raises(ValueError):
        Check("x", "ocean.gap_events", "true", "catastrophe", "why")


def test_report_and_exit_code(tmp_path, capsys):
    warn_only = _warehouse(tmp_path, gaps=[GOOD_GAP, ("D", "2021-09-30 00:00:00+00", "2025-06-22 00:00:00+00", 32651.9)])
    assert main(["--warehouse", str(warn_only)]) == 0  # a warning alone does not fail the run
    out = capsys.readouterr().out
    assert "[WARN]" in out and "extreme_duration" in out and "[ok]" in out and "32651.9" in out

    sub = tmp_path / "b"
    sub.mkdir()
    broken = _warehouse(sub, gaps=[("B", "2022-01-05 00:00:00+00", "2022-01-04 00:00:00+00", 5.0)])
    assert main(["--warehouse", str(broken)]) == 1  # an impossible value fails the run
    assert "[ERROR]" in capsys.readouterr().out


def test_report_lines_mention_skipped_tables(tmp_path):
    wh = _warehouse(tmp_path, gaps=[GOOD_GAP])
    lines = report_lines(run_checks(NEARBY_STARS_CHECKS, wh))
    assert any("[skip]" in line for line in lines)


def test_real_pipeline_flags_the_outlier_but_keeps_it(tmp_path):
    """The 32,651 h gap we met live: it must reach the warehouse AND be flagged, not silently dropped."""
    import pandas as pd

    from datalake.connectors.gfw import run_gap_pipeline

    class Events:
        async def get_all_events(self, **kw):
            class R:
                def df(self_inner):
                    df = pd.DataFrame({
                        "start": pd.to_datetime(["2021-09-30", "2022-01-05"], utc=True),
                        "end": pd.to_datetime(["2025-06-22", "2022-01-06"], utc=True),
                        "vessel": [{"id": "D", "name": "N", "type": "cargo", "flag": "MHL"},
                                   {"id": "A", "name": "N", "type": "fishing", "flag": "RUS"}],
                        "gap": [{"duration_hours": "32651.93", "intentional_disabling": False},
                                {"duration_hours": "20", "intentional_disabling": True}],
                    })
                    return df.iloc[kw["offset"]: kw["offset"] + kw["limit"]]
            return R()

    class Client:
        events = Events()

    wh = tmp_path / "wh.duckdb"
    asyncio.run(run_gap_pipeline(Client(), start_date="2022-01-01", end_date="2022-05-01",
                                 region={"dataset": "public-eez-areas", "id": "5690"},
                                 lake_root=tmp_path, warehouse_path=wh))
    r = _by_name(run_checks(GAP_EVENT_CHECKS, wh))
    assert (r["extreme_duration"].flagged, r["extreme_duration"].total) == (1, 2)
    assert r["end_before_start"].flagged == 0


def test_even_a_badly_written_rule_cannot_delete_anything(tmp_path):
    """The warehouse is opened read-only, so a rule that tries to write is refused."""
    wh = _warehouse(tmp_path, gaps=[GOOD_GAP, GOOD_GAP])
    evil = Check("oops", "ocean.gap_events", "true; DELETE FROM ocean.gap_events", "warning", "bad rule")
    with pytest.raises(duckdb.Error):
        run_checks([evil], wh)
    con = duckdb.connect(str(wh), read_only=True)
    assert con.execute("SELECT count(*) FROM ocean.gap_events").fetchone() == (2,)
