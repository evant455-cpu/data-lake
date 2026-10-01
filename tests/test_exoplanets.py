import duckdb
import pytest

from datalake import quality, rebuild
from datalake.connectors import exoplanets, gaia
from datalake.connectors.exoplanets import (
    EXOPLANET_CHECKS,
    EXOPLANET_COLUMNS,
    GAIA_SOURCE_ID_SQL,
    exoplanets_query,
    land_exoplanets,
    process_raw,
    run_exoplanet_pipeline,
    summary_lines,
)
from datalake.landing import land_raw
from datalake.warehousing import query

HEADER = ",".join(EXOPLANET_COLUMNS)
# pl_name, hostname, gaia_dr3_id, sy_dist, sy_plx, sy_gaiamag, sy_pnum, disc_year, discoverymethod, pl_orbper, pl_bmasse, pl_rade


def _row(name, host="Star A", gaia="Gaia DR3 4472832130942575872", orbper="11.2", mass="1.3", rade="1.1"):
    return f"{name},{host},{gaia},1.30,769.0,9.5,1,2016,Radial Velocity,{orbper},{mass},{rade}"


def _csv(*rows):
    return ("\n".join([HEADER, *rows]) + "\n").encode()


class FakeArchive:
    """Stands in for the Exoplanet Archive. Answers the count question and the table question."""

    def __init__(self, payload, count=None, count_reply=None):
        self.payload, self.queries = payload, []
        n = payload.count(b"\n") - 1
        self.count_reply = count_reply if count_reply is not None else f"count\n{n if count is None else count}\n".encode()

    def __call__(self, adql):
        self.queries.append(adql)
        return self.count_reply if "count(*)" in adql.lower() else self.payload


def _land(tmp_path, payload, **kw):
    arch = FakeArchive(payload, **kw)
    return arch, land_exoplanets(arch, lake_root=tmp_path)


# ---- asking and landing ----

def test_asks_for_every_planet_with_our_columns():
    q = exoplanets_query()
    assert "FROM pscomppars" in q and "TOP" not in q.upper()
    for col in EXOPLANET_COLUMNS:
        assert col in q


def test_lands_the_csv_untouched_and_says_what_it_is(tmp_path):
    payload = _csv(_row("P b"), _row("P c"))
    arch, got = _land(tmp_path, payload)
    assert got.path.relative_to(tmp_path).parts[:3] == ("raw", "exoplanets", "planets")
    assert got.path.read_bytes() == payload
    assert (got.path.parent / f"{got.path.name}.meta.json").exists()
    assert got.rows == 2 and got.complete and got.warning is None
    assert any("count(*)" in q.lower() for q in arch.queries)


def test_more_rows_than_the_count_still_counts_as_complete(tmp_path):
    """A planet added between our two questions must not look like a failure."""
    _, got = _land(tmp_path, _csv(_row("P b"), _row("P c")), count=1)
    assert got.complete


def test_fewer_rows_than_the_archive_says_it_has_is_incomplete(tmp_path):
    _, got = _land(tmp_path, _csv(_row("P b")), count=5)
    assert not got.complete and "1" in got.warning and "5" in got.warning


def test_an_unreadable_count_means_we_cannot_say_complete(tmp_path):
    _, got = _land(tmp_path, _csv(_row("P b")), count_reply=b"<xml>no</xml>")
    assert not got.complete and "verify" in got.warning


def test_a_query_error_reply_is_never_landed(tmp_path):
    xml = b'<?xml version="1.0"?><VOTABLE><INFO name="QUERY_STATUS" value="ERROR">bad column</INFO></VOTABLE>'
    with pytest.raises(RuntimeError, match="not a CSV"):
        land_exoplanets(FakeArchive(xml, count_reply=b"count\n1\n"), lake_root=tmp_path)
    assert not (tmp_path / "raw").exists()


def test_the_same_day_twice_refuses_to_overwrite(tmp_path):
    payload = _csv(_row("P b"))
    _land(tmp_path, payload)
    with pytest.raises(FileExistsError):
        _land(tmp_path, payload)


# ---- cleaning and loading ----

def test_the_pipeline_loads_typed_rows_into_astro_exoplanets(tmp_path):
    wh = tmp_path / "w.duckdb"
    s = run_exoplanet_pipeline(FakeArchive(_csv(_row("P b"), _row("P c", orbper="")), count=2), lake_root=tmp_path, warehouse_path=wh)
    assert s.table == "astro.exoplanets" and s.new == 2 and s.table_total == 2 and s.complete
    rows = query("SELECT pl_name, pl_orbper, sy_pnum, disc_year, gaia_dr3_id FROM astro.exoplanets ORDER BY pl_name", wh)
    assert rows[0] == ("P b", 11.2, 1, 2016, "Gaia DR3 4472832130942575872")
    assert rows[1][1] is None  # a missing period stays missing, not zero


def test_a_revised_planet_replaces_the_old_copy(tmp_path):
    """The archive revises planets as new papers appear: the newer snapshot wins, nothing is duplicated."""
    wh = tmp_path / "w.duckdb"
    a = land_raw(_csv(_row("P b", mass="1.3")), source="exoplanets", dataset="planets", filename="a.csv", lake_root=tmp_path)
    process_raw(a, lake_root=tmp_path, warehouse_path=wh)
    b = land_raw(_csv(_row("P b", mass="2.0"), _row("P c")), source="exoplanets", dataset="planets", filename="b.csv", lake_root=tmp_path)
    s = process_raw(b, lake_root=tmp_path, warehouse_path=wh)
    assert (s.new, s.updated, s.table_total) == (1, 1, 2)
    assert query("SELECT pl_bmasse FROM astro.exoplanets WHERE pl_name = 'P b'", wh) == [(2.0,)]


# ---- the Gaia link ----

def _gaia_star(tmp_path, wh, source_id):
    header = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"
    raw = land_raw(f"{header}\n{source_id},1.0,2.0,769.0,0.1,3.0,4.0,9.5,1.2,\n".encode(),
                   source="gaia", dataset="nearby-stars", filename="s.csv", lake_root=tmp_path)
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)


def test_the_gaia_id_text_becomes_a_number_that_joins_to_our_stars(tmp_path):
    wh = tmp_path / "w.duckdb"
    run_exoplanet_pipeline(FakeArchive(_csv(_row("P b"), _row("Q b", host="Star Q", gaia="Gaia DR3 99")), count=2),
                           lake_root=tmp_path, warehouse_path=wh)
    _gaia_star(tmp_path, wh, 4472832130942575872)
    rows = query(f"SELECT p.pl_name, s.source_id FROM astro.exoplanets p "
                 f"JOIN astro.nearby_stars s ON s.source_id = {GAIA_SOURCE_ID_SQL}", wh)
    assert rows == [("P b", 4472832130942575872)]  # Q b's star is not in our sample


@pytest.mark.parametrize("text, expected", [
    ("Gaia DR3 4472832130942575872", 4472832130942575872),
    ("4472832130942575872", 4472832130942575872),
    ("Gaia DR3 123 ", 123),
    ("Gaia DR2 77", 77),  # the number is taken as written: the release label is not checked here (see the check)
    ("not a number", None),
    ("", None),
])
def test_gaia_id_extraction(text, expected):
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT ? AS gaia_dr3_id", [text])
    assert con.execute(f"SELECT {GAIA_SOURCE_ID_SQL} FROM t").fetchone() == (expected,)


# ---- quality checks, registration, output ----

def _flagged(tmp_path, *rows):
    lake = tmp_path / f"lake{len(list(tmp_path.iterdir()))}"  # a fresh lake per call: a day's raw file is never overwritten
    wh = lake / "w.duckdb"
    run_exoplanet_pipeline(FakeArchive(_csv(*rows)), lake_root=lake, warehouse_path=wh)
    return {r.check.name for r in quality.run_checks(EXOPLANET_CHECKS, wh) if r.flagged}


def test_clean_planets_flag_nothing(tmp_path):
    assert _flagged(tmp_path, _row("P b")) == set()


def test_the_checks_catch_the_impossible_and_the_unmatchable(tmp_path):
    assert _flagged(tmp_path, _row("P b", orbper="-3")) == {"non_positive_orbit"}
    assert _flagged(tmp_path, _row("P b", rade="0")) == {"non_positive_size"}
    assert _flagged(tmp_path, _row("P b", host="")) == {"no_host_star"}
    assert _flagged(tmp_path, _row("P b", gaia="")) == {"no_gaia_id"}
    assert _flagged(tmp_path, _row("P b", gaia="Gaia DR3 abc")) == {"unreadable_gaia_id"}


def test_registered_with_rebuild_and_the_quality_run():
    assert rebuild._handlers()[("exoplanets", "planets")] is exoplanets.process_raw
    assert set(c.name for c in EXOPLANET_CHECKS) <= set(c.name for c in quality.all_checks())


def test_summary_warns_when_incomplete_and_credits_the_archive(tmp_path):
    wh = tmp_path / "w.duckdb"
    s = run_exoplanet_pipeline(FakeArchive(_csv(_row("P b")), count=9), lake_root=tmp_path, warehouse_path=wh)
    text = "\n".join(summary_lines(s))
    assert text.startswith("WARNING: INCOMPLETE FETCH") and "NASA Exoplanet Archive" in text


def test_command_line_re_cleans_a_raw_file_without_network(tmp_path, monkeypatch, capsys):
    raw = land_raw(_csv(_row("P b")), source="exoplanets", dataset="planets", filename="a.csv", lake_root=tmp_path / "lake")
    monkeypatch.chdir(tmp_path)
    assert exoplanets.main(["--raw", str(raw)]) == 0
    assert "astro.exoplanets: 1 new" in capsys.readouterr().out


def test_the_sidecar_records_whether_the_fetch_was_complete(tmp_path):
    import json
    _, got = _land(tmp_path, _csv(_row("P b")), count=5)
    meta = json.loads((got.path.parent / f"{got.path.name}.meta.json").read_text())
    assert meta["complete"] is False and meta["expected_rows"] == 5


def test_a_zero_orbit_is_impossible_too(tmp_path):
    assert _flagged(tmp_path, _row("P b", orbper="0")) == {"non_positive_orbit"}


def test_the_question_orders_planets_so_snapshots_are_comparable():
    assert exoplanets_query().endswith("ORDER BY pl_name")


def test_command_line_exits_with_2_when_the_fetch_is_not_complete(monkeypatch, capsys, tmp_path):
    from pathlib import Path
    bad = exoplanets.ExoplanetSummary(Path("r"), Path("c"), "astro.exoplanets", {}, complete=False, warning="short")
    monkeypatch.setattr(exoplanets, "run_exoplanet_pipeline", lambda: bad)
    assert exoplanets.main([]) == 2
    assert "INCOMPLETE" in capsys.readouterr().out
