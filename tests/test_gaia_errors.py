import pytest

from datalake import backfill
from datalake.backfill import run_queue
from datalake.connectors import gaia
from datalake.landing import land_raw
from datalake.quality import run_checks
from datalake.rebuild import rebuild
from datalake.warehousing import query

ERR_HEADER = "source_id,radial_velocity_error,rv_nb_transits,pmra_error,pmdec_error"


def _err_csv(*rows):
    """Fake error-bar CSV from (source_id, rv_error, transits) tuples (None = star with no radial velocity)."""
    lines = [ERR_HEADER]
    for sid, err, n in rows:
        lines.append(f"{sid},{'' if err is None else err},{'' if n is None else n},0.05,0.04")
    return ("\n".join(lines) + "\n").encode()


class FakeTap:
    def __init__(self, payload):
        self.payload, self.queries = payload, []

    def __call__(self, adql):
        self.queries.append(adql)
        return self.payload


def test_the_error_question_asks_for_error_columns_but_the_same_stars():
    plain = gaia.nearby_stars_query(limit=10, min_parallax_mas=50, max_parallax_mas=60)
    errors = gaia.nearby_stars_query(limit=10, min_parallax_mas=50, max_parallax_mas=60, dataset=gaia.STAR_ERRORS)
    assert "radial_velocity_error" in errors and "rv_nb_transits" in errors and "pmra_error" in errors
    assert "radial_velocity_error" not in plain  # the original question is unchanged
    # Everything after the column list (which stars, in what order) must be identical, or the tables would not line up.
    assert plain.split(" FROM ", 1)[1] == errors.split(" FROM ", 1)[1]
    assert "parallax_over_error > 10" in errors and "ORDER BY parallax DESC" in errors  # the quality cut itself


def test_error_bars_land_in_their_own_raw_folder(tmp_path):
    got = gaia.land_nearby_stars(FakeTap(_err_csv((1, 0.5, 12))), dataset=gaia.STAR_ERRORS, lake_root=tmp_path)
    assert got.path.relative_to(tmp_path).parts[:3] == ("raw", "gaia", "star-errors")


def test_pipeline_loads_the_error_table_and_keeps_missing_values_missing(tmp_path):
    wh = tmp_path / "w.duckdb"
    tap = FakeTap(_err_csv((1, 0.5, 12), (2, None, None)))
    s = gaia.run_nearby_stars_pipeline(tap, dataset=gaia.STAR_ERRORS, lake_root=tmp_path, warehouse_path=wh)
    assert s.table == "astro.star_errors" and (s.new, s.table_total) == (2, 2)
    assert query("SELECT source_id, radial_velocity_error, rv_nb_transits FROM astro.star_errors ORDER BY 1", wh) == [
        (1, 0.5, 12), (2, None, None)]


def test_refetching_updates_by_source_id(tmp_path):
    wh = tmp_path / "w.duckdb"
    gaia.run_nearby_stars_pipeline(FakeTap(_err_csv((1, 0.5, 12))), dataset=gaia.STAR_ERRORS,
                                   lake_root=tmp_path, warehouse_path=wh)
    s = gaia.run_nearby_stars_pipeline(FakeTap(_err_csv((1, 0.4, 14), (3, 1.0, 3))), dataset=gaia.STAR_ERRORS,
                                       min_parallax_mas=40, lake_root=tmp_path, warehouse_path=wh)
    assert (s.new, s.updated, s.table_total) == (1, 1, 2)


def test_the_original_star_table_is_not_touched_by_the_error_table(tmp_path):
    wh = tmp_path / "w.duckdb"
    stars = ("source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity\n"
             "1,1.0,2.0,60.0,0.1,3.0,4.0,9.5,1.2,5.0\n").encode()
    raw = land_raw(stars, source="gaia", dataset="nearby-stars", filename="s.csv", lake_root=tmp_path)
    gaia.process_raw(raw, lake_root=tmp_path, warehouse_path=wh)
    gaia.run_nearby_stars_pipeline(FakeTap(_err_csv((1, 0.5, 12))), dataset=gaia.STAR_ERRORS,
                                   lake_root=tmp_path, warehouse_path=wh)
    assert query("SELECT count(*) FROM astro.nearby_stars", wh) == [(1,)]
    assert query("SELECT s.source_id, e.radial_velocity_error FROM astro.nearby_stars s "
                 "JOIN astro.star_errors e USING (source_id)", wh) == [(1, 0.5)]


def test_rebuild_knows_how_to_replay_error_files(tmp_path):
    land_raw(_err_csv((1, 0.5, 12)), source="gaia", dataset="star-errors", filename="e.csv", lake_root=tmp_path)
    result = rebuild(tmp_path, tmp_path / "rebuilt.duckdb")
    assert result.tables == {"astro.star_errors": 1} and result.skipped == []


def test_an_impossible_negative_error_is_flagged(tmp_path):
    wh = tmp_path / "w.duckdb"
    gaia.run_nearby_stars_pipeline(FakeTap(_err_csv((1, 0.5, 12), (2, -1.0, 3))), dataset=gaia.STAR_ERRORS,
                                   lake_root=tmp_path, warehouse_path=wh)
    results = {r.check.name: r for r in run_checks(gaia.STAR_ERRORS_CHECKS, wh)}
    assert results["negative_error"].flagged == 1


# ---- the walk through the sky, shell by shell, for the error table ----

def test_error_shells_cover_the_inner_sky_in_big_steps_then_the_same_shells_as_before(tmp_path):
    plan = gaia.star_error_slices(lake_root=tmp_path, warehouse_path=tmp_path / "w.duckdb")
    ids = [s.id for s in plan]
    assert ids[0] == "gaia/star-errors/1-10pc" and ids[-1] == "gaia/star-errors/28-30pc"
    assert ids[3:] == [f"gaia/star-errors/{a}-{a + 2}pc" for a in (20, 22, 24, 26, 28)]
    assert len(ids) == len(set(ids)) == 8


def test_an_error_shell_runs_through_the_queue_and_a_full_one_stays_pending(tmp_path):
    wh, state = tmp_path / "w.duckdb", tmp_path / "state.json"
    ok = gaia.star_error_slices(fetch=FakeTap(_err_csv((1, 0.5, 12))), lake_root=tmp_path, warehouse_path=wh)
    r = run_queue(ok, state)
    assert r.stopped is None and query("SELECT count(*) FROM astro.star_errors", wh) == [(1,)]
    full = gaia.star_error_slices(fetch=FakeTap(_err_csv((1, 0.5, 1), (2, 0.5, 1))), limit=2,
                                  lake_root=tmp_path / "b", warehouse_path=tmp_path / "w2.duckdb")
    r2 = run_queue(full, tmp_path / "state2.json")
    assert "incomplete" in r2.stopped


def test_the_default_backfill_plan_includes_both_walks(tmp_path):
    ids = [s.id for s in backfill._plans(tmp_path, tmp_path / "w.duckdb")]
    assert any(i.startswith("gaia/nearby-stars/") for i in ids) and any(i.startswith("gaia/star-errors/") for i in ids)


def test_the_error_rule_is_part_of_the_normal_quality_run():
    from datalake.quality import all_checks
    assert "negative_error" in [c.name for c in all_checks()]
