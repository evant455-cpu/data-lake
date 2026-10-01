import json
from datetime import datetime, timezone

import pytest

from datalake import backfill
from datalake.backfill import Slice, SliceOutcome, load_state, main, run_queue
from datalake.connectors import gaia
from datalake.warehousing import query

NOW = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)


def _slice(name, log, *, complete=True, boom=None):
    """A fake slice: records that it ran, then returns an outcome (or raises)."""
    def run():
        log.append(name)
        if boom:
            raise boom
        return SliceOutcome(complete=complete, message=f"{name} ok")
    return Slice(id=name, run=run)


def _plan(log, *names):
    return [_slice(n, log) for n in names]


# ---- the engine: one slice per run, remember what is done ----

def test_default_runs_only_the_first_pending_slice_and_remembers_it(tmp_path):
    log, state = [], tmp_path / "state.json"
    r = run_queue(_plan(log, "a", "b", "c"), state, now=NOW)
    assert log == ["a"] and [x.id for x in r.ran] == ["a"] and r.remaining == 2 and r.stopped is None
    assert set(load_state(state)) == {"a"}


def test_next_run_skips_what_is_done(tmp_path):
    log, state = [], tmp_path / "state.json"
    run_queue(_plan(log, "a", "b", "c"), state, now=NOW)
    run_queue(_plan(log, "a", "b", "c"), state, now=NOW)
    assert log == ["a", "b"]


def test_max_slices_runs_several_in_order_and_then_nothing_is_left(tmp_path):
    log, state = [], tmp_path / "state.json"
    r = run_queue(_plan(log, "a", "b", "c"), state, max_slices=5, now=NOW)
    assert log == ["a", "b", "c"] and r.remaining == 0
    again = run_queue(_plan(log, "a", "b", "c"), state, max_slices=5, now=NOW)
    assert log == ["a", "b", "c"] and again.ran == [] and again.remaining == 0


def test_an_incomplete_slice_is_not_marked_done_and_stops_the_queue(tmp_path):
    log, state = [], tmp_path / "state.json"
    plan = [_slice("a", log), _slice("b", log, complete=False), _slice("c", log)]
    r = run_queue(plan, state, max_slices=5, now=NOW)
    assert log == ["a", "b"]  # c never ran
    assert set(load_state(state)) == {"a"} and r.remaining == 2
    assert "b" in r.stopped and "incomplete" in r.stopped


def test_a_crash_is_reported_not_marked_and_keeps_earlier_progress(tmp_path):
    log, state = [], tmp_path / "state.json"
    plan = [_slice("a", log), _slice("b", log, boom=RuntimeError("429 Too Many Requests")), _slice("c", log)]
    r = run_queue(plan, state, max_slices=5, now=NOW)
    assert log == ["a", "b"] and set(load_state(state)) == {"a"}
    assert "b" in r.stopped and "429" in r.stopped


def test_progress_is_saved_after_each_slice(tmp_path):
    """If the process dies during slice b, slice a must already be on disk."""
    state, seen = tmp_path / "state.json", []

    def watcher():
        seen.append(set(load_state(state)))
        return SliceOutcome(True, "ok")

    run_queue([_slice("a", []), Slice("b", watcher)], state, max_slices=2, now=NOW)
    assert seen == [{"a"}]


def test_a_slice_added_later_is_picked_up(tmp_path):
    log, state = [], tmp_path / "state.json"
    run_queue(_plan(log, "a"), state, max_slices=5, now=NOW)
    run_queue(_plan(log, "a", "b"), state, max_slices=5, now=NOW)
    assert log == ["a", "b"]


def test_state_records_when_and_what(tmp_path):
    state = tmp_path / "sub" / "state.json"  # folder is created for us
    run_queue(_plan([], "a"), state, now=NOW)
    entry = load_state(state)["a"]
    assert entry["done_at_utc"] == "2026-10-01T06:00:00+00:00" and entry["note"] == "a ok"
    assert not list(state.parent.glob("*.tmp"))  # temp file from the safe write is gone


def test_a_damaged_state_file_is_an_error_not_a_silent_reset(tmp_path):
    state = tmp_path / "state.json"
    state.write_text("{not json")
    with pytest.raises(RuntimeError, match="state.json"):
        run_queue(_plan([], "a"), state, now=NOW)
    assert state.read_text() == "{not json"  # left alone for a person to look at


def test_duplicate_slice_ids_are_refused(tmp_path):
    with pytest.raises(ValueError, match="twice"):
        run_queue(_plan([], "a", "a"), tmp_path / "s.json", now=NOW)


def test_max_slices_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        run_queue(_plan([], "a"), tmp_path / "s.json", max_slices=0, now=NOW)


# ---- Gaia distance shells as slices ----

class FakeTap:
    def __init__(self, rows_per_call=2):
        self.queries, self.n, self.rows = [], 0, rows_per_call

    def __call__(self, adql):
        self.queries.append(adql)
        lines = ["source_id,ra,dec,parallax,parallax_error,pmra,pmdec,phot_g_mean_mag,bp_rp,radial_velocity"]
        for _ in range(self.rows):
            self.n += 1
            lines.append(f"{self.n},1.0,2.0,47.0,0.1,3.0,4.0,9.5,1.2,")
        return ("\n".join(lines) + "\n").encode()


def test_shells_are_contiguous_and_start_at_the_20pc_edge(tmp_path):
    plan = gaia.band_slices(near_pc=20, far_pc=26, step_pc=2, lake_root=tmp_path, warehouse_path=tmp_path / "w.duckdb")
    assert [s.id for s in plan] == [
        "gaia/nearby-stars/20-22pc", "gaia/nearby-stars/22-24pc", "gaia/nearby-stars/24-26pc"]
    edges = gaia.shell_edges_mas(20, 26, 2)
    assert edges[0][0] == 50.0
    assert edges[-1][1] == 38.4615  # 26 pc, rounded to 4 decimals like every other edge  # 20 pc is 50 mas: where the existing 2626-star sphere ends
    # each shell's far edge (small parallax) is the next shell's near edge (large parallax): no gap, no overlap
    assert [far for _, far in edges[:-1]] == [near for near, _ in edges[1:]]


def test_bad_shell_settings_are_refused(tmp_path):
    with pytest.raises(ValueError):
        gaia.shell_edges_mas(20, 20, 2)
    with pytest.raises(ValueError):
        gaia.shell_edges_mas(20, 30, 0)


def test_a_gaia_shell_runs_through_the_real_pipeline_and_gets_marked_done(tmp_path):
    wh, state, tap = tmp_path / "w.duckdb", tmp_path / "state.json", FakeTap()
    plan = gaia.band_slices(near_pc=20, far_pc=24, step_pc=2, fetch=tap, lake_root=tmp_path, warehouse_path=wh)
    r = run_queue(plan, state, now=NOW)
    assert r.stopped is None and [x.id for x in r.ran] == ["gaia/nearby-stars/20-22pc"]
    assert "parallax > 45.4545" in tap.queries[0] and "parallax <= 50" in tap.queries[0]
    assert query("SELECT count(*) FROM astro.nearby_stars", wh) == [(2,)]
    assert "gaia/nearby-stars/20-22pc" in load_state(state)


def test_a_shell_that_hits_the_row_limit_stays_pending(tmp_path):
    wh, state = tmp_path / "w.duckdb", tmp_path / "state.json"
    tap = FakeTap(rows_per_call=3)
    plan = gaia.band_slices(near_pc=20, far_pc=24, step_pc=2, limit=3, fetch=tap, lake_root=tmp_path, warehouse_path=wh)
    r = run_queue(plan, state, now=NOW)
    assert load_state(state) == {} and "incomplete" in r.stopped and r.remaining == 2


# ---- command line ----

def test_command_line_runs_status_and_exit_codes(tmp_path, monkeypatch, capsys):
    log = []
    monkeypatch.setattr(backfill, "_plans", lambda lake, wh: _plan(log, "a", "b"))
    base = ["--lake", str(tmp_path / "lake"), "--warehouse", str(tmp_path / "w.duckdb")]
    assert main(base + ["--status"]) == 0 and log == []  # status never runs anything
    out = capsys.readouterr().out
    assert "pending" in out and "a" in out
    assert main(base) == 0 and log == ["a"]
    assert (tmp_path / "lake" / "state" / "backfill.json").exists()
    monkeypatch.setattr(backfill, "_plans", lambda lake, wh: [_slice("z", log, complete=False)])
    assert main(base) == 2
    assert "STOPPED" in capsys.readouterr().out


def test_nothing_pending_says_so(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(backfill, "_plans", lambda lake, wh: [])
    assert main(["--lake", str(tmp_path / "lake"), "--warehouse", str(tmp_path / "w.duckdb")]) == 0
    assert "Nothing pending" in capsys.readouterr().out
