import asyncio
import sys
import types
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from datalake.connectors import gfw
from datalake.connectors.gfw import covered_through, plan_incremental, run_incremental
from datalake.landing import land_raw

TODAY = date(2026, 10, 1)
REGION = {"dataset": "public-eez-areas", "id": "5690"}
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def _raw(lake, name, complete=None, minutes=0):
    """Land an empty raw gap file. complete=None writes a 'legacy' sidecar without the field."""
    extra = None if complete is None else {"complete": complete}
    return land_raw(b"[]", source="gfw", dataset="gap-events", filename=name, lake_root=lake,
                    now=NOW + timedelta(minutes=minutes), extra_meta=extra)


def _name(start, end, region_id="5690"):
    return f"{start}_{end}_public-eez-areas-{region_id}.json"


# ---- fake GFW: pages by offset, like the real thing ----

def _events(n):
    return pd.DataFrame({
        "start": pd.to_datetime([f"2022-01-{1 + i % 28:02d}" for i in range(n)], utc=True),
        "end": pd.to_datetime([f"2022-01-{1 + i % 28:02d}" for i in range(n)], utc=True) + pd.Timedelta(hours=5),
        "vessel": [{"id": f"V{i}", "name": "N", "type": "fishing", "flag": "RUS"} for i in range(n)],
        "gap": [{"duration_hours": "5", "intentional_disabling": True} for _ in range(n)],
    })


class _Events:
    def __init__(self, df, ignore_offset=False):
        self.df, self.calls, self.ignore_offset = df, [], ignore_offset

    async def get_all_events(self, **kw):
        self.calls.append(kw)
        off = 0 if self.ignore_offset else kw["offset"]

        class R:
            def df(_self):
                return self.df.iloc[off: off + kw["limit"]]
        return R()


class _Client:
    def __init__(self, df, ignore_offset=False):
        self.events = _Events(df, ignore_offset)


def _run(tmp_path, client, **kw):
    kw.setdefault("today", TODAY)
    return asyncio.run(run_incremental(
        client, region=REGION, lake_root=tmp_path / "lake", warehouse_path=tmp_path / "wh.duckdb", **kw))


# ---- covered_through: how far have we complete data for this region? ----

def test_nothing_landed_means_no_coverage(tmp_path):
    assert covered_through(tmp_path / "lake", REGION) is None


def test_coverage_is_the_latest_end_date_of_complete_files_for_this_region(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2022-01-01", "2022-05-01"), complete=True)
    _raw(lake, _name("2022-05-01", "2022-07-01"), complete=True, minutes=1)
    _raw(lake, _name("2022-01-01", "2023-01-01", region_id="1234"), complete=True, minutes=2)  # other region
    _raw(lake, "notes.json", complete=True, minutes=3)  # not one of ours
    assert covered_through(lake, REGION) == date(2022, 7, 1)


def test_an_incomplete_fetch_does_not_count_as_coverage(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2022-01-01", "2022-05-01"), complete=True)
    _raw(lake, _name("2022-05-01", "2022-12-01"), complete=False, minutes=1)  # cut off: must not advance
    assert covered_through(lake, REGION) == date(2022, 5, 1)


def test_files_from_before_the_complete_flag_existed_count_as_complete(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2022-05-01", "2022-07-01"), complete=None)  # legacy sidecar, no field
    assert covered_through(lake, REGION) == date(2022, 7, 1)


# ---- plan_incremental: what window to ask for next ----

def test_first_ever_run_needs_a_start_date(tmp_path):
    with pytest.raises(ValueError, match="--since"):
        plan_incremental(tmp_path / "lake", REGION, today=TODAY)


def test_since_sets_the_first_window_and_the_window_is_capped(tmp_path):
    p = plan_incremental(tmp_path / "lake", REGION, today=TODAY, since="2022-01-01", max_days=90)
    assert (p.start_date, p.end_date, p.catching_up) == ("2022-01-01", str(date(2022, 1, 1) + timedelta(days=90)), True)


def test_next_window_starts_a_week_before_coverage_ends(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2022-05-01", "2022-07-01"))
    p = plan_incremental(lake, REGION, today=TODAY, overlap_days=7, max_days=90)
    start = date(2022, 7, 1) - timedelta(days=7)
    assert (p.start_date, p.end_date) == (str(start), str(start + timedelta(days=90)))
    assert p.catching_up is True


def test_close_to_today_the_window_ends_today_and_is_not_catching_up(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2026-09-01", "2026-09-28"))
    p = plan_incremental(lake, REGION, today=TODAY)
    assert (p.start_date, p.end_date, p.catching_up) == ("2026-09-21", "2026-10-01", False)


def test_already_covered_through_today_means_nothing_to_do(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2026-09-01", "2026-10-01"))
    assert plan_incremental(lake, REGION, today=TODAY) is None


# ---- run_incremental: plan, fetch, clean, load ----

def test_catch_up_walks_forward_chunk_by_chunk_then_stops(tmp_path):
    _raw(tmp_path / "lake", _name("2022-05-01", "2022-07-01"))
    client = _Client(_events(3))
    seen = []
    for _ in range(10):
        run = _run(tmp_path, client, today=date(2022, 12, 1), max_days=90)
        if run is None:
            break
        seen.append((run.plan.start_date, run.plan.end_date))
    assert seen == [("2022-06-24", "2022-09-22"), ("2022-09-15", "2022-12-01")]
    assert client.events.calls[0]["start_date"] == "2022-06-24"  # the plan is what GFW is asked
    assert covered_through(tmp_path / "lake", REGION) == date(2022, 12, 1)


def test_second_run_the_same_day_makes_no_requests(tmp_path):
    _raw(tmp_path / "lake", _name("2026-09-01", "2026-09-28"))
    client = _Client(_events(3))
    assert _run(tmp_path, client) is not None
    n = len(client.events.calls)
    assert _run(tmp_path, client) is None
    assert len(client.events.calls) == n


def test_run_loads_events_into_the_warehouse(tmp_path):
    from datalake.warehousing import query

    _raw(tmp_path / "lake", _name("2026-09-01", "2026-09-28"))
    run = _run(tmp_path, _Client(_events(3)))
    assert run.summary.new == 3 and run.summary.complete
    assert query("SELECT count(*) FROM ocean.gap_events", tmp_path / "wh.duckdb") == [(3,)]


def test_a_cut_off_fetch_is_flagged_and_does_not_move_the_start_forward(tmp_path):
    lake = tmp_path / "lake"
    _raw(lake, _name("2022-05-01", "2022-07-01"))
    run = _run(tmp_path, _Client(_events(250), ignore_offset=True), today=date(2022, 12, 1))
    assert run.summary.complete is False and run.summary.warning
    assert covered_through(lake, REGION) == date(2022, 7, 1)  # unchanged, so the next run retries it
    assert plan_incremental(lake, REGION, today=date(2022, 12, 1)).start_date == run.plan.start_date


# ---- command line ----

@pytest.fixture
def cli(tmp_path, monkeypatch):
    """Run gfw.main() inside tmp_path with a stand-in GFW client and a fixed 'today'."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GFW_API_ACCESS_TOKEN", "test-token")
    monkeypatch.setattr(gfw, "_utc_today", lambda: TODAY)
    state = {"client": _Client(_events(3))}
    fake = types.ModuleType("gfwapiclient")
    fake.Client = lambda access_token: state["client"]
    monkeypatch.setitem(sys.modules, "gfwapiclient", fake)

    def call(*args):
        monkeypatch.setattr(sys, "argv", ["gfw", *args])
        return gfw.main()
    call.state, call.lake = state, tmp_path / "lake"
    return call


def test_cli_incremental_runs_then_reports_up_to_date(cli, capsys):
    _raw(cli.lake, _name("2026-09-01", "2026-09-28"))
    assert cli("--incremental") == 0
    out = capsys.readouterr().out
    assert "2026-09-21" in out and "2026-10-01" in out and "Warehouse" in out
    assert cli("--incremental") == 0
    assert "up to date" in capsys.readouterr().out.lower()


def test_cli_incremental_needs_since_the_first_time(cli):
    with pytest.raises(SystemExit) as e:
        cli("--incremental")
    assert "--since" in str(e.value)


def test_cli_incremental_with_since_starts_there(cli, capsys):
    assert cli("--incremental", "--since", "2026-09-20") == 0
    assert "2026-09-20" in capsys.readouterr().out


def test_cli_exit_code_2_when_the_fetch_may_be_incomplete(cli, capsys):
    _raw(cli.lake, _name("2026-09-01", "2026-09-28"))
    cli.state["client"] = _Client(_events(250), ignore_offset=True)
    assert cli("--incremental") == 2
    assert "INCOMPLETE" in capsys.readouterr().out


def test_cli_one_off_fetch_still_works(cli, capsys):
    assert cli("2022-01-01", "2022-02-01") == 0
    assert "Warehouse" in capsys.readouterr().out
