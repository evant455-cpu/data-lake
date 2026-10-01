"""Step 4 of the scheduling plan: a slice queue for slow backfills.

Some data is too big to fetch in one go (Gaia stars shell by shell, years of GFW events). We cut the job
into SLICES, each small enough to fetch completely, and walk through them a few at a time.

What the queue adds is memory. A small state file records which slices are DONE, so every run (today,
tomorrow, from a GitHub runner) picks up at the next slice instead of starting over.

Rules:
  - A slice counts as done only if its fetch was COMPLETE. A cut-off or crashed slice stays pending.
  - The first slice that is cut off or crashes STOPS the run. Something is wrong (a rate limit, a shell
    with too many stars) and fetching more would just pile up trouble.
  - Progress is saved after every slice, so a crash never loses finished work.
  - The state file is never silently reset: if it is damaged, we stop and say so.
  - By default one slice per run. That is the throttle.

Run:  python -m datalake.backfill [--status] [--max-slices N]
Sources add their slices in `_plans` below (one line per source).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from datalake.warehousing import DEFAULT_WAREHOUSE


@dataclass
class SliceOutcome:
    complete: bool  # True only if we know this slice holds everything it should
    message: str  # one line for the log and the state file


@dataclass(frozen=True)
class Slice:
    id: str  # stable name, e.g. "gaia/nearby-stars/20-22pc". Never reuse an id for different work.
    run: Callable[[], SliceOutcome]


@dataclass
class RanSlice:
    id: str
    outcome: SliceOutcome


@dataclass
class QueueRun:
    ran: list[RanSlice] = field(default_factory=list)
    remaining: int = 0  # slices still pending after this run
    stopped: str | None = None  # why the run stopped early, if it did


def load_state(state_path: Path) -> dict:
    """slice id -> {"done_at_utc", "note"}. No file yet = nothing done. A damaged file is an error."""
    if not state_path.exists():
        return {}
    try:
        done = json.loads(state_path.read_text(encoding="utf-8"))["done"]
        if not isinstance(done, dict):
            raise TypeError("'done' is not a table")
        return done
    except (ValueError, KeyError, TypeError) as e:
        raise RuntimeError(
            f"The backfill state file {state_path} is damaged ({e}). Not touching it: look at it, "
            "fix or delete it on purpose. Deleting it only means finished slices are fetched again."
        ) from e


def _save_state(state_path: Path, done: dict) -> None:
    """Write to a temp file, then swap it in, so a crash can never leave half a state file."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_name(state_path.name + ".tmp")
    tmp.write_text(json.dumps({"done": done}, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, state_path)


def _check_plan(plan: list[Slice]) -> None:
    seen = set()
    for s in plan:
        if s.id in seen:
            raise ValueError(f"Slice id {s.id!r} appears twice in the plan. Ids must be unique.")
        seen.add(s.id)


def pending_slices(plan: list[Slice], state_path: Path) -> list[Slice]:
    done = load_state(state_path)
    return [s for s in plan if s.id not in done]


def run_queue(plan: list[Slice], state_path: Path, *, max_slices: int = 1, now: datetime | None = None) -> QueueRun:
    """Run up to `max_slices` pending slices, in plan order, remembering the ones that finish."""
    if max_slices < 1:
        raise ValueError("max_slices must be at least 1")
    _check_plan(plan)
    done = load_state(state_path)
    todo = [s for s in plan if s.id not in done]
    result = QueueRun(remaining=len(todo))

    for s in todo[:max_slices]:
        try:
            outcome = s.run()
        except Exception as e:  # keep going is the wrong move: report and stop
            result.stopped = f"{s.id} failed: {type(e).__name__}: {e}"
            return result
        result.ran.append(RanSlice(s.id, outcome))
        if not outcome.complete:
            result.stopped = f"{s.id} was incomplete (not marked done): {outcome.message}"
            return result
        done[s.id] = {
            "done_at_utc": (now or datetime.now(timezone.utc)).isoformat(),
            "note": outcome.message,
        }
        _save_state(state_path, done)  # saved right away: a later crash cannot lose this
        result.remaining -= 1
    return result


def _plans(lake_root: Path, warehouse_path: Path) -> list[Slice]:
    """Every source's slices, in the order they should be walked. A new source adds one line here."""
    from datalake.connectors import gaia

    return gaia.band_slices(lake_root=lake_root, warehouse_path=warehouse_path)


def main(argv: list[str] | None = None) -> int:
    """Command line.

    See what is done and pending (changes nothing):  python -m datalake.backfill --status
    Run the next slice:                              python -m datalake.backfill
    Run several:                                     python -m datalake.backfill --max-slices 3
    Exit code 2 if a slice was cut off or failed (so a scheduled run can flag it).
    """
    import argparse

    p = argparse.ArgumentParser(description="Walk through slow backfills one slice at a time.")
    p.add_argument("--lake", default="lake")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--max-slices", type=int, default=1, help="slices to run this time (default 1, the throttle)")
    p.add_argument("--status", action="store_true", help="only list what is done and pending")
    a = p.parse_args(argv)

    lake = Path(a.lake)
    state_path = lake / "state" / "backfill.json"
    plan = _plans(lake, Path(a.warehouse))

    if a.status:
        done = load_state(state_path)
        for s in plan:
            print(f"[done]    {s.id}  ({done[s.id]['note']})" if s.id in done else f"[pending] {s.id}")
        n_pending = sum(1 for s in plan if s.id not in done)
        print(f"{len(plan) - n_pending} done, {n_pending} pending.")
        return 0

    if not pending_slices(plan, state_path):
        print("Nothing pending. Every slice in the plan is done.")
        return 0
    r = run_queue(plan, state_path, max_slices=a.max_slices)
    for item in r.ran:
        print(f"{'Done' if item.outcome.complete else 'INCOMPLETE'}: {item.id}. {item.outcome.message}")
    if r.stopped:
        print(f"STOPPED: {r.stopped}")
    print(f"{r.remaining} slice(s) still pending.")
    return 2 if r.stopped else 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
