"""One command that refreshes the astronomy side of the lake: `python -m datalake.refresh`.

Why it exists: Gaia DR3 is a fixed release (it never changes, and the slice queue already holds all of
it), but the NASA Exoplanet Archive gains new planets every few weeks. So repeating the astronomy work
means three steps, always in this order:

  1. planets         fetch the full list, clean it, upsert into astro.exoplanets
  2. host positions  same for astro.exoplanet_positions
  3. quality checks  Gaia + exoplanet rules on the warehouse (read-only, flag-only)

Every step runs even if an earlier one failed (a network hiccup on step 1 should not hide a quality
problem), and the exit code tells a scheduler what happened:

  0  everything fine
  1  a quality rule of severity `error` flagged rows
  2  a fetch failed or could not be shown complete
(2 wins over 1: a broken fetch matters more than a flagged row.)

"Already fetched today" is NOT a failure: raw files are never overwritten, so a second run on the same
day skips that fetch and carries on with the checks. This makes the command safe to re-run.
GFW is deliberately not part of this command.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from datalake import quality
from datalake.connectors import exoplanets
from datalake.warehousing import DEFAULT_WAREHOUSE


@dataclass
class StepResult:
    name: str
    ok: bool
    lines: list[str] = field(default_factory=list)
    exit_code: int = 0


def astro_checks() -> list[quality.Check]:
    """Only the astronomy rules, so a GFW problem cannot fail an astronomy refresh."""
    from datalake.connectors.gaia import NEARBY_STARS_CHECKS, STAR_ERRORS_CHECKS

    return NEARBY_STARS_CHECKS + STAR_ERRORS_CHECKS + exoplanets.EXOPLANET_CHECKS


def _fetch_step(name: str, dataset, fetch, lake_root: Path, warehouse_path: Path) -> StepResult:
    try:
        summary = exoplanets.run_exoplanet_pipeline(
            fetch, lake_root=lake_root, warehouse_path=warehouse_path, dataset=dataset
        )
    except FileExistsError:
        return StepResult(name, True, ["already fetched today (raw is never overwritten): skipped"])
    except Exception as e:  # network down, archive error reply, ...: report it, keep going
        return StepResult(name, False, [f"FAILED: {type(e).__name__}: {e}"], exit_code=2)
    ok = summary.complete
    return StepResult(name, ok, exoplanets.summary_lines(summary), exit_code=0 if ok else 2)


def _quality_step(warehouse_path: Path) -> StepResult:
    if not Path(warehouse_path).exists():
        return StepResult("quality", False, [f"FAILED: no warehouse at {warehouse_path}"], exit_code=2)
    results = quality.run_checks(astro_checks(), Path(warehouse_path))
    bad = any(r.flagged and r.check.severity == "error" for r in results)
    return StepResult("quality", not bad, quality.report_lines(results), exit_code=1 if bad else 0)


def run_refresh(
    fetch=exoplanets.fetch_tap_csv,
    *,
    lake_root: Path = Path("lake"),
    warehouse_path: Path = DEFAULT_WAREHOUSE,
) -> list[StepResult]:
    return [
        _fetch_step("planets", exoplanets.PLANETS, fetch, lake_root, warehouse_path),
        _fetch_step("host-positions", exoplanets.HOST_POSITIONS, fetch, lake_root, warehouse_path),
        _quality_step(warehouse_path),
    ]


def exit_code(steps: list[StepResult]) -> int:
    codes = {s.exit_code for s in steps}
    return 2 if 2 in codes else 1 if 1 in codes else 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Refresh the astronomy data: planets, host positions, quality checks.")
    p.add_argument("--lake", default="lake")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    a = p.parse_args(argv)
    steps = run_refresh(lake_root=Path(a.lake), warehouse_path=Path(a.warehouse))
    for s in steps:
        print(f"== {s.name}: {'ok' if s.ok else 'PROBLEM'}")
        print("\n".join("   " + line for line in s.lines))
    code = exit_code(steps)
    print({0: "Refresh finished cleanly.", 1: "Refresh finished, but a quality ERROR rule flagged rows.",
           2: "Refresh had a failed or incomplete fetch."}[code])
    return code


if __name__ == "__main__":
    import sys

    sys.exit(main())
