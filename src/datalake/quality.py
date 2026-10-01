"""Lesson 6: data-quality checks.

A check is a rule that describes a SUSPICIOUS ROW in a warehouse table, written as a SQL condition,
e.g. "a gap that ends before it starts". Running the checks counts and shows the rows that match.

Golden rule: checks only FLAG, they never change or delete data. A strange value may be a real
discovery (a gap of 3.7 years is real GFW data), so a person decides what it means.

Two severities:
  error    = impossible (an end time before its start). Fails the run (exit code 1).
  warning  = merely suspicious (a gap longer than a year). Reported, does not fail the run.

Rules live next to each source's schema (GAP_EVENT_CHECKS in connectors/gfw.py,
NEARBY_STARS_CHECKS in connectors/gaia.py). Run all of them with:  python -m datalake.quality
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from datalake.warehousing import DEFAULT_WAREHOUSE, _check_name, _connect_read_only

SEVERITIES = ("error", "warning")


@dataclass(frozen=True)
class Check:
    name: str  # short label, e.g. "extreme_duration"
    table: str  # "<schema>.<table>"
    where: str  # SQL condition that is TRUE for a suspicious row (written by us, never from user input)
    severity: str  # "error" or "warning"
    why: str  # one plain sentence on what a flagged row means

    def __post_init__(self):
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity must be one of {SEVERITIES}, got {self.severity!r}")
        parts = self.table.split(".")
        if len(parts) != 2:
            raise ValueError(f"table must look like schema.table, got {self.table!r}")
        for part in parts:
            _check_name(part)


@dataclass
class CheckResult:
    check: Check
    total: int = 0  # rows in the table
    flagged: int = 0  # rows matching the rule
    examples: list[dict] = field(default_factory=list)  # a few flagged rows, to look at
    skipped: str | None = None  # why the check could not run (e.g. table not loaded yet)


def run_checks(checks: list[Check], warehouse_path: Path = DEFAULT_WAREHOUSE, max_examples: int = 3) -> list[CheckResult]:
    """Run each check against the warehouse (read-only) and return one result per check."""
    results = []
    con = _connect_read_only(warehouse_path)
    try:
        for check in checks:
            try:
                total = con.execute(f"SELECT count(*) FROM {check.table}").fetchone()[0]
                flagged = con.execute(f"SELECT count(*) FROM {check.table} WHERE {check.where}").fetchone()[0]
                examples = []
                if flagged:
                    cur = con.execute(f"SELECT * FROM {check.table} WHERE {check.where} LIMIT {int(max_examples)}")
                    names = [d[0] for d in cur.description]
                    examples = [dict(zip(names, row)) for row in cur.fetchall()]
                results.append(CheckResult(check, total, flagged, examples))
            except duckdb.CatalogException:
                results.append(CheckResult(check, skipped=f"table not found: {check.table} (not loaded yet?)"))
    finally:
        con.close()
    return results


def report_lines(results: list[CheckResult]) -> list[str]:
    """Plain-text report: one line per check, flagged rows shown underneath."""
    lines = []
    for r in results:
        c = r.check
        if r.skipped:
            lines.append(f"[skip]  {c.table} {c.name}: {r.skipped}")
        elif not r.flagged:
            lines.append(f"[ok]    {c.table} {c.name}: 0 of {r.total} rows")
        else:
            tag = "[ERROR]" if c.severity == "error" else "[WARN] "
            lines.append(f"{tag} {c.table} {c.name}: {r.flagged} of {r.total} rows. {c.why}")
            for row in r.examples:
                lines.append("          " + ", ".join(f"{k}={v}" for k, v in row.items() if not k.startswith("_")))
    errors = sum(1 for r in results if r.flagged and r.check.severity == "error")
    warnings = sum(1 for r in results if r.flagged and r.check.severity == "warning")
    lines.append(f"{errors} rule(s) with errors, {warnings} with warnings, nothing changed or deleted.")
    return lines


def all_checks() -> list[Check]:
    """Every source's rules. Imported here (not at the top) so sources can import Check from this file."""
    from datalake.connectors.gaia import NEARBY_STARS_CHECKS, STAR_ERRORS_CHECKS
    from datalake.connectors.gfw import GAP_EVENT_CHECKS

    return GAP_EVENT_CHECKS + NEARBY_STARS_CHECKS + STAR_ERRORS_CHECKS


def main(argv: list[str] | None = None) -> int:
    """Command line: python -m datalake.quality [--warehouse PATH]. Exit code 1 if any error rule flags rows."""
    import argparse

    p = argparse.ArgumentParser(description="Run data-quality checks on the warehouse (read-only).")
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    a = p.parse_args(argv)
    wh = Path(a.warehouse)
    if not wh.exists():
        raise SystemExit(f"No warehouse at {wh}. Run a connector first.")
    results = run_checks(all_checks(), wh)
    print("\n".join(report_lines(results)))
    return 1 if any(r.flagged and r.check.severity == "error" for r in results) else 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
