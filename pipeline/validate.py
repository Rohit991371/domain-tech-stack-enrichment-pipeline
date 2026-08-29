"""validate.py -- the validation gate.

This is what makes "silent data loss impossible." arrival_check.py already
confirmed the source partition exists and has a plausible row count, but
that's not enough: the source could have landed with a broken subset (e.g.
a bug in HTTP Archive's own pipeline that month, or extract.py pulling from
the wrong client), and a technically-non-empty result could still be
garbage.

Every check below either PASSes or produces a hard FAIL with a specific
reason. run_pipeline.py treats any FAIL as: do not load this snapshot,
do not touch the previous snapshot, write an alert, exit non-zero.
There is no "load anyway with a warning" path -- if that's ever needed it
should be a deliberate, logged, human-invoked override, not something the
gate itself decides.

Checks, in order:
  1. Snapshot file exists and is parseable JSON.
  2. domain_count >= guardrails.min_domain_count.
  3. tech coverage ratio (domains with >=1 tech / total domains) >=
     guardrails.min_tech_coverage_ratio -- catches a broken detector, not
     just a broken crawl.
  4. Every row has crawl_date matching the requested month (catches a
     mixed-partition bug).
  5. No duplicate domain keys (aggregation bug check).
  6. If a previous month's snapshot exists: domain_count hasn't dropped
     by more than guardrails.max_row_count_drop_pct, and hasn't spiked by
     more than guardrails.max_row_count_spike_pct. Both directions matter --
     a sudden spike is just as often a join fan-out bug as a drop is a
     missing-data bug.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT


@dataclass
class ValidationResult:
    crawl_date: str
    passed: bool
    checks: list[dict] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str):
        self.checks.append({"check": name, "passed": passed, "detail": detail})
        if not passed:
            self.passed = False


def _load_snapshot(crawl_date: str) -> list[dict] | None:
    path = REPO_ROOT / CFG["paths"]["snapshots_dir"] / f"snapshot_{crawl_date}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def validate_snapshot(crawl_date: str, previous_crawl_date: str | None = None, mock: bool = False) -> ValidationResult:
    result = ValidationResult(crawl_date=crawl_date, passed=True)
    g = CFG["guardrails"]

    rows = _load_snapshot(crawl_date)
    if rows is None:
        result.add("snapshot_exists", False, f"No snapshot file for {crawl_date}")
        return result  # everything downstream is meaningless without this
    result.add("snapshot_exists", True, f"{len(rows)} rows")

    domain_count = len(rows)
    min_domains = g["min_domain_count_mock"] if mock else g["min_domain_count"]
    result.add(
        "min_domain_count",
        domain_count >= min_domains,
        f"{domain_count} domains (min {min_domains})",
    )

    with_tech = sum(1 for r in rows if r.get("tech"))
    coverage = with_tech / domain_count if domain_count else 0.0
    result.add(
        "tech_coverage_ratio",
        coverage >= g["min_tech_coverage_ratio"],
        f"{coverage:.2%} of domains have >=1 technology (min {g['min_tech_coverage_ratio']:.0%})",
    )

    bad_dates = [r["domain"] for r in rows if r.get("crawl_date") != crawl_date]
    result.add(
        "crawl_date_consistency",
        len(bad_dates) == 0,
        f"{len(bad_dates)} rows with mismatched crawl_date" if bad_dates
        else "all rows match requested crawl_date",
    )

    domain_keys = [r["domain"] for r in rows]
    dupes = len(domain_keys) - len(set(domain_keys))
    result.add(
        "no_duplicate_domains",
        dupes == 0,
        f"{dupes} duplicate domain keys found" if dupes else "all domain keys unique",
    )

    if previous_crawl_date:
        prev_rows = _load_snapshot(previous_crawl_date)
        if prev_rows is None:
            result.add("row_count_drift", True, f"No previous snapshot for {previous_crawl_date} -- skipping drift check (first run)")
        else:
            prev_count = len(prev_rows)
            if prev_count == 0:
                result.add("row_count_drift", True, "Previous snapshot had 0 domains -- skipping ratio check")
            else:
                ratio = domain_count / prev_count
                drop_floor = 1 - g["max_row_count_drop_pct"]
                spike_ceiling = 1 + g["max_row_count_spike_pct"]
                ok = drop_floor <= ratio <= spike_ceiling
                result.add(
                    "row_count_drift",
                    ok,
                    f"{domain_count} vs previous {prev_count} domains (ratio {ratio:.2f}, "
                    f"allowed [{drop_floor:.2f}, {spike_ceiling:.2f}])",
                )
    else:
        result.add("row_count_drift", True, "No previous_crawl_date supplied -- skipping drift check")

    return result


def main():
    parser = argparse.ArgumentParser(description="Run validation gate against a built snapshot.")
    parser.add_argument("--crawl-date", required=True)
    parser.add_argument("--previous-crawl-date", default=None)
    parser.add_argument("--mock", action="store_true")
    args = parser.parse_args()

    result = validate_snapshot(args.crawl_date, args.previous_crawl_date, mock=args.mock)
    print(json.dumps({"crawl_date": result.crawl_date, "passed": result.passed, "checks": result.checks}, indent=2))
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
