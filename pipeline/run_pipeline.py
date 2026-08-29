"""run_pipeline.py -- one command, the whole pipeline.

    Arrival Checker -> Validation Gate(pre) -> Production SQL (extract) ->
    Domain Normalizer + Origin Aggregator (build_snapshot) ->
    Quality Gates (validate) -> [FAIL -> STOP, alert] / [PASS -> Load] ->
    Diff Engine (vs previous snapshot) -> Change Events

This mirrors the architecture diagram in docs/design_doc.md. Every stage
writes its own artifact to disk (data/raw, data/snapshots,
data/change_events, data/rejected) so a failure at any stage leaves a clear,
inspectable trail instead of a stack trace and nothing else -- this is what
"what pages a human at 3 AM" is supposed to look at.

Add --land to any real (non-mock, non-sample) run to also land the
validated snapshot (+ change events, if a diff ran) into the BigQuery
warehouse after a successful local load. See pipeline/land_to_warehouse.py.

Three run modes:

  --mock                          Fully offline, synthetic fixtures. Fast dev loop.
  --source sample                 Real query against the free, small
                                   sample_data.pages_10k table. No arrival check
                                   (that table has no timing trap -- it's a
                                   static mirror of the latest crawl, not a
                                   dated monthly partition), no diff (only one
                                   snapshot exists), production-scale validation
                                   thresholds skipped in favor of the smaller
                                   mock-scale ones since 10k rows is still much
                                   smaller than a real production month.
  (default) --crawl-date ...      Real run against httparchive.crawl.pages.
                                   Add --limit-rows N to cap what's downloaded
                                   to disk (proves the pipeline runs correctly
                                   against real production data without pulling
                                   millions of rows) -- see extract.py's
                                   docstring for why this doesn't reduce billed
                                   cost. A --limit-rows run also uses the
                                   mock-scale validation thresholds, since a
                                   deliberately capped extract won't clear the
                                   full production min_domain_count.

Usage:
    # local dev, no BigQuery/GCP needed:
    python pipeline/run_pipeline.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01 --mock

    # real, free, small -- run this before ever touching production:
    python pipeline/run_pipeline.py --source sample

    # real production, but capped to a small proof download:
    python pipeline/run_pipeline.py --crawl-date 2026-08-01 --limit-rows 500

    # real production, full month (millions of rows, dry-run-gated on cost):
    python pipeline/run_pipeline.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01

Exit codes:
    0  everything ran and the new snapshot was loaded (+ diff, if applicable)
    1  arrival check failed -- source not ready, nothing was touched
    2  validation gate failed -- snapshot built but NOT loaded/promoted,
       previous good snapshot is untouched
    3  unexpected error in an earlier stage (extract/build)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT, state_file_path
from pipeline.arrival_check import check_arrival
from pipeline.build_snapshot import build_snapshot
from pipeline.diff_snapshots import diff_snapshots
from pipeline.extract import extract
from pipeline.validate import validate_snapshot


def _log(stage: str, payload: dict):
    print(f"\n=== [{stage}] ===")
    print(json.dumps(payload, indent=2, default=str))


def _write_state(entry: dict):
    """Append-only run log. This is the first thing a human at 3 AM should
    read: what did the last N pipeline runs do, and where did they stop."""
    path = state_file_path()
    history = []
    if path.exists():
        try:
            history = json.loads(path.read_text())
        except json.JSONDecodeError:
            history = []
    entry["timestamp"] = datetime.now(timezone.utc).isoformat()
    history.append(entry)
    path.write_text(json.dumps(history[-200:], indent=2))  # keep last 200 runs


def run(crawl_date: str | None, previous_crawl_date: str | None, mock: bool, dry_run: bool,
        source: str = "production", limit_rows: int | None = None, land: bool = False):
    is_sample = (source == "sample" and not mock)
    # A row-limited production run, or the sample table, both produce fewer
    # rows than a real production month -- use the smaller thresholds
    # (min_domain_count_mock, min_row_count_mock) for validation either way.
    # This is purely a threshold choice; it does not relax anything about
    # what's actually checked.
    use_small_thresholds = mock or is_sample or bool(limit_rows)

    label = "sample" if is_sample else crawl_date
    run_record = {"crawl_date": crawl_date, "previous_crawl_date": previous_crawl_date,
                  "mock": mock, "dry_run": dry_run, "source": source, "limit_rows": limit_rows,
                  "stages": {}}

    # 1. Arrival check -- only meaningful for a dated, partitioned monthly
    # crawl. The sample table is a static mirror with no timing trap, so
    # there's nothing to check arrival for; skip straight to extraction.
    if not is_sample:
        arrival = check_arrival(crawl_date, mock=mock)
        _log("arrival_check", arrival.__dict__)
        run_record["stages"]["arrival_check"] = arrival.__dict__
        if not arrival.arrived:
            run_record["outcome"] = "STOPPED_ARRIVAL_CHECK_FAILED"
            _write_state(run_record)
            print(f"\nSTOP: {crawl_date} has not arrived ({arrival.reason}). "
                  f"No files touched. Nothing to alert on beyond this log -- this "
                  f"is the expected, safe outcome of running before the crawl lands.")
            return 1
    else:
        _log("arrival_check", {"skipped": True, "reason": "source=sample has no dated partition to check"})
        run_record["stages"]["arrival_check"] = {"skipped": True}

    # 2. Extract (includes the dry-run cost ceiling check inside extract.py
    # for real production runs; sample and mock runs skip that gate).
    try:
        extract_result = extract(crawl_date, mock=mock, dry_run=dry_run, source=source, limit_rows=limit_rows)
    except Exception as exc:
        run_record["outcome"] = "ERROR_EXTRACT"
        run_record["error"] = str(exc)
        _write_state(run_record)
        _log("extract_ERROR", {"error": str(exc)})
        return 3
    _log("extract", extract_result)
    run_record["stages"]["extract"] = extract_result

    if dry_run and not mock and not is_sample:
        run_record["outcome"] = "DRY_RUN_ONLY"
        _write_state(run_record)
        print("\nDry-run complete. No data extracted, nothing loaded. "
              "Re-run without --dry-run to actually pull and load data.")
        return 0

    # 3. Domain normalization + origin aggregation -> snapshot
    try:
        snapshot_result = build_snapshot(label)
    except Exception as exc:
        run_record["outcome"] = "ERROR_BUILD_SNAPSHOT"
        run_record["error"] = str(exc)
        _write_state(run_record)
        _log("build_snapshot_ERROR", {"error": str(exc)})
        return 3
    _log("build_snapshot", snapshot_result)
    run_record["stages"]["build_snapshot"] = snapshot_result

    # 4. Validation gate. FAIL here means the snapshot file exists on disk
    # (for post-mortem inspection) but is explicitly NOT treated as the
    # "current" snapshot -- diff_snapshots.py is never pointed at a failed
    # snapshot, and nothing downstream should read snapshot_<label>.json
    # without checking this gate passed first (run_pipeline.py's exit code
    # 2 is the signal an orchestrator/cron wrapper should alert on).
    effective_previous = None if is_sample else previous_crawl_date
    validation = validate_snapshot(label, effective_previous, mock=use_small_thresholds)
    _log("validate", {"passed": validation.passed, "checks": validation.checks})
    run_record["stages"]["validate"] = {"passed": validation.passed, "checks": validation.checks}

    if not validation.passed:
        run_record["outcome"] = "STOPPED_VALIDATION_FAILED"
        _write_state(run_record)
        print(f"\nSTOP: validation gate failed for {label}. "
              f"Snapshot file was written to disk for inspection but is NOT "
              f"promoted. Previous snapshot (if any) is untouched. "
              f"ALERT: a human should look at data/snapshots/snapshot_{label}.json "
              f"and the failed checks above before re-running.")
        return 2

    print(f"\nPASS: {label} snapshot validated and loaded "
          f"({snapshot_result['domain_count']} domains).")
    run_record["outcome"] = "LOADED"

    # 5. Diff engine -- only runs for a dated production run with a previous
    # month supplied and on disk. Sample runs have nothing to diff against
    # (only one snapshot of that table exists).
    if is_sample:
        run_record["stages"]["diff_snapshots"] = {"skipped": True, "reason": "source=sample has no month-over-month comparison"}
    elif previous_crawl_date:
        prev_snapshot_path = REPO_ROOT / CFG["paths"]["snapshots_dir"] / f"snapshot_{previous_crawl_date}.json"
        if prev_snapshot_path.exists():
            diff_result = diff_snapshots(label, previous_crawl_date)
            _log("diff_snapshots", diff_result)
            run_record["stages"]["diff_snapshots"] = diff_result
        else:
            msg = f"previous_crawl_date {previous_crawl_date} has no snapshot on disk -- skipping diff"
            _log("diff_snapshots_SKIPPED", {"reason": msg})
            run_record["stages"]["diff_snapshots"] = {"skipped": True, "reason": msg}
    else:
        run_record["stages"]["diff_snapshots"] = {"skipped": True, "reason": "no previous_crawl_date supplied"}

    # 6. Land to warehouse (optional, off by default). This is the actual
    # "load" step going to BigQuery instead of just local JSON -- see
    # pipeline/land_to_warehouse.py and docs/design_doc.md section 3.
    # Deliberately gated behind an explicit flag and excluded for mock/sample
    # runs: it needs real GCS/BigQuery credentials, and a --mock test run
    # must never attempt a real network call as a side effect of "just
    # running the tests."
    if land and not mock and not is_sample:
        from pipeline.land_to_warehouse import land_snapshot, land_change_events
        try:
            land_result = land_snapshot(label)
            _log("land_to_warehouse_snapshot", land_result)
            run_record["stages"]["land_snapshot"] = land_result
            if previous_crawl_date and run_record["stages"].get("diff_snapshots", {}).get("output_path"):
                events_result = land_change_events(label, previous_crawl_date)
                _log("land_to_warehouse_change_events", events_result)
                run_record["stages"]["land_change_events"] = events_result
        except Exception as exc:
            # Landing failure does NOT retroactively invalidate the already-
            # validated, already-loaded-locally snapshot -- run_record's
            # outcome stays LOADED. This mirrors the real failure mode: the
            # pipeline's correctness-critical work (arrival -> validate) is
            # already done and good; only the warehouse write needs a retry,
            # and land_to_warehouse.py's MERGE-based steps make that retry
            # safe to just run again (see design_doc.md section 4).
            run_record["stages"]["land_snapshot_ERROR"] = str(exc)
            _log("land_to_warehouse_ERROR", {"error": str(exc)})
            print(f"\nWARNING: snapshot for {label} is validated and loaded locally, "
                  f"but landing to the warehouse failed: {exc}\n"
                  f"Re-run with the same --land flag once the underlying issue is fixed -- "
                  f"land_to_warehouse.py's MERGE steps are idempotent, so a retry is safe.")
    elif land:
        _log("land_to_warehouse_SKIPPED", {"reason": "mock and sample runs never land to the real warehouse"})

    _write_state(run_record)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Run the full tech-stack enrichment pipeline, one month (or the sample table) at a time.")
    parser.add_argument("--crawl-date", default=None, help="YYYY-MM-01. Required unless --source sample.")
    parser.add_argument("--previous-crawl-date", default=None, help="YYYY-MM-01, for the diff stage")
    parser.add_argument("--source", choices=["production", "sample"], default="production",
                         help="sample = free httparchive.sample_data.pages_10k, no --crawl-date needed")
    parser.add_argument("--mock", action="store_true", help="Use local fixtures instead of BigQuery")
    parser.add_argument("--dry-run", action="store_true", help="Stop after the extract cost check; don't load anything")
    parser.add_argument("--limit-rows", type=int, default=None,
                         help="Cap rows downloaded to disk on a real production run (does not reduce billed scan)")
    parser.add_argument("--land", action="store_true",
                         help="After a successful load (+diff), also land the snapshot/change-events "
                              "into the BigQuery warehouse (see pipeline/land_to_warehouse.py). "
                              "Requires real GCP credentials; ignored for --mock and --source sample.")
    args = parser.parse_args()

    if args.source == "production" and not args.mock and not args.dry_run and not args.crawl_date:
        parser.error("--crawl-date is required unless --source sample")

    exit_code = run(args.crawl_date, args.previous_crawl_date, args.mock, args.dry_run,
                     source=args.source, limit_rows=args.limit_rows, land=args.land)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
