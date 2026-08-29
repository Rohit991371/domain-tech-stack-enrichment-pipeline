"""arrival_check.py -- the timing trap.

HTTP Archive crawls are always labelled the 1st of the month
(e.g. `date = '2026-08-01'`), but per har.fyi's release-cycle guide the
crawl doesn't actually *start* until the CrUX dataset lands, on the second
Tuesday of the month, and takes 1-2 weeks to finish testing ~12-16M pages.
So the August partition can genuinely be empty or incomplete well past
August 20th.

The wrong mental model: "it's the 20th, therefore this month's data exists."
The right mental model: "does a query against this month's partition
actually return a healthy row count, right now?"

This module answers that question and nothing else. It does NOT decide
whether the row count is *correct* in a business sense -- that's
validate.py's job, run later, against the aggregated snapshot. This module
only answers "is there enough here to be worth extracting at all."

Usage:
    python pipeline/arrival_check.py --crawl-date 2026-08-01
    python pipeline/arrival_check.py --crawl-date 2026-08-01 --mock   # local dev, no BQ

Exit code 0 + prints "ARRIVED" if safe to proceed, exit code 1 + "NOT_ARRIVED"
otherwise. run_pipeline.py treats any non-zero exit as "stop, do nothing,
don't touch existing snapshots."
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG


@dataclass
class ArrivalResult:
    crawl_date: str
    arrived: bool
    row_count: int | None
    reason: str


def _row_count_via_bigquery(crawl_date: str, table: str) -> int:
    from google.cloud import bigquery  # imported lazily -- not needed in --mock mode

    client = bigquery.Client(project=CFG["gcp"]["project_id"])
    query = f"""
        SELECT COUNT(0) AS n
        FROM `{table}`
        WHERE date = @crawl_date AND client = @client AND is_root_page = TRUE
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("crawl_date", "DATE", crawl_date),
            bigquery.ScalarQueryParameter("client", "STRING", CFG["source"]["client"]),
        ],
        # Belt-and-braces: even this tiny COUNT query gets a scan ceiling,
        # because a typo'd WHERE clause should fail loud, not fail expensive.
        maximum_bytes_billed=CFG["guardrails"]["max_scan_bytes_production"],
    )
    result = list(client.query(query, job_config=job_config).result())
    return int(result[0].n) if result else 0


def _row_count_mock(crawl_date: str) -> int:
    """Local dev/testing path with no BigQuery access.

    Reads data/fixtures/mock_crawl_calendar.json, which maps crawl_date ->
    a simulated row count. This is what lets arrival_check.py, and
    everything downstream of it, be exercised end-to-end in an environment
    (like this sandbox) that can't reach googleapis.com. It is NOT a
    substitute for the real dry-run evidence required for submission --
    see sql/exploration/04_dry_run_notes.md.
    """
    import pathlib

    fixture_path = pathlib.Path(__file__).resolve().parent.parent / CFG["paths"]["fixtures_dir"] / "mock_crawl_calendar.json"
    if not fixture_path.exists():
        return 0
    calendar = json.loads(fixture_path.read_text())
    return int(calendar.get(crawl_date, 0))


def check_arrival(crawl_date: str, mock: bool = False, table: str | None = None) -> ArrivalResult:
    # 1. Sanity-check the parameter itself before spending anything on it.
    try:
        parsed = date.fromisoformat(crawl_date)
    except ValueError:
        return ArrivalResult(crawl_date=crawl_date, arrived=False, row_count=None,
                              reason=f"invalid_date_format:{crawl_date}")

    if parsed.day != 1:
        return ArrivalResult(crawl_date=crawl_date, arrived=False, row_count=None,
                              reason="crawl_date_must_be_first_of_month")

    # 2. Get the row count for this month's partition.
    table = table or CFG["source"]["dataset_prod"]
    try:
        if mock:
            row_count = _row_count_mock(crawl_date)
        else:
            row_count = _row_count_via_bigquery(crawl_date, table)
    except Exception as exc:
        return ArrivalResult(crawl_date=crawl_date, arrived=False, row_count=None,
                              reason=f"query_failed:{exc}")

    min_rows = CFG["guardrails"]["min_row_count_mock"] if mock else CFG["guardrails"]["min_row_count"]
    if row_count < min_rows:
        return ArrivalResult(
            crawl_date=crawl_date, arrived=False, row_count=row_count,
            reason=f"row_count_{row_count}_below_min_{min_rows}_partition_likely_not_landed_yet",
        )

    return ArrivalResult(crawl_date=crawl_date, arrived=True, row_count=row_count,
                          reason="ok")


def main():
    parser = argparse.ArgumentParser(description="Check whether an HTTP Archive crawl has actually landed.")
    parser.add_argument("--crawl-date", required=True, help="YYYY-MM-01")
    parser.add_argument("--mock", action="store_true", help="Use local fixture instead of BigQuery")
    parser.add_argument("--table", default=None, help="Override source table (e.g. sample_data.pages_10k for dev)")
    args = parser.parse_args()

    result = check_arrival(args.crawl_date, mock=args.mock, table=args.table)
    print(json.dumps(asdict(result), indent=2))

    if not result.arrived:
        print("NOT_ARRIVED", file=sys.stderr)
        sys.exit(1)
    print("ARRIVED", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
