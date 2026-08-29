"""extract.py -- the cost trap.

Three ways to run this:

  1. --mock              Local fixtures, no BigQuery at all. Fully offline dev/test.
  2. --source sample      Real query against the FREE httparchive.sample_data.pages_10k
                          table. Small (~10k rows), effectively free, no @crawl_date
                          needed. Run this before ever touching production. Writes
                          data/raw/extract_sample.json and universe_sample.json.
  3. (default) production Real query against httparchive.crawl.pages for one month.
                          Dry-run-gated: refuses to execute if the estimated scan
                          exceeds guardrails.max_scan_bytes_production. This is the
                          multi-terabyte table -- --crawl-date is required.

--limit-rows N (production only): appends `LIMIT N` to the real queries so only
a bounded number of rows are downloaded and written to disk. THIS DOES NOT
REDUCE THE BILLED SCAN -- BigQuery still reads every matching row's column
data to know which N to return; the dry-run estimate (computed WITHOUT the
LIMIT, on purpose) is the number that reflects real cost. --limit-rows exists
purely to prove the pipeline runs correctly end-to-end against real
production data without downloading and storing millions of rows locally --
e.g. `--limit-rows 500` produces a small, real, inspectable sample of actual
production output, not a full local mirror of the crawl.

Usage:
    python pipeline/extract.py --source sample                                  # real, free, ~10k rows
    python pipeline/extract.py --crawl-date 2026-08-01 --dry-run                # cost estimate only
    python pipeline/extract.py --crawl-date 2026-08-01 --limit-rows 500         # real, capped download
    python pipeline/extract.py --crawl-date 2026-08-01                          # real, FULL month (millions of rows)
    python pipeline/extract.py --crawl-date 2026-08-01 --mock                   # local fixtures
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT

PROD_SQL_DIR = REPO_ROOT / "sql" / "production"
SAMPLE_SQL_DIR = REPO_ROOT / "sql" / "exploration"


def _read_sql(directory: Path, name: str) -> str:
    return (directory / name).read_text()


def _run_mock(crawl_date: str) -> tuple[list[dict], list[dict]]:
    fixtures_dir = REPO_ROOT / CFG["paths"]["fixtures_dir"]
    extract_path = fixtures_dir / f"mock_extract_{crawl_date}.json"
    universe_path = fixtures_dir / f"mock_universe_{crawl_date}.json"
    if not extract_path.exists() or not universe_path.exists():
        raise FileNotFoundError(
            f"No mock fixture for {crawl_date}. Run `python pipeline/mock_source.py` first, "
            f"or use one of its generated dates (2026-07-01, 2026-08-01)."
        )
    return json.loads(extract_path.read_text()), json.loads(universe_path.read_text())


def _dry_run_bytes(client, query: str, crawl_date: str) -> int:
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("crawl_date", "DATE", crawl_date)],
        dry_run=True,
        use_query_cache=False,
    )
    job = client.query(query, job_config=job_config)
    return job.total_bytes_processed


def _universe_sql_with_limit(sql: str, limit_rows: int | None) -> str:
    """Deterministically cap the universe query to N origins, ordered by
    root_page so the result is reproducible run to run."""
    if not limit_rows:
        return sql
    limit_rows = int(limit_rows)
    return sql.rstrip().rstrip(";") + f"\nORDER BY root_page\nLIMIT {limit_rows};\n"


def _extract_sql_with_origin_filter(sql: str) -> str:
    """Restrict the tech-detail query to a specific, externally-supplied set
    of origins (via the @origins ARRAY parameter) instead of using a row
    LIMIT.

    This is the fix for a real bug found in testing: giving
    extract_snapshot.sql and extract_domains_universe.sql independent
    `LIMIT N` clauses (even with the same ORDER BY) doesn't guarantee they
    cover the same origins, because extract_snapshot.sql's grain is one row
    PER TECHNOLOGY, not per origin -- a row-count LIMIT there truncates
    mid-origin, cutting some origins' technology lists short and dropping
    others entirely. The fix: pick N origins from the universe query first
    (that query IS one row per origin, so a LIMIT there is safe), then pull
    ALL technologies for exactly those origins, with no further limit. Row
    count downstream is still small (N origins x a handful of technologies
    each), but every one of the N origins gets its complete, correct
    technology list rather than an arbitrary truncated slice.
    """
    return sql.rstrip().rstrip(";") + "\n  AND root_page IN UNNEST(@origins)\n;\n"


def _with_limit(sql: str, limit_rows: int | None) -> str:
    """Sample-table version: no separate origin-selection step needed since
    pages_10k is small and cheap regardless -- same ORDER BY root_page
    determinism as _universe_sql_with_limit, applied directly to both
    queries. (Production uses the two-step origin-filter approach above
    instead, because production's tech-detail query is too expensive to
    run twice.)"""
    if not limit_rows:
        return sql
    limit_rows = int(limit_rows)
    return sql.rstrip().rstrip(";") + f"\nORDER BY root_page\nLIMIT {limit_rows};\n"


def _run_real_production(crawl_date: str, dry_run_only: bool, limit_rows: int | None) -> tuple[list[dict], list[dict], dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=CFG["gcp"]["project_id"])
    ceiling = CFG["guardrails"]["max_scan_bytes_production"]

    extract_sql = _read_sql(PROD_SQL_DIR, "extract_snapshot.sql")
    universe_sql = _read_sql(PROD_SQL_DIR, "extract_domains_universe.sql")

    # Dry-run ALWAYS uses the unlimited query -- a LIMIT clause doesn't
    # change what BigQuery has to scan to know which rows qualify, so
    # estimating against the limited query would understate the true cost.
    dry_run_report = {}
    for name, sql in [("extract_snapshot", extract_sql), ("extract_domains_universe", universe_sql)]:
        estimated = _dry_run_bytes(client, sql, crawl_date)
        dry_run_report[name] = estimated
        print(f"[dry-run] {name}: {estimated:,} bytes ({estimated / 1e9:.2f} GB)")
        if estimated > ceiling:
            raise RuntimeError(
                f"{name} estimated at {estimated:,} bytes, over the "
                f"{ceiling:,} byte ceiling in config.yaml guardrails.max_scan_bytes_production. "
                f"Refusing to run. Tighten the filters or raise the ceiling deliberately."
            )

    if dry_run_only:
        return [], [], dry_run_report

    if limit_rows:
        print(f"[limit-rows] Selecting {limit_rows} origins deterministically (ORDER BY root_page), "
              f"then pulling ALL technologies for exactly those origins -- not an independent row LIMIT "
              f"on the tech-detail query, which would truncate mid-origin. "
              f"Billed scan is UNCHANGED from the dry-run estimate above; this only reduces what's written to disk.")

        universe_job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("crawl_date", "DATE", crawl_date)],
            maximum_bytes_billed=ceiling,
        )
        limited_universe_sql = _universe_sql_with_limit(universe_sql, limit_rows)
        universe_rows = [dict(row) for row in client.query(limited_universe_sql, job_config=universe_job_config).result()]

        origins = [row["root_page"] for row in universe_rows]
        filtered_extract_sql = _extract_sql_with_origin_filter(extract_sql)
        extract_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("crawl_date", "DATE", crawl_date),
                bigquery.ArrayQueryParameter("origins", "STRING", origins),
            ],
            maximum_bytes_billed=ceiling,
        )
        extract_rows = [dict(row) for row in client.query(filtered_extract_sql, job_config=extract_job_config).result()] if origins else []

        return extract_rows, universe_rows, dry_run_report

    def _run_query(sql: str) -> list[dict]:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("crawl_date", "DATE", crawl_date)],
            maximum_bytes_billed=ceiling,  # second, independent enforcement at execution time
        )
        return [dict(row) for row in client.query(sql, job_config=job_config).result()]

    extract_rows = _run_query(extract_sql)
    universe_rows = _run_query(universe_sql)
    return extract_rows, universe_rows, dry_run_report


def _run_real_sample(limit_rows: int | None) -> tuple[list[dict], list[dict], dict, str]:
    """Real query against the free sample_data.pages_10k table. No @crawl_date
    parameter, no dry-run cost gate (the table is small and free by design).

    If --limit-rows is supplied, uses the same origin-first approach as
    production (see _extract_sql_with_origin_filter's docstring) rather than
    independent LIMITs on each query, for the same reason: an independent
    LIMIT on the UNNESTed tech-detail query truncates mid-origin and won't
    line up with whichever origins the universe query's LIMIT happened to
    pick."""
    from google.cloud import bigquery

    client = bigquery.Client(project=CFG["gcp"]["project_id"])

    universe_sql = _read_sql(SAMPLE_SQL_DIR, "extract_sample_universe.sql")
    extract_sql = _read_sql(SAMPLE_SQL_DIR, "extract_sample.sql")

    if limit_rows:
        limited_universe_sql = _universe_sql_with_limit(universe_sql, limit_rows)
        universe_rows = [dict(row) for row in client.query(limited_universe_sql).result()]
        origins = [row["root_page"] for row in universe_rows]
        filtered_extract_sql = _extract_sql_with_origin_filter(extract_sql)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("origins", "STRING", origins)],
        )
        extract_rows = [dict(row) for row in client.query(filtered_extract_sql, job_config=job_config).result()] if origins else []
    else:
        extract_rows = [dict(row) for row in client.query(extract_sql).result()]
        universe_rows = [dict(row) for row in client.query(universe_sql).result()]

    # pages_10k has no meaningful date filter, but the rows still carry
    # whatever `date` HTTP Archive stamped on that crawl -- surface it so
    # downstream file naming and docs can reference the real date.
    observed_date = None
    if universe_rows:
        observed_date = str(universe_rows[0].get("crawl_date"))

    return extract_rows, universe_rows, {"sample": True, "observed_crawl_date": observed_date}, (observed_date or "unknown")


def extract(crawl_date: str | None = None, mock: bool = False, dry_run: bool = False,
            source: str = "production", limit_rows: int | None = None) -> dict:
    raw_dir = REPO_ROOT / CFG["paths"]["raw_dir"]
    raw_dir.mkdir(parents=True, exist_ok=True)

    if mock:
        if dry_run:
            print("[mock] --dry-run is a no-op in mock mode (no BigQuery call to estimate). "
                  "Loading fixture row counts instead.")
        extract_rows, universe_rows = _run_mock(crawl_date)
        dry_run_report = {"mock": True}
        label = crawl_date

    elif source == "sample":
        extract_rows, universe_rows, dry_run_report, observed_date = _run_real_sample(limit_rows)
        label = "sample"  # fixed filename regardless of the underlying crawl date

    else:  # production
        if not crawl_date:
            raise ValueError("--crawl-date is required for source=production")
        extract_rows, universe_rows, dry_run_report = _run_real_production(crawl_date, dry_run_only=dry_run, limit_rows=limit_rows)
        label = crawl_date

    if dry_run and source == "production" and not mock:
        return {"crawl_date": crawl_date, "source": source, "dry_run_only": True, "dry_run_report": dry_run_report}

    extract_out = raw_dir / f"extract_{label}.json"
    universe_out = raw_dir / f"universe_{label}.json"
    # default=str handles BigQuery's non-JSON-native return types --
    # DATE columns come back as datetime.date, and some NUMERIC columns
    # can come back as decimal.Decimal. str() on a date gives ISO format
    # (YYYY-MM-DD), which is the format needed anyway.
    extract_out.write_text(json.dumps(extract_rows, indent=2, default=str))
    universe_out.write_text(json.dumps(universe_rows, indent=2, default=str))

    return {
        "crawl_date": crawl_date,
        "source": source,
        "label": label,
        "limit_rows": limit_rows,
        "dry_run_only": False,
        "dry_run_report": dry_run_report,
        "extract_rows_written": len(extract_rows),
        "universe_rows_written": len(universe_rows),
        "extract_path": str(extract_out),
        "universe_path": str(universe_out),
    }


def main():
    parser = argparse.ArgumentParser(description="Extract HTTP Archive technology detections.")
    parser.add_argument("--crawl-date", default=None, help="YYYY-MM-01. Required for source=production; ignored for source=sample.")
    parser.add_argument("--source", choices=["production", "sample"], default="production",
                         help="production = httparchive.crawl.pages (needs --crawl-date). "
                              "sample = free httparchive.sample_data.pages_10k (no date needed).")
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Only estimate scan size, don't extract (production only)")
    parser.add_argument("--limit-rows", type=int, default=None,
                         help="Cap rows written to disk (does NOT reduce billed scan -- see module docstring)")
    args = parser.parse_args()

    result = extract(args.crawl_date, mock=args.mock, dry_run=args.dry_run,
                      source=args.source, limit_rows=args.limit_rows)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
