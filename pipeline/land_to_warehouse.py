"""land_to_warehouse.py -- the actual "load" step: gets a validated local
snapshot (and optionally a change-events batch) into the warehouse.

I'm using BigQuery-native tables as the warehouse rather than a literal
Snowflake instance -- see docs/design_doc.md section 3.0 for why that's a
deliberate, in-spirit substitution for "Snowflake or similar" (same
staging -> MERGE pattern, same join-key contract, just the warehouse I
actually have a trial account for) rather than a shortcut.

Landing path, every run:

    local JSON (data/snapshots/, data/change_events/)
        -> NDJSON                          (_to_ndjson)
        -> GCS staging object               (_upload_to_gcs)
        -> BigQuery staging table            (_load_staging_table, WRITE_TRUNCATE)
        -> MERGE into production table(s)    (_run_merge, sql/warehouse/merge_*.sql)

Idempotency is the whole point of doing it this way instead of a plain
INSERT: every one of the four steps can be safely re-run. This is the
warehouse-write half of the "what happens if this fails at 3 AM and gets
retried" story in design_doc.md section 4 -- see the new rows added there
for the specific failure modes this introduces (GCS upload failure, BQ load
job failure, partial MERGE) and how each is handled.

This script assumes `pipeline/run_pipeline.py` has already produced a
VALIDATED snapshot on disk (i.e. validate_snapshot().passed was True) --
it does not re-check validation itself. Called either directly, or via
`run_pipeline.py --land` immediately after a successful load+diff.

Usage:
    # one-time setup, idempotent (CREATE TABLE IF NOT EXISTS):
    python pipeline/land_to_warehouse.py --create-tables

    # land a snapshot only:
    python pipeline/land_to_warehouse.py --crawl-date 2026-08-01

    # land a snapshot AND its change-events batch against the previous month:
    python pipeline/land_to_warehouse.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01

    # see exactly what would run without touching GCS/BigQuery:
    python pipeline/land_to_warehouse.py --crawl-date 2026-08-01 --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT

SQL_DIR = REPO_ROOT / "sql" / "warehouse"

SNAPSHOT_SCHEMA_FIELDS = [
    ("domain", "STRING", "REQUIRED"),
    ("url", "STRING", "NULLABLE"),
    ("tech", "RECORD", "REPEATED"),  # nested fields added below
    ("rank", "INTEGER", "NULLABLE"),
    ("crawl_date", "DATE", "REQUIRED"),
    ("origin_count", "INTEGER", "NULLABLE"),
]

CHANGE_EVENTS_SCHEMA_FIELDS = [
    ("domain", "STRING", "REQUIRED"),
    ("event_type", "STRING", "REQUIRED"),
    ("crawl_date", "DATE", "REQUIRED"),
    ("previous_crawl_date", "DATE", "REQUIRED"),
    ("added", "STRING", "REPEATED"),
    ("dropped", "STRING", "REPEATED"),
]


def _bq_snapshot_schema():
    from google.cloud import bigquery
    tech_subfields = [
        bigquery.SchemaField("technology", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    ]
    return [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tech", "RECORD", mode="REPEATED", fields=tech_subfields),
        bigquery.SchemaField("rank", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("crawl_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("origin_count", "INTEGER", mode="NULLABLE"),
    ]


def _bq_change_events_schema():
    from google.cloud import bigquery
    return [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("previous_crawl_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("added", "STRING", mode="REPEATED"),
        bigquery.SchemaField("dropped", "STRING", mode="REPEATED"),
    ]


def _to_ndjson(rows: list[dict], out_path: Path) -> Path:
    """BigQuery's load API wants newline-delimited JSON, one object per line
    -- not the pretty-printed JSON array build_snapshot.py/diff_snapshots.py
    write to disk. This is a pure reshape, no data touched."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return out_path


def _upload_to_gcs(local_path: Path, bucket_name: str, blob_name: str) -> str:
    from google.cloud import storage
    client = storage.Client(project=CFG["gcp"]["project_id"])
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{blob_name}"


def _load_staging_table(gcs_uri: str, table_id: str, schema) -> dict:
    """WRITE_TRUNCATE: the staging table only ever holds this run's batch.
    Truncate-then-load is itself idempotent (re-running with the same GCS
    object produces the same staging content), which is what lets the
    downstream MERGE steps be idempotent too."""
    from google.cloud import bigquery
    client = bigquery.Client(project=CFG["gcp"]["project_id"])
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # blocks until done; raises on failure
    table = client.get_table(table_id)
    return {"table_id": table_id, "row_count": table.num_rows, "gcs_uri": gcs_uri}


def _run_merge(sql_filename: str) -> dict:
    from google.cloud import bigquery
    client = bigquery.Client(project=CFG["gcp"]["project_id"])
    wh = CFG["warehouse"]
    sql = (SQL_DIR / sql_filename).read_text().format(
        project=CFG["gcp"]["project_id"], dataset=wh["bq_dataset"]
    )
    job = client.query(sql)
    job.result()
    return {
        "sql_file": sql_filename,
        "dml_affected_rows": job.dml_stats.inserted_row_count + job.dml_stats.updated_row_count
        if job.dml_stats else None,
        "bytes_billed": job.total_bytes_billed,
    }


def create_tables():
    from google.cloud import bigquery
    client = bigquery.Client(project=CFG["gcp"]["project_id"])
    wh = CFG["warehouse"]
    sql = (SQL_DIR / "create_tables.sql").read_text().format(
        project=CFG["gcp"]["project_id"],
        dataset=wh["bq_dataset"],
        location=CFG["gcp"]["bq_location"],
    )
    for statement in [s.strip() for s in sql.split(";") if s.strip()]:
        client.query(statement).result()
    print(f"Tables ready in {CFG['gcp']['project_id']}.{wh['bq_dataset']}")


def land_snapshot(crawl_date: str, dry_run: bool = False) -> dict:
    wh = CFG["warehouse"]
    snapshot_path = REPO_ROOT / CFG["paths"]["snapshots_dir"] / f"snapshot_{crawl_date}.json"
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"No validated snapshot at {snapshot_path}. Run run_pipeline.py for "
            f"{crawl_date} first -- land_to_warehouse.py does not re-run the pipeline "
            f"or re-check validation, only lands what's already on disk."
        )
    rows = json.loads(snapshot_path.read_text())

    staging_dir = REPO_ROOT / "data" / "staging"
    ndjson_path = _to_ndjson(rows, staging_dir / f"snapshot_{crawl_date}.ndjson")
    blob_name = f"{wh['gcs_staging_prefix']}/snapshot/{crawl_date}.ndjson"

    if dry_run:
        return {
            "action": "DRY_RUN",
            "would_upload_to": f"gs://{wh['gcs_bucket']}/{blob_name}",
            "would_load_rows": len(rows),
            "would_run_merges": ["merge_snapshot_latest.sql", "merge_snapshot_history.sql"],
        }

    gcs_uri = _upload_to_gcs(ndjson_path, wh["gcs_bucket"], blob_name)
    staging_table_id = f"{CFG['gcp']['project_id']}.{wh['bq_dataset']}.{wh['staging_table_snapshot']}"
    load_result = _load_staging_table(gcs_uri, staging_table_id, _bq_snapshot_schema())
    merge_latest = _run_merge("merge_snapshot_latest.sql")
    merge_history = _run_merge("merge_snapshot_history.sql")

    return {
        "crawl_date": crawl_date,
        "rows_staged": len(rows),
        "gcs_upload": gcs_uri,
        "staging_load": load_result,
        "merge_latest": merge_latest,
        "merge_history": merge_history,
    }


def land_change_events(current_date: str, previous_date: str, dry_run: bool = False) -> dict:
    wh = CFG["warehouse"]
    events_path = (REPO_ROOT / CFG["paths"]["change_events_dir"] /
                   f"change_events_{previous_date}_to_{current_date}.json")
    if not events_path.exists():
        raise FileNotFoundError(
            f"No change-events file at {events_path}. Run run_pipeline.py with both "
            f"--crawl-date and --previous-crawl-date first."
        )
    rows = json.loads(events_path.read_text())

    staging_dir = REPO_ROOT / "data" / "staging"
    ndjson_path = _to_ndjson(rows, staging_dir / f"change_events_{previous_date}_to_{current_date}.ndjson")
    blob_name = f"{wh['gcs_staging_prefix']}/change_events/{previous_date}_to_{current_date}.ndjson"

    if dry_run:
        return {
            "action": "DRY_RUN",
            "would_upload_to": f"gs://{wh['gcs_bucket']}/{blob_name}",
            "would_load_rows": len(rows),
            "would_run_merges": ["merge_change_events.sql"],
        }

    gcs_uri = _upload_to_gcs(ndjson_path, wh["gcs_bucket"], blob_name)
    staging_table_id = f"{CFG['gcp']['project_id']}.{wh['bq_dataset']}.{wh['staging_table_change_events']}"
    load_result = _load_staging_table(gcs_uri, staging_table_id, _bq_change_events_schema())
    merge_result = _run_merge("merge_change_events.sql")

    return {
        "crawl_date": current_date,
        "previous_crawl_date": previous_date,
        "events_staged": len(rows),
        "gcs_upload": gcs_uri,
        "staging_load": load_result,
        "merge": merge_result,
    }


def main():
    parser = argparse.ArgumentParser(description="Land a validated local snapshot (+ change events) into the BigQuery warehouse.")
    parser.add_argument("--create-tables", action="store_true", help="Idempotent DDL setup, then exit.")
    parser.add_argument("--crawl-date", default=None)
    parser.add_argument("--previous-crawl-date", default=None, help="If given, also lands the change-events batch for this month-pair.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen; touch no GCS/BigQuery state.")
    args = parser.parse_args()

    if args.create_tables:
        create_tables()
        return

    if not args.crawl_date:
        parser.error("--crawl-date is required unless --create-tables")

    result = {"snapshot": land_snapshot(args.crawl_date, dry_run=args.dry_run)}
    if args.previous_crawl_date:
        result["change_events"] = land_change_events(args.crawl_date, args.previous_crawl_date, dry_run=args.dry_run)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
