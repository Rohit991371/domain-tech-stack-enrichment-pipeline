"""Tests for pipeline/land_to_warehouse.py.

These never touch real GCS/BigQuery -- that's the point. Two layers:

1. Orchestration tests (land_snapshot / land_change_events / create_tables):
   monkeypatch the module's own I/O helpers (_upload_to_gcs,
   _load_staging_table, _run_merge) and assert they're called in the right
   order with the right arguments. This is what actually matters for
   "will this do the right thing before I run it against real BigQuery" --
   the orchestration logic is mine; the google-cloud client internals
   aren't.
2. Low-level tests (_to_ndjson, and the google.cloud client wiring in
   _upload_to_gcs / _load_staging_table / _run_merge / create_tables):
   mock google.cloud.bigquery.Client / google.cloud.storage.Client
   directly, to prove the actual SDK calls (upload_from_filename,
   load_table_from_uri, query, job.result()) are wired correctly, not
   just that my own functions call each other correctly.

Run with: pytest tests/test_land_to_warehouse.py -v
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline import land_to_warehouse as ltw

SNAP_DATE = "2099-07-01"
PREV_DATE = "2099-06-01"
NO_FILE_DATE = "2099-08-01"  # deliberately never written


def _write_snapshot(crawl_date: str, n: int = 5):
    snap_dir = REPO_ROOT / CFG["paths"]["snapshots_dir"]
    snap_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {"domain": f"d{i}.com", "url": f"https://d{i}.com/", "rank": i,
         "crawl_date": crawl_date, "origin_count": 1,
         "tech": [{"technology": "WordPress", "categories": ["CMS"], "version": None}]}
        for i in range(n)
    ]
    (snap_dir / f"snapshot_{crawl_date}.json").write_text(json.dumps(rows))
    return rows


def _write_change_events(current: str, previous: str, n: int = 3):
    ev_dir = REPO_ROOT / CFG["paths"]["change_events_dir"]
    ev_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {"domain": f"d{i}.com", "event_type": "tech_change", "crawl_date": current,
         "previous_crawl_date": previous, "added": ["Shopify"], "dropped": ["Magento"]}
        for i in range(n)
    ]
    (ev_dir / f"change_events_{previous}_to_{current}.json").write_text(json.dumps(rows))
    return rows


def setup_module(module):
    _write_snapshot(SNAP_DATE)
    _write_snapshot(PREV_DATE)
    _write_change_events(SNAP_DATE, PREV_DATE)


# ── pure function ────────────────────────────────────────────────────────

def test_to_ndjson_writes_one_object_per_line(tmp_path):
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    out = ltw._to_ndjson(rows, tmp_path / "out.ndjson")
    lines = out.read_text().strip().split("\n")
    assert len(lines) == 3
    assert [json.loads(l) for l in lines] == rows


# ── land_snapshot: missing file / dry-run (no network at all) ───────────

def test_land_snapshot_missing_file_raises():
    try:
        ltw.land_snapshot(NO_FILE_DATE)
        assert False, "expected FileNotFoundError"
    except FileNotFoundError as e:
        assert NO_FILE_DATE in str(e)


def test_land_snapshot_dry_run_touches_no_network(monkeypatch):
    called = []
    monkeypatch.setattr(ltw, "_upload_to_gcs", lambda *a, **kw: called.append("upload"))
    monkeypatch.setattr(ltw, "_load_staging_table", lambda *a, **kw: called.append("load"))
    monkeypatch.setattr(ltw, "_run_merge", lambda *a, **kw: called.append("merge"))

    result = ltw.land_snapshot(SNAP_DATE, dry_run=True)

    assert result["action"] == "DRY_RUN"
    assert result["would_load_rows"] == 5
    assert "merge_snapshot_latest.sql" in result["would_run_merges"]
    assert "merge_snapshot_history.sql" in result["would_run_merges"]
    assert called == []  # nothing real was touched


def test_land_change_events_missing_file_raises():
    try:
        ltw.land_change_events(NO_FILE_DATE, PREV_DATE)
        assert False, "expected FileNotFoundError"
    except FileNotFoundError:
        pass


def test_land_change_events_dry_run_touches_no_network(monkeypatch):
    called = []
    monkeypatch.setattr(ltw, "_upload_to_gcs", lambda *a, **kw: called.append("upload"))
    monkeypatch.setattr(ltw, "_load_staging_table", lambda *a, **kw: called.append("load"))
    monkeypatch.setattr(ltw, "_run_merge", lambda *a, **kw: called.append("merge"))

    result = ltw.land_change_events(SNAP_DATE, PREV_DATE, dry_run=True)

    assert result["action"] == "DRY_RUN"
    assert result["would_load_rows"] == 3
    assert result["would_run_merges"] == ["merge_change_events.sql"]
    assert called == []


# ── orchestration: real path, but with I/O helpers mocked out ───────────

def test_land_snapshot_calls_upload_then_load_then_both_merges_in_order(monkeypatch):
    calls = []
    monkeypatch.setattr(ltw, "_upload_to_gcs",
                         lambda local_path, bucket, blob: calls.append(("upload", blob)) or "gs://fake/blob")
    monkeypatch.setattr(ltw, "_load_staging_table",
                         lambda gcs_uri, table_id, schema: calls.append(("load", table_id)) or {"row_count": 5})
    monkeypatch.setattr(ltw, "_run_merge",
                         lambda sql_file: calls.append(("merge", sql_file)) or {"sql_file": sql_file})

    result = ltw.land_snapshot(SNAP_DATE)

    order = [c[0] for c in calls]
    assert order == ["upload", "load", "merge", "merge"], (
        "landing must upload, THEN load staging, THEN merge -- merging before the "
        "staging table is loaded would MERGE against stale/empty staging data"
    )
    merge_files = [c[1] for c in calls if c[0] == "merge"]
    assert merge_files == ["merge_snapshot_latest.sql", "merge_snapshot_history.sql"]
    assert result["rows_staged"] == 5
    assert result["crawl_date"] == SNAP_DATE


def test_land_snapshot_blob_path_includes_crawl_date(monkeypatch):
    # Regression guard: two different months must not silently overwrite
    # each other's staged GCS object before the staging table is loaded.
    seen_blobs = []
    monkeypatch.setattr(ltw, "_upload_to_gcs",
                         lambda local_path, bucket, blob: seen_blobs.append(blob) or "gs://fake/blob")
    monkeypatch.setattr(ltw, "_load_staging_table", lambda *a, **kw: {"row_count": 5})
    monkeypatch.setattr(ltw, "_run_merge", lambda *a, **kw: {})

    ltw.land_snapshot(SNAP_DATE)
    ltw.land_snapshot(PREV_DATE)

    assert seen_blobs[0] != seen_blobs[1]
    assert SNAP_DATE in seen_blobs[0]
    assert PREV_DATE in seen_blobs[1]


def test_land_change_events_calls_upload_then_load_then_one_merge(monkeypatch):
    calls = []
    monkeypatch.setattr(ltw, "_upload_to_gcs",
                         lambda local_path, bucket, blob: calls.append(("upload", blob)) or "gs://fake/blob")
    monkeypatch.setattr(ltw, "_load_staging_table",
                         lambda gcs_uri, table_id, schema: calls.append(("load", table_id)) or {"row_count": 3})
    monkeypatch.setattr(ltw, "_run_merge",
                         lambda sql_file: calls.append(("merge", sql_file)) or {"sql_file": sql_file})

    result = ltw.land_change_events(SNAP_DATE, PREV_DATE)

    order = [c[0] for c in calls]
    assert order == ["upload", "load", "merge"]
    assert calls[-1][1] == "merge_change_events.sql"
    assert result["events_staged"] == 3


# ── low-level: real google.cloud SDK call shape, client mocked ──────────

def test_upload_to_gcs_calls_expected_storage_sdk_methods(tmp_path):
    fake_file = tmp_path / "f.ndjson"
    fake_file.write_text("{}\n")

    with patch("google.cloud.storage.Client") as MockClient:
        mock_blob = MagicMock()
        MockClient.return_value.bucket.return_value.blob.return_value = mock_blob

        uri = ltw._upload_to_gcs(fake_file, "my-bucket", "path/to/obj.ndjson")

        MockClient.return_value.bucket.assert_called_once_with("my-bucket")
        mock_blob.upload_from_filename.assert_called_once_with(str(fake_file))
        assert uri == "gs://my-bucket/path/to/obj.ndjson"


def test_load_staging_table_uses_write_truncate(tmp_path):
    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_table = MagicMock(num_rows=42)
        MockClient.return_value.get_table.return_value = mock_table
        mock_load_job = MagicMock()
        MockClient.return_value.load_table_from_uri.return_value = mock_load_job

        result = ltw._load_staging_table("gs://bucket/obj.ndjson", "proj.ds.stg_table", schema=[])

        mock_load_job.result.assert_called_once()  # blocks until done, raises on failure
        _, kwargs = MockClient.return_value.load_table_from_uri.call_args
        job_config = kwargs["job_config"]
        from google.cloud import bigquery as real_bq
        assert job_config.write_disposition == real_bq.WriteDisposition.WRITE_TRUNCATE
        assert result == {"table_id": "proj.ds.stg_table", "row_count": 42,
                           "gcs_uri": "gs://bucket/obj.ndjson"}


def test_run_merge_substitutes_project_and_dataset_into_sql(tmp_path, monkeypatch):
    fake_sql_dir = tmp_path
    (fake_sql_dir / "merge_fake.sql").write_text(
        "MERGE `{project}.{dataset}.target` USING `{project}.{dataset}.staging` ..."
    )
    monkeypatch.setattr(ltw, "SQL_DIR", fake_sql_dir)

    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_job = MagicMock()
        mock_job.dml_stats.inserted_row_count = 4
        mock_job.dml_stats.updated_row_count = 1
        mock_job.total_bytes_billed = 12345
        MockClient.return_value.query.return_value = mock_job

        result = ltw._run_merge("merge_fake.sql")

        run_sql = MockClient.return_value.query.call_args[0][0]
        assert CFG["gcp"]["project_id"] in run_sql
        assert CFG["warehouse"]["bq_dataset"] in run_sql
        assert "{project}" not in run_sql and "{dataset}" not in run_sql
        mock_job.result.assert_called_once()
        assert result["dml_affected_rows"] == 5
        assert result["bytes_billed"] == 12345


def test_create_tables_is_idempotent_ddl_and_runs_every_statement():
    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_client = MockClient.return_value
        ltw.create_tables()

        real_sql = (ltw.SQL_DIR / "create_tables.sql").read_text()
        expected_statement_count = len([s for s in real_sql.split(";") if s.strip()])
        assert mock_client.query.call_count == expected_statement_count

        all_sql_run = " ".join(c.args[0] for c in mock_client.query.call_args_list)
        assert "CREATE SCHEMA IF NOT EXISTS" in all_sql_run
        assert "CREATE TABLE IF NOT EXISTS" in all_sql_run
        # this script must never be destructive (note: can't just check for
        # "DROP" -- the `dropped ARRAY<STRING>` column comment contains it)
        assert "DROP TABLE" not in all_sql_run.upper()
        assert "DROP SCHEMA" not in all_sql_run.upper()
