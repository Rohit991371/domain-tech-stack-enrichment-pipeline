# Dry-run evidence log

`sample_data.pages_10k` is a fixed 10k-row table with no `date` partition, so
queries against it don't need `--dry_run` -- there's nothing to accidentally
scan more than 10k rows of. The dry-run discipline matters once I point
`sql/production/extract_snapshot.sql` at `httparchive.crawl.pages`.

This file is the evidence log for those production dry-runs. Every entry
below should be produced by running, from repo root, with a real
`gcloud auth application-default login` session against a project that has
the httparchive dataset starred:

```bash
bq query --use_legacy_sql=false --dry_run \
  --parameter='crawl_date:DATE:2026-08-01' \
  < sql/production/extract_snapshot.sql
```

or equivalently:

```bash
python pipeline/extract.py --crawl-date 2026-08-01 --dry-run
```

which does the same thing via the `google-cloud-bigquery` client and also
enforces the `guardrails.max_scan_bytes_production` ceiling from
`config.yaml` before it will let a real (non-dry-run) extraction proceed.

## Log

| Date run | Query | Crawl date param | Estimated bytes | Notes |
|---|---|---|---|---|
| 2026-08-27 | `extract_snapshot.sql` | `2026-08-01` | 29,844,314,010 bytes (29.84 GB) | Real `python pipeline/extract.py --crawl-date 2026-08-01 --dry-run` against `httparchive.crawl.pages`. Well under the 300 GB ceiling and the brief's own ~150-250 GB estimate. |
| 2026-08-27 | `extract_domains_universe.sql` | `2026-08-01` | 29,844,314,010 bytes (29.84 GB) | Identical to the extract_snapshot figure -- expected, not a bug. BigQuery bills by columns referenced (both queries touch `technologies`), not by whether the column is UNNESTed or just measured with `ARRAY_LENGTH`. |

**Combined real cost per month: ~59.68 GB**, comfortably inside BigQuery's
1 TiB/month on-demand free tier. See `docs/design_doc.md` §2 for the cost
model, now updated with this real measurement instead of the brief's
estimate.

