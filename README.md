# tech-stack-pipeline

A domain -> tech-stack enrichment pipeline built on the HTTP Archive
BigQuery public dataset, producing:

1. A **monthly snapshot**: `domain -> [technologies]`, join-ready against a
   company database (`data/snapshots/`).
2. A **change-events feed**: `domain -> technologies added/dropped` between
   two months, for buyer-intent signals (`data/change_events/`).

Built for a take-home assessment. See `docs/design_doc.md` for the full
reasoning behind every design decision (the three "traps," the cost model,
the join-key contract, and failure modes), `docs/findings.md` for the raw
source investigation this was based on, and `docs/how_you_build.md` for the
dev-process reflection.

## Status: verified against real BigQuery

Every pipeline stage was first built and tested end-to-end against a
synthetic, schema-accurate fixture generator
(`pipeline/mock_source.py` -- see docstring for details) that mimics HTTP
Archive's actual row shapes, including deliberately injected grain
collisions and month-over-month tech churn. That fixture path (`--mock`)
still exists and is the fastest local dev loop.

Since then, this has also been run for real against a live GCP trial
account:
- Real dry-run evidence against `httparchive.crawl.pages` is in
  `sql/exploration/04_dry_run_notes.md` (~29.84-30.70 GB per query,
  ~60 GB combined for a full month -- well under the 300 GB guardrail
  and the 1 TiB/month free tier).
- A real run against the free `sample_data.pages_10k` table
  (`python pipeline/run_pipeline.py --source sample`) succeeded: 5,432
  domains, 99.10% tech coverage, validation passed.
- A real, cost-controlled run against production `crawl.pages`
  (`--crawl-date 2026-07-01 --limit-rows 500` and the same for
  `2026-08-01`) succeeded for two consecutive months, producing real
  snapshots and a real `change_events_2026-07-01_to_2026-08-01.json`.
  `--limit-rows` caps what's written to local disk, not what BigQuery
  bills -- see `extract.py`'s docstring for why those are different
  things. This repo's local artifacts reflect 500 rows/month by
  deliberate choice (local disk constraints during dev), not a pipeline
  limitation; the production SQL and guardrails are scoped for a full
  month (§2 of `docs/design_doc.md`).
- Both real production snapshots plus the change-events batch have also
  been landed for real into a BigQuery-native warehouse (`--create-tables`,
  then `land_to_warehouse.py` for each month) and verified with `SELECT`s
  against the three production tables -- see `docs/design_doc.md` §3 and
  `docs/submission_prep.md` §2 for the exact commands and what to check.

Before running any of this yourself: update `config.yaml`'s
`gcp.project_id` (and `warehouse.gcs_bucket`) to your own GCP project.

## Quickstart (no GCP account needed)

```bash
pip install -r requirements.txt

# Generate synthetic fixtures shaped like two months of HTTP Archive data
# (500ish origins each, ~15% tech churn between them, one deliberately
# "not landed" month to exercise the timing-trap check):
python pipeline/mock_source.py

# Run the full pipeline for "month 1" (no previous month to diff against):
python pipeline/run_pipeline.py --crawl-date 2026-07-01 --mock

# Run "month 2," which will also produce a change-events diff against month 1:
python pipeline/run_pipeline.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01 --mock

# See the timing trap correctly refuse a not-yet-landed month:
python pipeline/run_pipeline.py --crawl-date 2026-09-01 --previous-crawl-date 2026-08-01 --mock
# -> exits 1, prints "STOP: 2026-09-01 has not arrived", touches nothing

# Run the test suite:
pytest tests/ -v
```

Outputs land in `data/snapshots/`, `data/change_events/`, and (if any rows
fail domain normalization) `data/rejected/`. Every run, pass or fail, is
appended to `data/pipeline_state.json` -- the first thing to check if a
run didn't do what you expected.

## Running against real BigQuery

1. Set up a GCP project (free tier is sufficient) and authenticate:
   ```bash
   gcloud auth application-default login
   ```
2. In the BigQuery console, star the `httparchive` project so its datasets
   are visible (see har.fyi's getting-started guide for the click-path).
3. Set your project ID, either in `config.yaml` (`gcp.project_id`) or via
   `export GCP_PROJECT_ID=your-project-id`.
4. **Always dry-run before a real extraction:**
   ```bash
   python pipeline/extract.py --crawl-date 2026-08-01 --dry-run
   ```
   This prints the estimated bytes for both `extract_snapshot.sql` and
   `extract_domains_universe.sql`, and refuses to let a real run proceed if
   either estimate exceeds `guardrails.max_scan_bytes_production` in
   `config.yaml` (default 300 GB). There is no flag to bypass this.
5. Run the real pipeline (drop `--mock`):
   ```bash
   python pipeline/run_pipeline.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01
   ```
   `arrival_check.py` will query the live partition first; if that month
   hasn't landed yet, the pipeline stops before any extraction happens.

To develop/test against the **free sample table** instead of the full
production table, point `extract.py` at it directly:
```bash
python pipeline/extract.py --crawl-date 2026-08-01 --table httparchive.sample_data.pages_10k
```
(`sample_data.pages_10k` has no `date` partition in the usual sense --
consult `har.fyi` for its current semantics before relying on a specific
`crawl_date` filter against it; `sql/exploration/01_schema_and_counts.sql`
is a good first query to run there.)

## Landing in the warehouse

`run_pipeline.py` stops once a validated snapshot (and, if applicable, a
change-events batch) is on local disk -- that's the boundary the take-home
brief draws between "the pipeline" and "landing it." The actual warehouse
load is a separate, explicit step: `pipeline/land_to_warehouse.py`.

I used **BigQuery-native tables in my own GCP project** as the
warehouse rather than a literal Snowflake instance -- same reasoning as
`docs/design_doc.md` §3.0: it's a real trial account I can actually run
against, and the mechanics (stage -> load -> `MERGE`) are identical to
what a Snowflake `COPY INTO` + `MERGE` implementation would do.

```bash
# one-time, idempotent setup (CREATE TABLE IF NOT EXISTS):
python pipeline/land_to_warehouse.py --create-tables

# land a snapshot only:
python pipeline/land_to_warehouse.py --crawl-date 2026-07-01

# land a snapshot AND its change-events batch against the previous month:
python pipeline/land_to_warehouse.py --crawl-date 2026-08-01 --previous-crawl-date 2026-07-01

# see exactly what would run, touching nothing:
python pipeline/land_to_warehouse.py --crawl-date 2026-08-01 --dry-run
```

This only lands a snapshot that already passed `validate.py` -- it reads
`data/snapshots/snapshot_<date>.json` off disk and does not re-run or
re-check validation itself. The full mechanics (target tables, the
join-key contract, idempotency/retry behavior) are in
`docs/design_doc.md` §3; the actual DDL and `MERGE` statements are in
`sql/warehouse/`.

## Repo structure

```
tech-stack-pipeline/
├── sql/
│   ├── production/
│   │   ├── extract_snapshot.sql          # UNNESTed tech-detail extract, parameterized by @crawl_date
│   │   └── extract_domains_universe.sql  # companion query: every origin, even with zero detected tech
│   ├── exploration/                      # dev queries against sample_data.pages_10k
│   │   ├── 01_schema_and_counts.sql
│   │   ├── 02_technology_shape.sql
│   │   ├── 03_grain_and_domain_collisions.sql
│   │   └── 04_dry_run_notes.md           # real dry-run evidence, filled in 2026-08-27
│   └── warehouse/                        # DDL + MERGE for the BigQuery-as-warehouse landing step
│       ├── create_tables.sql             # target + staging tables (idempotent)
│       ├── merge_snapshot_latest.sql     # upsert, one row per domain
│       ├── merge_snapshot_history.sql    # append-only, one row per (domain, crawl_date)
│       └── merge_change_events.sql       # append-only event log
├── pipeline/
│   ├── arrival_check.py       # timing trap: does the crawl actually exist yet?
│   ├── extract.py             # cost trap: dry-run-gated BQ extraction (or --mock fixtures)
│   ├── normalize_domain.py    # grain trap: PSL-aware origin -> registrable domain
│   ├── build_snapshot.py      # aggregates origins -> one row per domain
│   ├── validate.py            # the validation gate -- 6 checks, any failure stops the load
│   ├── diff_snapshots.py      # month-over-month added/dropped -> change events
│   ├── run_pipeline.py        # orchestrates all of the above, one command
│   ├── land_to_warehouse.py   # separate step: validated snapshot -> GCS -> BigQuery MERGE
│   └── mock_source.py         # synthetic fixture generator for local dev/testing
├── tests/                     # pytest, 32 tests: the three traps + warehouse-landing orchestration
├── data/                      # generated at runtime; fixtures/ has a generator, rest is gitignored
├── docs/
│   ├── findings.md            # source investigation (har.fyi), done before writing any SQL
│   ├── schema_draft.md        # exact output schemas for both deliverables
│   ├── design_doc.md          # the three traps + answers, cost model, join-key contract, failure modes
│   └── how_you_build.md       # dev-loop / AI-tooling reflection
├── config.yaml                # all thresholds and knobs live here, not scattered in code
├── config.py                  # thin YAML loader shared by every pipeline module
├── requirements.txt
└── README.md                  # this file
```

## Design highlights (see docs/design_doc.md for full detail)

- **Timing trap**: `arrival_check.py` never infers arrival from the
  calendar; it queries the live partition's row count before anything else
  runs.
- **Grain trap**: origins normalize to registrable domains via a
  public-suffix-aware parser (not naive "last two labels"); domains with
  multiple constituent origins get the *union* of detected technologies,
  a deliberate, documented, defensible choice.
- **Cost trap**: `extract.py` structurally cannot run a real query without
  a passing dry-run first, enforced against `guardrails.max_scan_bytes_production`.
  `LIMIT` is never used as a cost control (it isn't one).
- **Validation gate**: 6 checks (existence, min domain count, tech
  coverage ratio, crawl-date consistency, no duplicate domains, row-count
  drift vs. previous month in both directions). Any failure stops the load
  and leaves the previous good snapshot untouched.

## LLM usage

**None, in the production pipeline.** Every enrichment step here is
deterministic SQL + Python (domain normalization, technology aggregation,
diffing). HTTP Archive's Wappalyzer detections already arrive as
structured `{technology, categories, info}` records -- there's no
unstructured text to classify or summarize that would justify introducing
an LLM call, its latency, its cost, and its nondeterminism into a pipeline
whose main design goal is "make silent data loss impossible." If a future
requirement needs judgment calls an LLM is actually suited for (e.g.
deriving a "migration intent" flag from a version-string pattern, or
summarizing a domain's stack in natural language for a sales rep), that's
a genuinely different, additive feature -- it would get its own eval set,
tracing schema, and prompt-versioning discipline at that point, not be
retrofitted into this deterministic core.
