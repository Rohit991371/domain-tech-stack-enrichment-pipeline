-- create_tables.sql
--
-- DDL for the actual landing target. This is the BigQuery-as-warehouse
-- equivalent of the Snowflake sketch in design_doc.md section 3 -- same
-- shape, same join-key contract, just native SQL for the warehouse I
-- actually have a trial account for (see design_doc.md 3.0 for why this
-- is an in-spirit substitution for "Snowflake or similar", not a shortcut).
--
-- Run once via `python pipeline/land_to_warehouse.py --create-tables`.
-- Idempotent: every statement is CREATE ... IF NOT EXISTS.
--
-- {project} / {dataset} are substituted by land_to_warehouse.py from
-- config.yaml's gcp.project_id / warehouse.bq_dataset -- BigQuery does not
-- allow dataset/table identifiers to be query parameters, only values can
-- be parameterized (see sql/production/*.sql for that pattern -- this file
-- uses Python string substitution instead, deliberately, for the same
-- reason extract.py's @crawl_date is a parameter but the table name isn't).
--
-- GUARDRAIL: land_to_warehouse.py's create_tables() splits this file on
-- literal semicolon characters -- it has no real SQL statement parser.
-- Never put a literal semicolon inside a comment or a string literal
-- anywhere in this file. Doing so will silently corrupt the split and
-- produce a confusing BigQuery error, not an error from this file.
-- Use "--" or "," in prose comments instead of a semicolon.

CREATE SCHEMA IF NOT EXISTS `{project}.{dataset}`
OPTIONS (location = '{location}');

-- ── Production tables ────────────────────────────────────────────────────

-- One row per domain: "what does this company use right now." Upserted by
-- merge_snapshot_latest.sql. This is what a sales-intelligence query joins
-- against 95% of the time -- cheap to scan, no need to filter to MAX(crawl_date).
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.tech_stack_snapshot_latest` (
  domain            STRING NOT NULL,   -- join key -- contract in design_doc.md 3.2
  url               STRING,
  tech              ARRAY<STRUCT<technology STRING, categories ARRAY<STRING>, version STRING>>,
  rank              INT64,
  crawl_date        DATE NOT NULL,     -- which month this row's data is from
  origin_count      INT64,
  loaded_at         TIMESTAMP NOT NULL
)
CLUSTER BY domain
OPTIONS (
  description = 'Current tech stack per registrable domain, upserted monthly. See docs/design_doc.md 3.2 for the join-key contract.'
);

-- Full append-only history: one row per (domain, crawl_date). Needed for
-- "what did this company run 6 months ago" / trend analysis / re-deriving
-- change events for any two months without re-running the pipeline.
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.tech_stack_snapshot` (
  domain            STRING NOT NULL,
  url               STRING,
  tech              ARRAY<STRUCT<technology STRING, categories ARRAY<STRING>, version STRING>>,
  rank              INT64,
  crawl_date        DATE NOT NULL,
  origin_count      INT64,
  loaded_at         TIMESTAMP NOT NULL
)
PARTITION BY crawl_date
CLUSTER BY domain
OPTIONS (
  description = 'Append-only monthly history, one row per (domain, crawl_date). Partitioned by crawl_date -- a query scoped to one month never scans the others.'
);

-- Change-events feed: buyer-intent signal, "domain X added/dropped tech Y
-- between month A and month B". Append-only event log.
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.tech_stack_change_events` (
  domain                STRING NOT NULL,
  event_type            STRING NOT NULL,  -- tech_change | new_domain | dropped_domain
  crawl_date            DATE NOT NULL,
  previous_crawl_date   DATE NOT NULL,
  added                 ARRAY<STRING>,
  dropped               ARRAY<STRING>,
  loaded_at             TIMESTAMP NOT NULL
)
PARTITION BY crawl_date
CLUSTER BY domain
OPTIONS (
  description = 'Append-only change-event log between consecutive snapshots. Buyer-intent signal source.'
);

-- ── Staging tables ───────────────────────────────────────────────────────
-- Transient landing zone for one load job. Truncated and reloaded on every
-- run (WRITE_TRUNCATE in land_to_warehouse.py) -- these are never read
-- directly by downstream consumers, only by the MERGE statements below.
-- Keeping them as real tables (not BigQuery temp tables) means a failed
-- MERGE leaves the staged batch inspectable for debugging instead of
-- vanishing with the session.

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_tech_stack_snapshot` (
  domain            STRING NOT NULL,
  url               STRING,
  tech              ARRAY<STRUCT<technology STRING, categories ARRAY<STRING>, version STRING>>,
  rank              INT64,
  crawl_date        DATE NOT NULL,
  origin_count      INT64
);

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_tech_stack_change_events` (
  domain                STRING NOT NULL,
  event_type            STRING NOT NULL,
  crawl_date            DATE NOT NULL,
  previous_crawl_date   DATE NOT NULL,
  added                 ARRAY<STRING>,
  dropped               ARRAY<STRING>
);
