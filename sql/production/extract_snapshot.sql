-- extract_snapshot.sql
--
-- Pulls one month of origin-level technology detections from HTTP Archive.
-- Run ONLY through pipeline/extract.py, which forces a --dry_run first and
-- refuses to execute if the estimated scan exceeds
-- guardrails.max_scan_bytes_production in config.yaml.
--
-- Grain of the OUTPUT ROWS: one row per origin (root_page), NOT per domain.
-- Domain-level aggregation (origin -> registrable domain, union of tech
-- across origins) happens later in pipeline/build_snapshot.py, in Python,
-- against the small extracted result -- not in BigQuery. Doing the
-- aggregation in SQL over the full table would cost the same bytes scanned
-- but be harder to unit test and harder to reason about when something
-- looks wrong at 3 AM.
--
-- Cost-control decisions baked into this query (see docs/design_doc.md
-- "the cost trap" for the full reasoning):
--   1. `date` is a required equality filter -- this table is partitioned by
--      date, so this is what keeps the scan to one month instead of the
--      full 15-year history.
--   2. `client` and `is_root_page` are the next two clustering keys, filtered
--      first, in cluster order, so BigQuery can prune blocks.
--   3. I select `technologies` and `root_page`/`rank` only -- never `payload`,
--      `lighthouse`, `custom_metrics`, or `summary`, which are the
--      multi-terabyte columns for this table.
--   4. client = 'desktop' only for v1. Mobile roughly doubles both the row
--      count and the scan cost for detections that are >95% identical to
--      desktop (Wappalyzer detections come from server-rendered HTML/headers,
--      which don't usually differ by client). This is a documented, revisit-able
--      trade-off, not an oversight.
--
-- Parameters (BigQuery named query parameters, supplied by extract.py):
--   @crawl_date  DATE   e.g. DATE '2026-08-01' -- must equal a value already
--                        confirmed to exist by pipeline/arrival_check.py.

SELECT
  root_page,                    -- origin URL; normalized to a registrable domain downstream
  rank,                         -- CrUX popularity rank (nullable; unranked origins keep NULL, not dropped)
  date AS crawl_date,
  t.technology,
  t.categories,
  t.info AS version_info
FROM
  `httparchive.crawl.pages`,
  UNNEST(technologies) AS t
WHERE
  date = @crawl_date
  AND client = 'desktop'
  AND is_root_page = TRUE
  -- Defensive filter: HTTP Archive occasionally has origins with an empty
  -- technologies array; UNNEST would drop those rows entirely (no NULL row
  -- emitted for an empty array), which is fine for this table since a
  -- domain with zero detected technologies still needs to be countable.
  -- I handle that separately in build_snapshot.py by extracting the
  -- distinct set of root_pages from a companion no-UNNEST query
  -- (see extract_domains_universe.sql) rather than inferring presence from
  -- this UNNESTed result.
;
