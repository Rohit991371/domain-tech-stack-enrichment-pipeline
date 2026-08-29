-- merge_snapshot_latest.sql
--
-- Upserts the just-loaded staging batch into tech_stack_snapshot_latest,
-- keyed by domain only (this table holds ONE row per domain: "right now").
--
-- Idempotency + correctness, both load-bearing for the "3 AM retry" story
-- in design_doc.md section 4:
--   1. Re-running this exact statement twice with the same staging content
--      produces the same end state both times (MERGE, not INSERT).
--   2. The `AND source.crawl_date >= target.crawl_date` guard on the UPDATE
--      branch means landing an OLDER month after a NEWER one has already
--      landed (e.g. a delayed retry of July after August already succeeded)
--      cannot silently overwrite newer data with stale data. It's the same
--      "don't let a bad/late run clobber a good one" principle as
--      arrival_check.py and validate.py, applied at the warehouse-write step.

MERGE `{project}.{dataset}.tech_stack_snapshot_latest` AS target
USING `{project}.{dataset}.stg_tech_stack_snapshot` AS source
ON target.domain = source.domain

WHEN MATCHED AND source.crawl_date >= target.crawl_date THEN
  UPDATE SET
    url          = source.url,
    tech         = source.tech,
    rank         = source.rank,
    crawl_date   = source.crawl_date,
    origin_count = source.origin_count,
    loaded_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (domain, url, tech, rank, crawl_date, origin_count, loaded_at)
  VALUES (source.domain, source.url, source.tech, source.rank,
          source.crawl_date, source.origin_count, CURRENT_TIMESTAMP());
