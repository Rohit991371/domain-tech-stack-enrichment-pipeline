-- merge_snapshot_history.sql
--
-- Appends the staging batch into the full history table, keyed by
-- (domain, crawl_date). MERGE rather than INSERT for the same reason as
-- merge_snapshot_latest.sql: re-landing the same month twice (a retried
-- job after a partial failure) must not create duplicate history rows --
-- it should just re-write that month's rows to match the latest staged
-- content.

MERGE `{project}.{dataset}.tech_stack_snapshot` AS target
USING `{project}.{dataset}.stg_tech_stack_snapshot` AS source
ON target.domain = source.domain AND target.crawl_date = source.crawl_date

WHEN MATCHED THEN
  UPDATE SET
    url          = source.url,
    tech         = source.tech,
    rank         = source.rank,
    origin_count = source.origin_count,
    loaded_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (domain, url, tech, rank, crawl_date, origin_count, loaded_at)
  VALUES (source.domain, source.url, source.tech, source.rank,
          source.crawl_date, source.origin_count, CURRENT_TIMESTAMP());
