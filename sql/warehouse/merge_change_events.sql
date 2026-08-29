-- merge_change_events.sql
--
-- Appends the staged change-events batch, keyed by the event's natural key:
-- (domain, crawl_date, previous_crawl_date, event_type). A given domain can
-- only have one event of a given type for a given month-pair, so this key
-- is enough to make re-landing the same diff idempotent.
--
-- WHEN MATCHED still updates added/dropped (rather than being a no-op) so
-- that if diff_snapshots.py is ever re-run after a bugfix and produces a
-- corrected event list for the same month-pair, re-landing it actually
-- corrects the warehouse row instead of leaving stale added/dropped arrays
-- silently in place.

MERGE `{project}.{dataset}.tech_stack_change_events` AS target
USING `{project}.{dataset}.stg_tech_stack_change_events` AS source
ON  target.domain              = source.domain
AND target.crawl_date          = source.crawl_date
AND target.previous_crawl_date = source.previous_crawl_date
AND target.event_type          = source.event_type

WHEN MATCHED THEN
  UPDATE SET
    added     = source.added,
    dropped   = source.dropped,
    loaded_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (domain, event_type, crawl_date, previous_crawl_date, added, dropped, loaded_at)
  VALUES (source.domain, source.event_type, source.crawl_date, source.previous_crawl_date,
          source.added, source.dropped, CURRENT_TIMESTAMP());
