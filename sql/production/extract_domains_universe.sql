-- extract_domains_universe.sql
--
-- Companion to extract_snapshot.sql. That query UNNESTs `technologies`,
-- which silently drops any origin whose technologies array is empty
-- (UNNEST of an empty array produces zero rows, not a NULL row). If I only
-- used the UNNESTed query, an origin HTTP Archive successfully crawled but
-- detected nothing on would just vanish from my output -- indistinguishable
-- from an origin that was never crawled at all. That distinction matters for
-- validate.py's row-count gate: "fewer domains than last month" should mean
-- fewer domains were crawled, not "fewer domains happened to run detectable
-- tech".
--
-- This query has no UNNEST, so every crawled root page is present exactly
-- once, and it's cheap: same partition/cluster filters, and no repeated
-- fields expanded, but it does still touch the `technologies` column
-- footprint via ARRAY_LENGTH (BigQuery only bills the record subfields
-- actually referenced, so this is far cheaper than the UNNEST query).
--
-- build_snapshot.py LEFT JOINs extract_snapshot.sql's rows onto this
-- universe on root_page, so domains with zero detected technologies still
-- get a row with an empty tech array rather than disappearing.

SELECT
  root_page,
  rank,
  date AS crawl_date,
  ARRAY_LENGTH(technologies) AS tech_count
FROM
  `httparchive.crawl.pages`
WHERE
  date = @crawl_date
  AND client = 'desktop'
  AND is_root_page = TRUE
;
