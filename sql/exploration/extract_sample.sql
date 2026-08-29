-- extract_sample.sql
--
-- Same shape as sql/production/extract_snapshot.sql, but targets the free
-- httparchive.sample_data.pages_10k table instead of httparchive.crawl.pages.
--
-- pages_10k is a fixed ~10k-row mirror of the latest crawl -- it has no
-- meaningful `date` partition to filter on (there's only ever one crawl's
-- worth of data in it), so unlike the production query this one takes no
-- @crawl_date parameter. Whatever `date` value is actually present in the
-- rows is selected through as `crawl_date` so downstream code can label the
-- output correctly.
--
-- This is the query to run FIRST, before ever touching sql/production/*.sql
-- against the real multi-terabyte table -- it's free, small, and proves the
-- pipeline logic against real (if small) HTTP Archive data.

SELECT
  root_page,
  rank,
  date AS crawl_date,
  t.technology,
  t.categories,
  t.info AS version_info
FROM
  `httparchive.sample_data.pages_10k`,
  UNNEST(technologies) AS t
WHERE
  client = 'desktop'
  AND is_root_page = TRUE
;
