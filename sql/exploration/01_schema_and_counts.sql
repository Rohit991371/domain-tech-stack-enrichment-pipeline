-- Run against the free sample_data.pages_10k table. No date filter needed --
-- this table is already a fixed 10k-row mirror of the latest crawl, so it
-- carries no partition. Purpose: sanity-check the schema and get a feel for
-- what "one row per origin" actually looks like before touching production.

SELECT
  COUNT(0) AS total_rows,
  COUNT(DISTINCT root_page) AS distinct_root_pages,
  COUNTIF(is_root_page) AS root_page_rows,
  COUNTIF(NOT is_root_page) AS secondary_page_rows,
  COUNT(DISTINCT client) AS distinct_clients
FROM `httparchive.sample_data.pages_10k`;
