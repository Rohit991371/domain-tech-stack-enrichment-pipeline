-- extract_sample_universe.sql
--
-- Companion to extract_sample.sql, same relationship as
-- sql/production/extract_domains_universe.sql is to extract_snapshot.sql:
-- every origin in the sample, including ones with zero detected
-- technologies, so build_snapshot.py doesn't lose them to UNNEST's
-- empty-array behavior.

SELECT
  root_page,
  rank,
  date AS crawl_date,
  ARRAY_LENGTH(technologies) AS tech_count
FROM
  `httparchive.sample_data.pages_10k`
WHERE
  client = 'desktop'
  AND is_root_page = TRUE
;
