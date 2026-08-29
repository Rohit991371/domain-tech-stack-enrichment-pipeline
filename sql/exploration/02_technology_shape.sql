-- How many technologies does a typical origin carry, and what does the
-- struct actually contain? Confirms technology/categories/info shape from
-- har.fyi before I design build_snapshot.py's aggregation.

SELECT
  root_page,
  ARRAY_LENGTH(technologies) AS tech_count,
  (SELECT ARRAY_AGG(t.technology) FROM UNNEST(technologies) AS t) AS technologies_sample
FROM `httparchive.sample_data.pages_10k`
WHERE is_root_page
ORDER BY tech_count DESC
LIMIT 20;

-- Top 15 technologies by number of distinct origins in the sample -- gives
-- me a feel for which categories dominate (CMS/analytics almost always do).
SELECT
  t.technology,
  ANY_VALUE(t.categories) AS categories,
  COUNT(DISTINCT root_page) AS origin_count
FROM
  `httparchive.sample_data.pages_10k`,
  UNNEST(technologies) AS t
WHERE is_root_page
GROUP BY t.technology
ORDER BY origin_count DESC
LIMIT 15;
