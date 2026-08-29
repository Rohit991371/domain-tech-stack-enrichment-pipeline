-- The grain trap: does more than one origin ever collapse into the same
-- registrable domain, and do they disagree on tech? I can't run
-- public-suffix-aware extraction in SQL easily, so this is a rough proxy
-- using a naive "strip scheme + www" transform, just to see whether
-- collisions exist at all in the sample. The real normalization
-- (tldextract, PSL-aware) happens in pipeline/normalize_domain.py.

WITH origins AS (
  SELECT
    root_page,
    REGEXP_REPLACE(
      REGEXP_REPLACE(root_page, r'^https?://', ''),
      r'^www\.', ''
    ) AS naive_host
  FROM `httparchive.sample_data.pages_10k`
  WHERE is_root_page
)
SELECT
  naive_host,
  COUNT(DISTINCT root_page) AS origin_variants,
  ARRAY_AGG(DISTINCT root_page) AS origins
FROM origins
GROUP BY naive_host
HAVING origin_variants > 1
ORDER BY origin_variants DESC
LIMIT 20;
