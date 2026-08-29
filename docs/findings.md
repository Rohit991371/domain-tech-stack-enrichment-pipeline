# Findings: understanding the source before building

This is the "spend time understanding the source" step the brief asks for,
done before any pipeline code was written. Everything here is sourced from
har.fyi (the community docs site for HTTP Archive's BigQuery dataset) and
cross-checked against the `httparchive.crawl.pages` schema reference.

## 1. Release cycle -- the timing trap, in detail

Source: https://har.fyi/guides/release-cycle/

- HTTP Archive used to start its crawl on the 1st of the month. It doesn't
  anymore. It now starts as soon as that month's **Chrome UX Report (CrUX)**
  dataset is available, which lands on the **second Tuesday** of the month.
- The crawl then takes **1-2 weeks** to test ~12-16M mobile pages and
  ~12-16M desktop pages.
- Regardless of when the crawl actually started or finished, the resulting
  BigQuery partition is **always labelled the 1st of that month**
  (`date = '2026-08-01'`), even though the underlying test runs happened
  mid-to-late August.
- Net effect: for a crawl labelled `2026-08-01`, the BigQuery partition
  should not be expected to be complete before roughly **the third week of
  August**, and in a slow month it could be later. There is no published
  SLA guaranteeing a specific landing day.

**Implication for this pipeline:** a cron schedule that says "run on the
20th, assume this month's data exists" is not just occasionally wrong, it's
wrong by design given how the crawl is sourced from CrUX. `arrival_check.py`
therefore never infers arrival from the calendar date -- it queries the
partition directly and checks for a healthy row count before touching
anything.

## 2. Grain -- what one row of `crawl.pages` actually represents

Source: https://har.fyi/reference/tables/pages/

- `httparchive.crawl.pages` is **one row per page tested**, not one row per
  site and not one row per domain.
- As of April 2022, HTTP Archive tests **two pages per origin**: the root
  page and one secondary page. `is_root_page: BOOLEAN` distinguishes them.
- `root_page: STRING` is the origin followed by `/` (e.g.
  `https://shop.example.com/`) -- this is the field I normalize into a
  registrable domain, not `page`, which for secondary-page rows points at
  an arbitrary internal URL that tells me nothing extra about the
  company's tech stack that the root page detection doesn't already cover.
- Filtering `is_root_page = TRUE` gets me to "approximately one row per
  origin" as the assignment brief states. But an *origin* (`shop.example.com`)
  is still not a *registrable domain* (`example.com`) -- see the grain trap
  writeup in `design_doc.md`.
- `client: STRING` is `'desktop'` or `'mobile'` -- HTTP Archive crawls each
  origin twice, once per client, meaning the same origin can appear as two
  separate root-page rows in the same monthly partition.

## 3. The `technologies` field -- what Wappalyzer actually gives me

Source: https://har.fyi/reference/structs/technology/

- `technologies: ARRAY<RECORD>` per page, detected by **Wappalyzer**
  (HTTP Archive runs its own fork).
- Each element has:
  - `technology: STRING` -- the name, e.g. `"Shopify"`
  - `categories: ARRAY<STRING>` -- e.g. `["Ecommerce"]`. A single technology
    can carry more than one category (`HubSpot` shows up under both
    "Marketing automation" and "CRM" in practice).
  - `info: ARRAY<STRING>` -- free-text metadata, typically a version string,
    but **repeated**, because a page can load two versions of the same
    library (e.g. two jQuery widgets). Version parsing needs a regex over
    this array, not a direct field read, and garbage/pre-release version
    strings do show up (har.fyi's own docs call this out).
- **Empty `technologies` arrays exist.** `UNNEST(technologies)` on such a
  row produces **zero output rows**, not one row with a `NULL` technology.
  This is why the pipeline runs two separate extract queries
  (`extract_snapshot.sql` for the UNNESTed tech detail, and
  `extract_domains_universe.sql` for the un-UNNESTed universe of origins) --
  see `sql/production/extract_domains_universe.sql` for the full reasoning.

## 4. Cost mechanics -- what's actually expensive

Source: https://har.fyi/guides/minimizing-costs/, https://har.fyi/guides/getting-started/

- `httparchive.crawl.pages` is **partitioned by `date`** and **clustered by
  `client`, `is_root_page`, `rank`, `page`, in that order**. Filtering in
  that exact order lets BigQuery prune the most.
- `date` is described by har.fyi as **required for every query** over this
  table -- an unfiltered query is not just expensive, it's explicitly
  against the documented usage pattern.
- The table was ~30 TB/month as of Oct 2024 (`crawl.pages`); `crawl.requests`
  (which I don't touch) was ~199 TB. The brief's "two multi-terabyte
  columns" almost certainly refers to `payload` and `lighthouse` (or
  `custom_metrics`/`summary`), which is why my extract query never selects
  them.
- `LIMIT` does **not** reduce bytes scanned -- BigQuery scans the full
  filtered set before applying `LIMIT`. This is a documented trap in
  har.fyi itself, called out with a worked example (a `LIMIT 1` query that
  still processes 6.56 TB).
- `RECORD`/nested columns are billed per subfield referenced, not per whole
  column -- e.g. `custom_metrics.a11y` alone is ~30x cheaper than selecting
  all of `custom_metrics`. I apply the same logic: I select
  `technologies` (needed) but never `payload`, `lighthouse`,
  `custom_metrics`, or `summary`.
- Recommended dev-time safety nets, in order of preference for this project:
  `sample_data.pages_10k` (fixed size, zero risk, no `date` needed) during
  development; `TABLESAMPLE SYSTEM (n PERCENT)` or a `rank <=` filter for
  spot-checks against the full table if ever needed; `bq query --dry_run`
  before every non-trivial production query, always.

## 5. What this means for the pipeline's design

These four findings map directly onto the pipeline architecture:

| Finding | Pipeline response |
|---|---|
| Partition labelled 1st, lands mid/late month | `arrival_check.py` queries live row count, never infers from calendar date |
| One row per (origin, client) pair, two pages per origin | `is_root_page = TRUE`, `client = 'desktop'` filters in `extract_snapshot.sql`; origin -> domain aggregation happens in Python, not SQL |
| Empty `technologies` arrays vanish under UNNEST | Two-query extract: UNNESTed detail + un-UNNESTed universe, joined in `build_snapshot.py` |
| `date` + cluster-order filtering is what controls cost; `LIMIT` doesn't help | `extract_snapshot.sql` always filters `date`, `client`, `is_root_page` in that order; `extract.py` refuses to run without a passing dry-run first |
