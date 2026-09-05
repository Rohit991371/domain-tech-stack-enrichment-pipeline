# Design document

Companion to `docs/findings.md` (source investigation) and
`docs/schema_draft.md` (output shapes). This covers the three traps named
in the brief, the cost model, warehouse landing, and failure modes.

---

## 1. The three traps

### 1.1 The timing trap

**What it is:** crawls are labelled the 1st of the month but don't land
until CrUX is available (2nd Tuesday) plus 1-2 weeks of crawling -- see
`findings.md` §1. A schedule that assumes "day N of the month -> data
exists" will periodically hit an empty or partial partition.

**What goes wrong if unhandled:** a scheduled job runs on the 20th, queries
`date = '2026-08-01'`, gets 0 (or a handful of) rows because the crawl is
still running, and if that result is written straight to the production
snapshot table with a naive overwrite, last month's good data is destroyed
by an empty result -- silent data loss, discovered only when a salesperson
asks why every domain in the system suddenly has no tech stack.

**My answer:** `arrival_check.py` runs first, always, and is the *only*
stage allowed to reason from "does data exist" rather than "is the data
correct." It queries the partition directly (`COUNT(0)` filtered to
`client = 'desktop', is_root_page = TRUE`) and compares against
`guardrails.min_row_count` in `config.yaml`. Below threshold => the
pipeline stops immediately, before any extraction, and nothing on disk is
touched. This is deliberately a *different* check from the validation gate
in §1.3 below: arrival answers "is there enough here to bother extracting,"
validation answers "is what I built from it actually good." Conflating
them would mean a legitimately-arrived-but-anomalous month either gets
silently accepted (bad) or a not-yet-arrived month gets a scary "validation
failed" alert instead of the calmer, expected "not arrived yet, will retry"
outcome.

**Trade-off accepted:** `min_row_count` is a static threshold, not a
statistical model of "expected row count for this month." A slow-but-real
partial landing that happens to clear the threshold with genuinely
incomplete data would pass arrival_check and only get caught (if it's bad
enough) by validation's row-count-drift check against the previous month.
A static threshold plus a drift check is the defensible v1; a production
version would want the threshold to scale with a trailing average of
recent months' row counts instead of one fixed number.

### 1.2 The grain trap

**What it is:** `crawl.pages` is ~one row per origin (root + one secondary
page, times two clients) -- see `findings.md` §2. The business wants one
row per **registrable domain**. Multiple origins (`www.example.com`,
`shop.example.com`) can roll up into the same domain (`example.com`) and
can disagree on detected tech.

**My rule:** for each registrable domain, union the distinct technologies
detected across every origin that normalizes to it; take the best
(numerically lowest) non-null CrUX rank across those origins as the
domain's rank, and that origin's URL as the representative `url`.

**Why union, not "pick one origin":** the product's job is "does this
company use Shopify." A marketing site on WordPress and a storefront on
Shopify are both real, current facts about the same company, and a sales
rep filtering for "companies on Shopify" wants this domain to show up.
Picking only the root origin (dropping `shop.example.com`'s signal) or only
the best-ranked origin would silently make the core feature worse. The
cost I accept: a domain's tech list can include tools that, strictly
speaking, live on a distinct subdomain/product area a prospect might not
consider "the same thing." I think that's the right trade-off for
lead-gen and buyer-intent use cases specifically -- a different downstream
consumer (e.g. someone building a *technical* audit of the marketing site
only) could re-derive a root-origin-only view from the same raw extract
(`data/raw/extract_<date>.json`, before aggregation), since I don't throw
that away.

**How it's built:** public-suffix-aware normalization (`tldextract`, not a
naive "last two labels" split) in `pipeline/normalize_domain.py`, unit
tested against exactly the case that breaks naive logic
(`example.co.uk` must not collapse to `co.uk` -- see
`tests/test_normalize_domain.py::test_multi_label_public_suffix_not_collapsed`).
Aggregation itself happens in Python (`build_snapshot.py`), not BigQuery
SQL, against the already-extracted (small, cost-bounded) result set -- see
§1.3 in `findings.md` for why doing this in SQL over the full table would
cost the same scan for a much harder-to-test/harder-to-debug
implementation.

**Origins that fail normalization** (malformed URLs with no parseable
suffix) are written to `data/rejected/`, not silently dropped -- see
`schema_draft.md` §3.

### 1.3 The cost trap

**What it is:** `payload`, `lighthouse`, and similar columns are
multi-terabyte; an unfiltered or carelessly-selected query against
`crawl.pages` can scan the full monthly partition (documented at ~30 TB as
of Oct 2024 for `crawl.pages` alone -- `findings.md` §4) instead of the
~150-250 GB a correctly filtered extract should cost per the brief.

**Structural (not just habitual) guardrails:**
1. `extract_snapshot.sql` and `extract_domains_universe.sql` never select
   `payload`, `lighthouse`, `custom_metrics`, or `summary`. Only
   `root_page`, `rank`, `date`, and `technologies` (needed) are selected.
2. Both queries filter `date`, then `client`, then `is_root_page` -- in
   that order, matching the table's documented cluster-column order, so
   BigQuery can actually prune blocks rather than just satisfying the
   `WHERE` clause after a fuller scan.
3. `pipeline/extract.py` **cannot execute a real, billed query without
   first dry-running it.** If the dry-run estimate exceeds
   `guardrails.max_scan_bytes_production` (300 GB in `config.yaml`), it
   raises and refuses to proceed -- there is no flag to skip this. This is
   enforced in code, not left as a habit for whoever runs it.
4. As a second, independent line of defense, the real (non-dry-run) query
   is also issued with `maximum_bytes_billed` set to the same ceiling, so
   even if the dry-run estimate were somehow wrong (BigQuery doesn't
   guarantee dry-run accuracy for clustered tables -- see `findings.md`),
   the actual execution still can't blow past the ceiling; it errors
   instead of billing.
5. `LIMIT` is never used as a cost-control mechanism anywhere in this
   codebase, because it isn't one (`findings.md` §4).
6. All development and testing happened against `sample_data.pages_10k`
   (free, fixed 10k rows, no `date` needed) or local mock fixtures
   (`pipeline/mock_source.py`) -- the production query was designed on
   paper from the schema docs and is meant to be dry-run-verified once
   against the real table before its first real execution, not iterated on
   against the full table.

**Confirmed for real** (2026-08-27): `python pipeline/extract.py
--crawl-date 2026-08-01 --dry-run` against the real
`httparchive.crawl.pages` table came back at 29.84 GB per query, ~59.68 GB
combined -- see `sql/exploration/04_dry_run_notes.md` for the raw evidence
and §2.2 below for the updated cost model. This was comfortably under the
300 GB ceiling, so the guardrail never had to refuse anything, but it's
worth noting the ceiling *would* have refused a real run had this number
come back higher -- that check ran regardless of the outcome.

---

## 2. Cost model

All figures below use BigQuery on-demand pricing as of mid-2026:
**$6.25/TiB scanned**, with the **first 1 TiB/month free**. Storage is
$0.02/GB/month active (not a meaningful cost here -- my own output tables
are a few hundred MB at most; the source data storage cost is Google's, as
the owner of the public dataset, not mine).

### 2.1 Setup cost

One-time: a free-tier GCP project, no billing account strictly required to
develop against `sample_data.pages_10k` at all (that table has no scan
cost risk). Enabling billing is only needed to run the production query
against `crawl.pages`, and even then the first 1 TiB/month is free.
**Setup cost: effectively $0.**

### 2.2 Steady-state monthly cost

**Measured, not estimated** (see `sql/exploration/04_dry_run_notes.md` for
the raw dry-run evidence): a real dry-run of both `extract_snapshot.sql`
and `extract_domains_universe.sql` against `httparchive.crawl.pages` for
`2026-08-01` came back at **29.84 GB each**, identical between the two
queries because BigQuery bills by column footprint referenced, not by
whether that column is UNNESTed (`extract_snapshot.sql`) or just measured
with `ARRAY_LENGTH` (`extract_domains_universe.sql`) -- both touch the same
`technologies` column. That's **~59.68 GB combined per month**, well
under the brief's own ~150-250 GB estimate (my column selection --
`root_page`, `rank`, `technologies` only -- is evidently tighter than that
baseline assumes) and comfortably inside the 1 TiB/month free tier.

- **Steady-state monthly cost: $0**, with the free tier having roughly
  16x headroom left over (1024 GiB free vs. ~60 GB used) even before
  accounting for any other BigQuery usage on the same project.
- Even without a free tier at all, 60 GB ≈ 0.056 TiB × $6.25 ≈
  **~$0.35/month**. This is a trivially cheap pipeline to run monthly --
  the real cost risk remains a single mistaken unfiltered query
  (`SELECT *` with no `date` filter, or selecting `payload`), not the
  steady-state extraction.

**Note on what this figure actually measures:** this is the *production*
cost, not a test-data estimate. `sample_data.pages_10k` (used for
development) has no scan cost at all -- it's a free, fixed 10k-row mirror.
The 29.84 GB/query figure above comes from dry-running `extract_snapshot.sql`
and `extract_domains_universe.sql` against the real, full-size
`httparchive.crawl.pages` table for one month, with no `LIMIT` applied.
It is **not** affected by the `--limit-rows 500` flag used elsewhere in this
project to cap what gets written to local disk during proof runs --
`findings.md` §4 documents why `LIMIT` doesn't reduce BigQuery's billed
scan (the engine still has to read every matching row's referenced columns
before truncating the output), so this dry-run number reflects the actual
cost of extracting a full month at full row volume, not a scaled-down
sample of it. In other words, the cost model in this section already *is*
the production-scale number, measured against the production table.

### 2.3 36-month backfill cost, and whether it's worth it

Using the **measured** per-month figure (§2.2) instead of the brief's
estimate: 36 months × ~60 GB/month ≈ **2.16 TB** total scan (a single
backfill run wouldn't benefit from the free tier resetting monthly the way
steady-state runs do -- it would burn through the 1 TiB free allotment
within the first ~17 months' worth of scanning, in one billing period).

- 2.16 TB ≈ 2.11 TiB total scan, minus 1 TiB free tier for the month it
  runs in, ≈ 1.11 TiB billed × $6.25/TiB ≈ **~$7 one-time** for the scan
  itself -- an order of magnitude cheaper than my original ~$50 estimate,
  now that I have a real measured per-month cost instead of the brief's
  conservative 150-250 GB ballpark.
- Storage for 36 monthly snapshot outputs (my own aggregated tables, not
  the raw HTTP Archive data) is negligible -- each snapshot is on the order
  of a few hundred MB to low GB depending on domain-universe size at
  production scale; 36 of them is still well under 100 GB, i.e. a couple
  dollars a month in storage at most.
- **Total 36-month backfill: roughly $10-15 all-in**, a one-time cost, not
  recurring.

**Is it worth it?** At ~$10-15 one-time, cost is now even more clearly not
the limiting factor -- the real question is whether 3 years of
technology-adoption history is useful for the stated buyer-intent use case.
I still think a **partial backfill (12 months) is the better default**,
not because of cost, but because of relevance: most "just adopted X" or
"migrating off Y" signals that matter to a sales team decay in usefulness
well before 3 years, and Wappalyzer's detection coverage/accuracy for a
given technology can itself change over that window (new detections get
added to the Wappalyzer fork over time -- see findings.md §3), so very old
snapshots aren't strictly comparable to recent ones on a like-for-like
basis anyway. If historical trend analysis (e.g. "is Shopify adoption
accelerating industry-wide") becomes a real, named requirement, the full
36-month backfill is cheap enough to run at that point -- there's no reason
to pay for it speculatively now.

---

## 3. Landing this in a warehouse (Snowflake or similar)

### 3.0 Why BigQuery-native tables instead of an actual Snowflake instance

The brief says "Snowflake/similar" and asks what the warehouse mechanics
would look like -- it doesn't require a literal Snowflake account. I
already have a real GCP trial account with real BigQuery access (used for
the extraction side, §2), so I landed this in **BigQuery-native tables
in my own project** rather than sketching Snowflake DDL I couldn't
actually run. The mechanics are the same shape either way -- stage a
batch, load it, `MERGE` it into a target table -- so nothing here is
warehouse-specific reasoning; swapping the target for Snowflake later
means re-pointing `pipeline/land_to_warehouse.py`'s load/merge calls at
Snowflake's API and rewriting `sql/warehouse/*.sql` in Snowflake SQL
(`COPY INTO` + `MERGE`, which Snowflake also supports natively), not
redesigning the approach.

### 3.1 Target tables

Two production tables plus a snapshot-latest convenience table, mirroring
the two outputs in `schema_draft.md`. Full DDL: `sql/warehouse/create_tables.sql`.

```sql
-- tech_stack_snapshot_latest: ONE row per domain, upserted every month.
-- "What does this company use right now" -- what a sales query joins
-- against 95% of the time.
CREATE TABLE tech_stack_snapshot_latest (
  domain        STRING NOT NULL,   -- join key; see contract below
  url           STRING,
  tech          ARRAY<STRUCT<technology STRING, categories ARRAY<STRING>, version STRING>>,
  rank          INT64,
  crawl_date    DATE NOT NULL,
  origin_count  INT64,
  loaded_at     TIMESTAMP NOT NULL
) CLUSTER BY domain;

-- tech_stack_snapshot: append-only history, one row per (domain, crawl_date).
-- Partitioned by crawl_date so a query scoped to one month never scans others.
CREATE TABLE tech_stack_snapshot (
  ...  -- same columns as above
) PARTITION BY crawl_date CLUSTER BY domain;

-- tech_stack_change_events: append-only event log
CREATE TABLE tech_stack_change_events (
  domain                STRING NOT NULL,
  event_type            STRING NOT NULL,  -- tech_change | new_domain | dropped_domain
  crawl_date            DATE NOT NULL,
  previous_crawl_date   DATE NOT NULL,
  added                 ARRAY<STRING>,
  dropped               ARRAY<STRING>,
  loaded_at             TIMESTAMP NOT NULL
) PARTITION BY crawl_date CLUSTER BY domain;
```

Loading approach: **both** a cheap-to-query `tech_stack_snapshot_latest`
(upserted) and the full `tech_stack_snapshot` history (append-only), since
the marginal storage cost of keeping history is small (§2.3) relative to
the value of not having to re-derive it later -- e.g. re-deriving a change-
events batch for any two arbitrary months without re-running the pipeline.

### 3.2 The join key contract

This is the single most important sentence in this document for a
downstream engineer:

> `domain` is the **registrable domain (eTLD+1)**, derived with a
> public-suffix-aware parser (`tldextract`, using the Mozilla Public
> Suffix List), **lowercased**, with **no scheme, path, port, query
> string, or leading `www.`**.

Concretely: `https://www.Example.com:8443/path?x=1` and
`https://shop.example.com/` both normalize to `example.com`. This must be
the exact same normalization Firmable's own `companies.domain` column uses
for the join (`companies.domain = tech_stack_snapshot.domain`) to actually
match rows -- if `companies.domain` is stored with a leading `www.` or
mixed case, the join will silently under-match, which looks like "I don't
have tech data for this company" rather than the join bug it actually is.
I'd flag this explicitly to whoever owns `companies.domain` before this
ships, and recommend either (a) normalizing `companies.domain` the same way
at write time, or (b) exposing a normalized view/computed column on top of
it, rather than asking every downstream query to remember to normalize on
the fly.

### 3.3 What "landing" actually runs

`pipeline/run_pipeline.py`'s final stage (loading the validated snapshot)
writes to local JSON. `pipeline/land_to_warehouse.py` is the separate,
explicit next step, and its actual path is:

```
local JSON (data/snapshots/, data/change_events/)
    -> NDJSON reshape
    -> GCS staging upload
    -> BigQuery staging table (WRITE_TRUNCATE)
    -> MERGE into the production table(s)
```

This is a deliberate design choice: the expensive, correctness-critical
part of this pipeline (getting from raw HTTP Archive rows to a validated
domain-level snapshot) is entirely warehouse-agnostic, so swapping
Snowflake for BigQuery-native tables or Redshift later doesn't require
touching `arrival_check.py`, `normalize_domain.py`, `build_snapshot.py`,
`validate.py`, or `diff_snapshots.py` at all -- only `land_to_warehouse.py`
and `sql/warehouse/*.sql` would change.

### 3.4 Why local disk, not straight to BigQuery -- and why 500 rows,
not the 10k sample

**Why the extract doesn't write straight into the warehouse.** BigQuery
does support a server-side `EXPORT DATA`/`bq extract` that writes query
results straight to GCS with no local file and no client machine involved.
That's not used here, because it would export raw, origin-level rows
*before* any of the business logic that makes this a domain-level, join-
ready product has run: `normalize_domain.py` (public-suffix-aware
origin -> registrable-domain extraction), `build_snapshot.py` (the
origin -> domain union rule from §1.2), and `validate.py` (the six-check
gate from §4) are all Python, and none of that currently runs inside
BigQuery. Getting a real snapshot without a local-disk stop would mean
porting that logic into BigQuery SQL/UDFs -- a genuine architecture change,
not a small patch, and out of scope for what this project needed to prove.
Landing itself (validated snapshot -> GCS) is the easier half of this --
`google-cloud-storage` supports streaming writes, so `land_to_warehouse.py`
could skip the local NDJSON file and stream straight to the GCS blob with a
small, low-risk change. That still assumes a normalized snapshot already
exists as a Python object, though, which is why it doesn't solve the first
problem on its own. The local-disk-first pattern is kept as-is for this
submission and documented here as the "how this scales past local disk"
plan, rather than built under deadline pressure -- the brief asks for a
small, cost-controlled proof run on the free tier, not a production-scale
rebuild of the extract path.

**Why only the 500-row production runs are landed, not the 10k-row sample
run.** `run_pipeline.py --land` deliberately skips landing anything
produced by `--source sample`. Two reasons: first, `sample_data.pages_10k`
has no real `crawl_date` of its own -- it's a static mirror of "the latest
crawl," not a dated monthly partition -- so landing it into a
`crawl_date`-partitioned production history table would mean picking an
arbitrary, not-actually-representative date, which is exactly the kind of
ambiguity the join-key contract in §3.2 is trying to eliminate. Second, the
sample table's entire purpose is cost-free *development* iteration --
mixing its output into the same production tables that a downstream sales
team queries would blur the line between "verified real data" and "dev
scratch data" in the warehouse itself. The landing code is still proven to
be source-agnostic without taking that risk: `python
pipeline/land_to_warehouse.py --crawl-date sample --dry-run` runs the full
NDJSON/GCS/staging path against the sample output and prints what it
would do, without writing anything.

The real production runs (`--crawl-date 2026-07-01 --limit-rows 500` and
`2026-08-01 --limit-rows 500`) are what's actually landed, for the reason
in §3.3/§2.2: `--limit-rows` caps what's written to local disk, not what
BigQuery bills or what the query matches server-side, so 500 rows is a
real, inspectable slice of genuine production output, not a synthetic
stand-in. Scaling this to a full production month (12-16M rows) needs two
changes, neither of which touches the pipeline's logic: (1) drop
`--limit-rows` so the extract writes the full matched row set instead of a
capped slice, and (2) move the local JSON/NDJSON intermediate off a laptop
disk and onto something sized for it (a Cloud Storage-backed temp path, or
the streaming-upload change described in §3.4 above) -- purely an
infrastructure change, not a rewrite of `normalize_domain.py`,
`build_snapshot.py`, `validate.py`, or the `MERGE` logic in
`sql/warehouse/`.

---

## 4. Failure modes, and what pages a human at 3 AM

| Failure | Detected by | Pipeline behavior | What a human needs to do |
|---|---|---|---|
| Crawl hasn't landed yet (timing trap) | `arrival_check.py` row-count check | STOP before extraction. Nothing written or touched. Exit code 1. | Nothing urgent -- this is expected some months. Retry later (or let the next scheduled run catch it). Not an alert-worthy page by itself unless it persists past ~day 25 of the month. |
| Source landed but with a broken/anomalous subset (e.g. a bug in that month's HTTP Archive crawl itself) | `validate.py` row-count-drift check (vs previous month, both directions) or `tech_coverage_ratio` check | Snapshot **file is written to disk** (for inspection) but **not promoted** -- the previous good snapshot stays live. Exit code 2. | **Page a human.** Look at `data/snapshots/snapshot_<date>.json` and the specific failed check in the run log. Decide: wait and retry next week (crawl was genuinely still landing), or escalate to HTTP Archive's own status/discuss channel if the anomaly looks like it's on their end. |
| BigQuery query estimate exceeds the cost ceiling (cost trap, e.g. someone edited the SQL and broke a filter) | `extract.py`'s dry-run check, enforced before any real query | Refuses to run. Raises with the estimated bytes and the ceiling. Nothing extracted. | Review the SQL diff. This should essentially never fire in normal operation -- if it does, something changed in the query, not just in the data. |
| Query estimate was fine but actual execution somehow bills more (clustered-table estimate inaccuracy is a documented BigQuery limitation) | `maximum_bytes_billed` at execution time | Query errors out mid-run rather than completing over-budget. No partial/corrupt data written. | Investigate why the dry-run and actual diverged; likely a clustering/partition-pruning edge case worth understanding before re-running. |
| Domain normalization fails on some origins (malformed URL) | `normalize_domain.py` returns `is_valid=False`; `build_snapshot.py` routes these to `data/rejected/` instead of dropping silently | Pipeline continues (a handful of malformed rows shouldn't block an entire month), but the rejected count is visible in `build_snapshot.py`'s output and validate.py's checks account for it. | Only urgent if `rejected_count` spikes unexpectedly -- check `data/rejected/rejected_<date>.json` for a pattern (e.g. a new malformed-URL format from HTTP Archive) rather than assuming it's noise. |
| Aggregation bug produces duplicate domain rows | `validate.py`'s `no_duplicate_domains` check | STOP, same as any other validation failure. Exit code 2. | This should never happen given the aggregation logic (a Python dict keyed by domain can't produce duplicates by construction) -- if it fires, it means the code itself was changed in a way that broke that invariant; treat it as a code bug, not a data bug. |
| Diff engine run against a snapshot that never passed validation | Structurally prevented -- `run_pipeline.py` only calls `diff_snapshots()` after `validate_snapshot().passed` is confirmed `True` for the current month, and only against a `previous_crawl_date` snapshot file that exists on disk (which itself could only have been written by a prior, validated run) | N/A -- not reachable in normal operation | If someone manually invokes `diff_snapshots.py` directly against two arbitrary snapshot files (bypassing `run_pipeline.py`), that's on them; the standalone script doesn't re-check validation status, by design, so it stays usable for ad-hoc "what changed between any two snapshots" investigation. |

**Every run, pass or fail, is logged** to `data/pipeline_state.json`
(append-only, last 200 runs kept, oldest dropped past that) by
`run_pipeline.py`. This is the first thing to check at 3 AM: what did the
last run actually do, at which stage did it stop, and what did each stage
report -- without needing to have caught the terminal output live or read
source code to reconstruct it.

Each entry is one JSON object appended to the array, and looks like this:

```json
{
  "crawl_date": "2026-08-01",
  "previous_crawl_date": "2026-07-01",
  "mock": false,
  "dry_run": false,
  "source": "production",
  "limit_rows": 500,
  "stages": {
    "arrival_check": { "arrived": true, "reason": "..." },
    "extract": { "row_count": 500, "bytes_scanned": "..." },
    "build_snapshot": { "domain_count": 431 },
    "validate": { "passed": true, "checks": { "...": "..." } },
    "diff_snapshots": { "added": 12, "dropped": 5 }
  },
  "outcome": "LOADED",
  "timestamp": "2026-08-27T09:14:02+00:00"
}
```

The field that matters most for a fast triage is `outcome`, which is
always one of a fixed set: `LOADED`, `STOPPED_ARRIVAL_CHECK_FAILED`,
`STOPPED_VALIDATION_FAILED`, `DRY_RUN_ONLY`, `ERROR_EXTRACT`, or
`ERROR_BUILD_SNAPSHOT` -- that alone says which guardrail (if any) stopped
the run, and `stages` has the per-stage detail (e.g. exactly which of the
six validation checks failed) for anyone who needs to go one level deeper.
The intent is that a human paged for this pipeline should never need to
read source code to figure out what happened -- the run log plus the
stage-specific artifact (`data/snapshots/`, `data/rejected/`, or the
dry-run error message) should be enough. It's gitignored like the other
`data/` outputs (§3.4) since it's local run history, not source.

---

## 5. AI-native: the decision not to use an LLM in v1

The brief explicitly allows this as a first-class answer: *"This pipeline
can legitimately be built with zero LLM calls."* I took that option for
v1, and this section is the defense of that choice, not an excuse for
skipping the AI-native section.

**Where an LLM could plausibly help**, per the brief's own examples:
- Classifying technologies into sales-relevant groupings (e.g. bucketing
  "Shopify", "Magento", "BigCommerce" under a normalized "Ecommerce
  Platform" label for filtering).
- Summarizing a domain's stack in natural language for a rep.
- Deriving a "migration-intent" flag from version-string changes.

**Why I didn't, for v1:**
1. **The source is already structured.** Wappalyzer's `technologies`
   struct already gives me `technology` + `categories` as clean,
   machine-readable fields (`findings.md` §3) -- across ~4,000 known
   technologies and 108 categories per the brief. A classification task is
   only worth an LLM call if the input is unstructured; here it mostly
   isn't. The `categories` array Wappalyzer already assigns *is* the
   sales-relevant grouping in most cases.
2. **Determinism matters more than fluency here.** "Does this domain use
   Shopify" needs to be a stable, reproducible, auditable fact a sales team
   can filter on and trust every month -- not a fact that could shift
   because a model version changed or a prompt got tweaked. SQL/rules give
   me that; an LLM call in the hot path doesn't, without a lot of
   additional eval/guardrail infrastructure to make it trustworthy.
3. **Cost math doesn't favor it at this scale.** Rough order of magnitude:
   10-16M domains/month × ~200 input tokens (a domain's tech list) ×
   even a cheap model (~$0.15/M input tokens) is already **~$300-480/month**
   for a single classification pass over the *entire* crawl -- and that's
   before output tokens, retries, or running it for every one of the
   ~4,000 possible technologies rather than just the ones a given domain
   has. Compare to the **$0-1.50/month** SQL-only steady-state cost from
   §2.2. An LLM-based classification layer would be the single largest
   line item in this pipeline's budget for a task Wappalyzer's own
   `categories` field mostly already solves for free.
4. **Latency and failure surface.** Every LLM call is a new thing that can
   time out, rate-limit, or silently degrade in quality -- exactly the kind
   of new failure mode §4's "3 AM" table is designed to keep to a minimum.
   Adding one here would mean adding a new row to that table for a feature
   that isn't required to hit the brief's core deliverables.

**What would change this decision:** if a *specific, named* downstream
requirement showed up that genuinely needs judgment beyond what
`categories` provides -- e.g. "flag domains that look like they're
mid-migration off a legacy platform" (inferring intent from a *pattern* of
changes over multiple months, not just a static category label) -- that's
a better-shaped LLM task, because it's inference over ambiguous signal
rather than relabeling already-structured data.

To be clear about why v1 stops here rather than building that feature now:
this isn't a claim that an LLM has no place in this pipeline, only that
within this project's timeframe the core, correctness-critical path
(arrival check -> extract -> normalize -> aggregate -> validate -> load ->
diff) was the right thing to spend the time on, and a judgment-call feature
like migration-intent detection deserved more room than was left to do it
properly. It's a strong candidate for a v2, not a rejected idea. If/when it
gets built, it should ship with the scaffolding the brief asks for and this
submission doesn't yet have: a `skills/http-archive-investigator/SKILL.md`
packaging the dev-workflow discipline in §1.3 as a reusable agent skill, a
20-30 example hand-labelled eval set for the migration-intent
classification specifically, a request/response/model/prompt-version/
latency/cost trace schema, and versioned prompts under `prompts/`. None of
that exists in this submission because no LLM call exists in this
submission -- adding the scaffolding without the feature it scaffolds
would be theater, not engineering.

## 6. v2: the agentic autopilot layer

Section 5 ended by naming this exact gap as a strong v2 candidate, and now
the pipeline needs to "run on autopilot mode, fully agentic driven instead
of deterministic legacy way of running things."

**First pass at this request was wrong, and worth naming.** My first
instinct was to look for places inside the existing deterministic stages
(arrival check, validation) to swap in LLM judgment. That would have been
exactly the failure mode I criticize elsewhere in this doc: relabeling
already-correct deterministic control flow with agentic vocabulary
without changing what the system actually does. The real gap wasn't
missing judgment inside the pipeline -- it was that a human (me) still has
to manually type `python pipeline/run_pipeline.py --crawl-date ...` every
month. "Legacy way of running things" means *invoked*, not *unintelligent*.
"Autopilot" means the system decides *when* to check itself, without being
told to.

That reframing produced a three-layer design, each layer solving a
distinct problem, with the deterministic core from sections 1-5 untouched
underneath all three:

**Layer 1 -- self-triggering.** A GitHub Actions `schedule:` cron
(`.github/workflows/autopilot.yml`) wakes the pipeline with no manual
invocation. The cron window itself is grounded in `findings.md` §1 (crawl
labelled the 1st, doesn't land until ~1-2 weeks after the 2nd Tuesday) --
it starts checking from the 15th, not the 1st, so it isn't burning runs on
days already known not to have data. I deliberately did not attempt a live
scheduled demo in the Loom given the free-tier trial account's time
constraints; this layer is proven instead by local testing plus the workflow
file and setup steps documented in detail below.

**Layer 2 -- autonomous retry/diagnosis, bounded by a deterministic circuit
breaker.** `pipeline/orchestrator_agent.py` wraps the existing arrival
check: each scheduled wake calls it, and if the partition hasn't landed,
the agent reasons over the specific failure (attempt number, days past the
expected arrival window, the known release-cycle pattern) to recommend a
retry interval or an escalation, rather than a blind fixed cron interval
retrying forever. This is the one place in the whole pipeline where I think
LLM judgment is actually earning its keep over a plain `if/else`: "is this
still within the normal range of when HTTP Archive crawls land, given how
many times I've already checked" is a fuzzier call than anything else in
the pipeline, and the cost of getting it slightly wrong (checking a bit too
early or late) is low.

That said, the *ceiling* on this judgment is not agentic. `config.yaml`'s
`agentic.max_retries` and `agentic.max_days_past_expected` are checked in
plain Python in `_decide_retry_or_escalate()` *before* any LLM is even
called -- if either limit is hit, the function returns `"recommendation":
"escalate"` without consulting the model at all. And as defense in depth,
if a model is called and (contrary to its instructions) still recommends
`retry_later` past the limit, the code overrides it. The circuit breaker is
therefore enforced twice, and only one of those two enforcements involves
the LLM's cooperation. This is the same category of guardrail as
`validate.py`'s thresholds: a number in `config.yaml`, checked in Python,
that no prompt can talk its way around.

**Layer 3 -- human-in-the-loop approval before the one irreversible
action.** Once arrival, extraction, normalization, and `validate.py` all
deterministically PASS, `proceed_to_load()` does not fire automatically.
The run stops in a `pending_approval` state, `orchestrator_agent.py`
writes a plain-language summary of what passed, and a second GitHub
Actions job -- gated behind a `production-approval` *environment* with a
required reviewer -- is the only thing that can call
`proceed_to_load()`. I added this checkpoint myself, and kept it after reviewing the design: writing a new
snapshot over `tech_stack_snapshot_latest` is the one step in this whole
pipeline that isn't cheaply reversible the way a re-run of an earlier
stage is, so it gets a human at the moment of highest stakes, same
category as the validation gate, just enforced by a person instead of a
rule. Critically, `proceed_to_load()` does not trust that it was invoked
correctly -- it re-runs `validate_snapshot()` itself, fresh, every time
it's called, and refuses (exit code 2) if that fresh check isn't PASS,
regardless of what any state file or prior approval says. This is checked
directly in `tests/test_orchestrator_agent.py`
(`test_proceed_to_load_refuses_when_no_snapshot_exists`,
`test_proceed_to_load_refuses_when_snapshot_too_small`).

**This guardrail fired for real, unprompted, during GitHub Actions testing --
not just in the test suite.** `min_domain_count` in `config.yaml` has two
values: `1000` (real production scale) and `min_domain_count_mock: 200`
(deliberately small, for local `--mock` fixture runs). `check`'s extraction
step treats `--limit-rows` the same way it treats `--mock` for validation
purposes -- `use_small_thresholds = mock or bool(limit_rows)` -- so a
manually-capped test run (`--limit-rows 500`, to keep a live GitHub Actions
test cheap) is validated against the relaxed 200-domain floor and passes
(358 domains cleared it). But `proceed_to_load()` -- the function `approve`
calls -- deliberately does **not** inherit that leniency:
`use_small_thresholds = mock` there, with no `limit_rows` parameter at all.
Re-validating that same 358-domain snapshot against the real 1000-domain
floor correctly failed it, and `approve` refused to load, exit code 2,
before touching BigQuery:
 
```
"min_domain_count": { "passed": false, "detail": "358 domains (min 1000)" }
REFUSED: validate_snapshot() did not return PASS when re-checked just now.
```
 
This is intentional, not a bug found and left unfixed: however much leeway
a test run is given at `check` time to keep costs down, the moment
something is about to become the new production snapshot, the real bar
applies unconditionally -- the same "no shortcuts at the irreversible
step" principle as everywhere else `proceed_to_load()` behaves. In normal
operation this never triggers: a real monthly extraction with no
`--limit-rows` cap produces well over 1000 domains after aggregation, the
same way the deterministic v1 pipeline always has. It only fires when
someone (in this case, me, deliberately) caps the extraction for a cheap
test -- exactly the scenario it should catch.


**Guardrails, restated as a single list, because this is the part that
actually matters:** the agent cannot (1) load anything without a fresh
`validate.py` PASS, (2) exceed the retry-count or days-past-expected
ceilings in `config.yaml` regardless of its own reasoning, (3) skip the
dry-run cost check before any billed BigQuery execution, or (4) touch any
row-count/anomaly threshold -- all four remain exactly as deterministic as
they were in v1. The only things genuinely delegated to the model are
*when to retry* (within a bounded window) and *how to phrase a summary*
of already-computed, already-correct facts.

**LLM call site #2: `pipeline/summarize_change_event.py`.** Takes one
event from `diff_snapshots.py`'s output and produces a single grounded
sentence -- e.g. "Zoho CRM -> Salesforce, commonly associated with CRM
migrations." It does not score, prioritize, or invent anything about the
company beyond what the technology names themselves imply; the prompt
(`prompts/change_event_summary_v1.txt`) explicitly forbids that. If this
call is disabled or fails, the change-events feed itself is unaffected --
it's a descriptive layer on top of already-correct data, not a dependency
for correctness, same posture as everything else agentic in this
submission.

**Provider choice.** Groq's free tier (OpenAI-compatible endpoint) is what
both call sites run against, not the Claude API, to keep this within a
free-tier build. Both were prototyped against Claude first specifically so
switching providers stayed a one-line config change (`agentic.llm_provider`
in `config.yaml`) rather than a rewrite -- `pipeline/llm_client.py` is the
only file that talks to either API, and both request-shaping functions
exist side by side there. Every call, on either provider, writes one line
to `data/llm_traces/traces.jsonl` with a `trace_id`, `model`,
`prompt_version`, latency, token counts, and an estimated cost -- this is
the tracing/prompt-versioning piece of the brief that v1 had no LLM call
to attach to. Prompts themselves live as versioned files under `prompts/`
(`orchestrator_diagnosis_v1.txt`, `change_event_summary_v1.txt`), never as
inline strings in the calling code, so the version number in a trace line
and the version number in the filename can't drift apart.

**Firmable product alignment, and what's deliberately scoped out.**
Firmable's existing Signals / Signal Agent Actions surface (per
help.firmable.com's public docs) already does lead scoring, CRM task
creation, and webhook fan-out on top of buying signals. It would have been
easy to over-scope this submission into simulating that -- an ICP-scoring
layer, a fake CRM push, a "migration intent" score derived from
`summarize_change_event()`'s output. I deliberately did not build any of
that. There's no ground truth to validate scoring logic against inside
this project, and inventing plausible-looking business logic against a
CRM surface I don't actually have access to would undermine the
correctness-first argument the rest of this document makes -- it's the
same restraint as the zero-LLM-in-v1 decision in section 5, applied one
layer further out. What I did keep is the framing: `summarize_change_event()`'s
output is shaped so that it *could* plug into Firmable's existing Signals
pipeline as an upstream signal-generation step, and that's as far as this
submission goes on that front -- design rationale, not built integration.

**GitHub Actions setup this workflow depends on (one-time, manual):**
1. Repo Settings -> Environments -> New environment -> name it
   `production-approval` -> add yourself as a required reviewer. This is
   the actual mechanism behind Layer 3 -- GitHub will not start the
   `approve` job in `.github/workflows/autopilot.yml` until that reviewer
   approves the run in the Actions UI.
2. Nothing else -- the cron schedule and the `workflow_dispatch` manual
   trigger are both already defined in the workflow file itself.

**Authentication: Workload Identity Federation, not a static key.** The
free-tier project this was built against enforces GCP's
`iam.disableServiceAccountKeyCreation` organization policy (part of
Google's "Secure by Default" baseline) -- exporting a service-account key
JSON is blocked outright. Rather than working around that, the workflow
authenticates via Workload Identity Federation instead: a Workload Identity
Pool + Provider trusts GitHub's own OIDC token issuer, scoped by an
attribute condition to this exact repository, and grants it permission to
impersonate a least-privilege service account (`bigquery.dataEditor`,
`bigquery.jobUser`, `storage.objectAdmin` -- nothing broader). The result
is arguably a stronger guardrail than the key-based approach it replaced:
no long-lived credential is ever stored in GitHub at all, the token minted
per run is short-lived, and it cannot be used by any repository other than
this one, even if it were somehow exfiltrated from a run log.