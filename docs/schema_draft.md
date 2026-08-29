# Schema draft: outputs of this pipeline

Two outputs, matching the take-home brief's deliverables exactly. Both are
written as JSON here for local dev (`data/snapshots/`, `data/change_events/`);
`docs/design_doc.md` covers how these land as actual warehouse tables.

## 1. Monthly snapshot (`domain -> tech stack`)

File: `data/snapshots/snapshot_<crawl_date>.json` -- one JSON array, one
element per registrable domain.

```json
{
  "domain": "atlasstudio113.com",
  "url": "https://www.atlasstudio113.com/",
  "tech": [
    { "technology": "Google Analytics", "categories": ["Analytics"], "version": "4" },
    { "technology": "PayPal", "categories": ["Payments"], "version": null },
    { "technology": "Shopify", "categories": ["Ecommerce"], "version": null },
    { "technology": "Vue.js", "categories": ["JavaScript frameworks"], "version": "3" },
    { "technology": "Webflow", "categories": ["CMS", "Website builders"], "version": null }
  ],
  "rank": 9705,
  "crawl_date": "2026-08-01",
  "origin_count": 1
}
```

| Field | Type | Notes |
|---|---|---|
| `domain` | `STRING` | The **join key**. Registrable domain (eTLD+1), public-suffix-aware, lowercased, no scheme/path/port/`www`. See design_doc.md "join key contract". |
| `url` | `STRING` | The origin URL of the domain's best-ranked constituent origin (lowest CrUX rank). Informational -- not a join key. |
| `tech` | `ARRAY<RECORD>` | Union of all technologies detected across every origin that normalized to this domain. Deduplicated on `(technology, categories)`. |
| `tech[].technology` | `STRING` | Wappalyzer technology name, verbatim from HTTP Archive. |
| `tech[].categories` | `ARRAY<STRING>` | Wappalyzer category names, verbatim. A technology can have >1 category. |
| `tech[].version` | `STRING \| null` | First entry of Wappalyzer's `info` array for that detection, if present. Not guaranteed reliable -- see findings.md on garbage version strings. |
| `rank` | `INTEGER \| null` | CrUX popularity rank of the domain's best origin. `null` if no origin had a rank (unranked in CrUX that month). Lower is more popular. |
| `crawl_date` | `DATE` (`YYYY-MM-DD`) | Always the 1st of the month; the label of the source partition, not the actual crawl date (see findings.md). |
| `origin_count` | `INTEGER` | How many distinct origins rolled up into this domain. `1` for the common case; `>1` flags a grain-collision domain worth spot-checking. Not required by the brief, kept for auditability. |

**Grain:** exactly one row per registrable domain per `crawl_date`. Enforced
by `validate.py`'s `no_duplicate_domains` check.

## 2. Change events (`added`/`dropped` per domain per month)

File: `data/change_events/change_events_<prev>_to_<curr>.json` -- one JSON
array, one element per domain with a detected change, or a lifecycle event.

```json
{
  "domain": "acmegoods41.co.uk",
  "event_type": "tech_change",
  "crawl_date": "2026-08-01",
  "previous_crawl_date": "2026-07-01",
  "added": [],
  "dropped": ["React"]
}
```

| Field | Type | Notes |
|---|---|---|
| `domain` | `STRING` | Same join key as the snapshot. |
| `event_type` | `STRING` | One of `tech_change`, `new_domain`, `dropped_domain`. See below. |
| `crawl_date` | `DATE` | The newer of the two months being compared. |
| `previous_crawl_date` | `DATE` | The older of the two months being compared. |
| `added` | `ARRAY<STRING>` | Technology names present in `crawl_date` but not `previous_crawl_date`. |
| `dropped` | `ARRAY<STRING>` | Technology names present in `previous_crawl_date` but not `crawl_date`. |

**`event_type` semantics** (deliberately kept separate rather than
collapsed into one undifferentiated "diff" -- see rationale in
`pipeline/diff_snapshots.py`):
- `tech_change` -- domain existed in both months, some technology was
  added and/or dropped. The core buyer-intent signal.
- `new_domain` -- domain appears in `crawl_date` but not
  `previous_crawl_date` (newly crawled / newly ranked). All of its current
  technologies are listed under `added`.
- `dropped_domain` -- domain appeared in `previous_crawl_date` but not
  `crawl_date` (fell out of the crawl -- site down, deranked, etc). All of
  its prior technologies are listed under `dropped`.

Domains with **no change** between the two months produce **no row** in
this output -- this is a change-events feed, not a full month-over-month
join. A downstream consumer wanting "show me everything, changed or not"
should read the two snapshots directly, not this file.

## 3. Rejected rows (operational, not a deliverable, but worth documenting)

File: `data/rejected/rejected_<crawl_date>.json` -- origins that failed
`normalize_domain.py` (e.g. malformed URLs with no parseable
domain+suffix). Written so `validate.py` and a human reviewer can see how
much was excluded and why, rather than these rows silently vanishing from
the domain count. Empty in every real run so far (HTTP Archive's `root_page`
values are well-formed by construction), but the path exists for when it
isn't.
