"""mock_source.py -- generates fixtures shaped like httparchive.crawl.pages
results, for developing and testing this pipeline in an environment with no
BigQuery access (this sandbox has no network path to googleapis.com).

This is explicitly a development aid, not a substitute for testing against
the real `httparchive.sample_data.pages_10k` table. README.md and
docs/design_doc.md both call this out. Run this once to populate
data/fixtures/, then everything downstream (extract.py --mock,
arrival_check.py --mock, and the whole run_pipeline.py --mock path) is
exercised against realistic, schema-accurate, deterministic data --
including deliberately-injected grain collisions (multiple origins per
domain with disagreeing tech) and a deliberately-injected "not yet landed"
month, since those are exactly the traps the real assignment is testing for.

Two months are generated:
    2026-07-01  ("month 1", 520 origins, fully landed)
    2026-08-01  ("month 2", derived from month 1 with deliberate tech churn
                 on ~15% of domains, fully landed) -- this is the
                 documented simulation of "the second month" the take-home
                 brief explicitly allows.
A third calendar entry, 2026-09-01, is registered with a near-zero row count
to simulate a crawl that has *not* actually landed yet, so arrival_check.py
has something real to correctly refuse.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # allow `python pipeline/mock_source.py` from anywhere
from config import CFG, REPO_ROOT

random.seed(42)

TECH_POOL = [
    ("WordPress", ["CMS"], None),
    ("Shopify", ["Ecommerce"], None),
    ("Magento", ["Ecommerce"], None),
    ("BigCommerce", ["Ecommerce"], None),
    ("HubSpot", ["Marketing automation", "CRM"], None),
    ("Salesforce", ["CRM"], None),
    ("Google Analytics", ["Analytics"], "4"),
    ("Google Tag Manager", ["Tag managers"], None),
    ("Cloudflare", ["CDN", "Security"], None),
    ("Fastly", ["CDN"], None),
    ("React", ["JavaScript frameworks"], "18"),
    ("Vue.js", ["JavaScript frameworks"], "3"),
    ("Stripe", ["Payments"], None),
    ("PayPal", ["Payments"], None),
    ("Webflow", ["CMS", "Website builders"], None),
    ("Squarespace", ["CMS", "Website builders"], None),
    ("Zendesk", ["Customer support"], None),
    ("Intercom", ["Customer support", "Live chat"], None),
    ("Mailchimp", ["Marketing automation"], None),
    ("jQuery", ["JavaScript libraries"], "3.6"),
]

FIRST_WORDS = ["nova", "acme", "summit", "harbor", "cedar", "quartz", "delta",
               "north", "orbit", "vista", "maple", "forge", "keystone", "atlas",
               "bright", "clover", "granite", "ironclad", "lumen", "pioneer"]
SECOND_WORDS = ["works", "labs", "goods", "supply", "digital", "studio", "group",
                "systems", "market", "collective", "partners", "solutions"]
TLDS = ["com", "com", "com", "co", "io", "net", "co.uk"]


def _make_domain_name(i: int) -> str:
    a = FIRST_WORDS[i % len(FIRST_WORDS)]
    b = SECOND_WORDS[(i // len(FIRST_WORDS)) % len(SECOND_WORDS)]
    tld = TLDS[i % len(TLDS)]
    return f"{a}{b}{i}.{tld}"


def _random_tech_set(rng: random.Random, n_min=1, n_max=5):
    k = rng.randint(n_min, n_max)
    return rng.sample(TECH_POOL, k)


def generate_month(crawl_date: str, n_domains: int = 500, seed: int = 42):
    rng = random.Random(seed)
    origins = []  # list of dict: root_page, rank, technologies(list of tuples)

    for i in range(n_domains):
        domain = _make_domain_name(i)
        rank = rng.randint(50, 5_000_000)
        base_tech = _random_tech_set(rng)

        # ~12% of domains get a second origin (e.g. shop.<domain>) to
        # deliberately create grain collisions for build_snapshot.py to
        # resolve -- this is the "grain trap" fixture data.
        n_origins = 2 if rng.random() < 0.12 else 1
        for o in range(n_origins):
            host = f"https://{'shop.' if o == 1 else 'www.' if rng.random() < 0.5 else ''}{domain}/"
            # secondary origin sometimes has a different/overlapping tech set
            tech = base_tech if o == 0 else _random_tech_set(rng)
            origins.append({
                "root_page": host,
                "rank": rank if o == 0 else None,
                "technologies": tech,
            })

    # ~2% of domains: zero detected technologies (still a valid crawled origin)
    for i in range(int(n_domains * 0.02)):
        domain = _make_domain_name(1000 + i)
        origins.append({"root_page": f"https://{domain}/", "rank": rng.randint(50, 5_000_000), "technologies": []})

    return origins


def churn_month(month1_origins: list[dict], crawl_date: str, seed: int = 99, churn_rate: float = 0.15):
    """Derive 'month 2' from month 1 with deliberate technology churn on a
    subset of domains -- the documented simulated second month for the diff
    engine. Every change is tracked in a returned manifest so
    tests/test_diff_snapshots.py can assert the diff engine actually found
    exactly what was injected, not just "something."
    """
    rng = random.Random(seed)
    month2 = [dict(o, technologies=list(o["technologies"])) for o in month1_origins]
    manifest = []

    for o in month2:
        if rng.random() < churn_rate and o["technologies"]:
            action = rng.choice(["add", "drop", "swap"])
            if action == "add" and len(o["technologies"]) < len(TECH_POOL):
                candidates = [t for t in TECH_POOL if t not in o["technologies"]]
                new_tech = rng.choice(candidates)
                o["technologies"].append(new_tech)
                manifest.append({"root_page": o["root_page"], "action": "add", "technology": new_tech[0]})
            elif action == "drop" and len(o["technologies"]) > 1:
                dropped = rng.choice(o["technologies"])
                o["technologies"].remove(dropped)
                manifest.append({"root_page": o["root_page"], "action": "drop", "technology": dropped[0]})
            elif action == "swap" and o["technologies"]:
                dropped = rng.choice(o["technologies"])
                o["technologies"].remove(dropped)
                candidates = [t for t in TECH_POOL if t not in o["technologies"]]
                added = rng.choice(candidates)
                o["technologies"].append(added)
                manifest.append({"root_page": o["root_page"], "action": "drop", "technology": dropped[0]})
                manifest.append({"root_page": o["root_page"], "action": "add", "technology": added[0]})

    return month2, manifest


def _to_extract_rows(origins: list[dict], crawl_date: str) -> list[dict]:
    """Shape matching extract_snapshot.sql's UNNESTed output: one row per
    (origin, technology) pair."""
    rows = []
    for o in origins:
        for tech, categories, version in o["technologies"]:
            rows.append({
                "root_page": o["root_page"],
                "rank": o["rank"],
                "crawl_date": crawl_date,
                "technology": tech,
                "categories": categories,
                "version_info": [version] if version else [],
            })
    return rows


def _to_universe_rows(origins: list[dict], crawl_date: str) -> list[dict]:
    """Shape matching extract_domains_universe.sql's output: one row per
    origin regardless of whether it has any technologies."""
    return [
        {
            "root_page": o["root_page"],
            "rank": o["rank"],
            "crawl_date": crawl_date,
            "tech_count": len(o["technologies"]),
        }
        for o in origins
    ]


def write_fixtures():
    fixtures_dir = REPO_ROOT / CFG["paths"]["fixtures_dir"]
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    month1_date = "2026-07-01"
    month2_date = "2026-08-01"
    not_landed_date = "2026-09-01"

    month1 = generate_month(month1_date, n_domains=500, seed=42)
    month2, manifest = churn_month(month1, month2_date, seed=99, churn_rate=0.15)

    for crawl_date, origins in [(month1_date, month1), (month2_date, month2)]:
        extract_rows = _to_extract_rows(origins, crawl_date)
        universe_rows = _to_universe_rows(origins, crawl_date)
        (fixtures_dir / f"mock_extract_{crawl_date}.json").write_text(json.dumps(extract_rows, indent=2))
        (fixtures_dir / f"mock_universe_{crawl_date}.json").write_text(json.dumps(universe_rows, indent=2))

    (fixtures_dir / "mock_churn_manifest.json").write_text(json.dumps(manifest, indent=2))

    calendar = {
        month1_date: len(month1),
        month2_date: len(month2),
        not_landed_date: 3,  # deliberately below guardrails.min_row_count -> arrival_check must refuse
    }
    (fixtures_dir / "mock_crawl_calendar.json").write_text(json.dumps(calendar, indent=2))

    print(f"Wrote fixtures for {month1_date} ({len(month1)} origins), "
          f"{month2_date} ({len(month2)} origins, {len(manifest)} tech changes), "
          f"and a not-landed month at {not_landed_date}.")


if __name__ == "__main__":
    write_fixtures()
