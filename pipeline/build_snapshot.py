"""build_snapshot.py -- turns origin-level extract rows into the final
domain-level snapshot: one row per registrable domain.

Aggregation rule (the answer to the grain trap, documented in full in
docs/design_doc.md):

    For each registrable domain, collect every origin that normalizes to it,
    UNION the distinct technologies detected across all of those origins,
    and take the lowest (best) non-null rank as the domain's rank.

Why union rather than "pick the root origin only" or "pick the origin with
better rank": a company's marketing site (example.com, WordPress) and its
storefront (shop.example.com, Shopify) are both real technology signal about
the same company. A sales rep asking "does this company use Shopify" wants
"yes" here, not "no, because I only looked at the bare domain." Throwing
away the subdomain's signal would silently make the product worse at its
one job. The trade-off I accept: a domain's tech list can include
technologies that live on a subdomain a prospect might consider a separate
product area. I think that's the right trade-off for a lead-gen /
buyer-intent use case; a different downstream consumer could re-derive a
root-origin-only view from the same raw extract if they needed it.

Origins that fail normalization (normalize_domain.normalize_origin returns
is_valid=False) are written to data/rejected/ rather than silently dropped,
so validate.py's row-count gate can see how many rows were excluded and
why, and a human can audit them.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.normalize_domain import normalize_origin


def _tech_key(technology: str, categories: list[str]) -> tuple:
    # Dedup key: same technology name + same category set. Two different
    # origins reporting "React" with the same categories collapse into one
    # entry; if HTTP Archive ever reports the same technology name under
    # different categories (rare, but seen with Wappalyzer edge cases) I
    # keep both, since that's genuinely different classification info.
    return (technology, tuple(sorted(categories or [])))


def build_snapshot(crawl_date: str) -> dict:
    raw_dir = REPO_ROOT / CFG["paths"]["raw_dir"]
    rejected_dir = REPO_ROOT / CFG["paths"]["rejected_dir"]
    rejected_dir.mkdir(parents=True, exist_ok=True)

    extract_path = raw_dir / f"extract_{crawl_date}.json"
    universe_path = raw_dir / f"universe_{crawl_date}.json"
    extract_rows = json.loads(extract_path.read_text())
    universe_rows = json.loads(universe_path.read_text())

    # domain -> accumulator
    domains: dict[str, dict] = {}
    rejected: list[dict] = []

    def _get_or_init(domain: str):
        if domain not in domains:
            domains[domain] = {
                "domain": domain,
                "url": None,          # set to the best (lowest-rank) origin's URL
                "rank": None,
                "tech_by_key": {},    # tech_key -> {technology, categories, version}
                "origin_count": 0,
                "crawl_date": crawl_date,
            }
        return domains[domain]

    # Pass 1: universe rows establish every domain that exists this month,
    # even ones with zero detected technologies -- see
    # sql/production/extract_domains_universe.sql for why this matters.
    origin_to_domain: dict[str, str] = {}
    for row in universe_rows:
        origin = row["root_page"]
        norm = normalize_origin(origin)
        if not norm.is_valid:
            rejected.append({"stage": "normalize_domain", "origin": origin, "reason": norm.reason})
            continue
        origin_to_domain[origin] = norm.registrable_domain
        acc = _get_or_init(norm.registrable_domain)
        acc["origin_count"] += 1
        rank = row.get("rank")
        if rank is not None and (acc["rank"] is None or rank < acc["rank"]):
            acc["rank"] = rank
            acc["url"] = origin

    # Pass 2: extract rows add the actual technology detections, unioned
    # per domain across every origin that rolled up into it.
    for row in extract_rows:
        origin = row["root_page"]
        domain = origin_to_domain.get(origin)
        if domain is None:
            # Origin appeared in the tech-detail extract but not in the
            # universe extract (or failed normalization there). Re-normalize
            # defensively so a partial re-run/mismatched fixture doesn't
            # silently drop real signal.
            norm = normalize_origin(origin)
            if not norm.is_valid:
                rejected.append({"stage": "normalize_domain", "origin": origin, "reason": norm.reason})
                continue
            domain = norm.registrable_domain
        acc = _get_or_init(domain)
        key = _tech_key(row["technology"], row.get("categories") or [])
        acc["tech_by_key"][key] = {
            "technology": row["technology"],
            "categories": row.get("categories") or [],
            "version": (row.get("version_info") or [None])[0],
        }

    # Finalize
    snapshot_rows = []
    for domain, acc in domains.items():
        snapshot_rows.append({
            "domain": domain,
            "url": acc["url"] or f"https://{domain}/",
            "tech": sorted(acc["tech_by_key"].values(), key=lambda t: t["technology"]),
            "rank": acc["rank"],
            "crawl_date": crawl_date,
            "origin_count": acc["origin_count"],
        })

    snapshot_rows.sort(key=lambda r: (r["rank"] is None, r["rank"] or 0))

    snapshots_dir = REPO_ROOT / CFG["paths"]["snapshots_dir"]
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    out_path = snapshots_dir / f"snapshot_{crawl_date}.json"
    out_path.write_text(json.dumps(snapshot_rows, indent=2))

    if rejected:
        rej_path = rejected_dir / f"rejected_{crawl_date}.json"
        rej_path.write_text(json.dumps(rejected, indent=2))

    multi_origin_domains = sum(1 for r in snapshot_rows if r["origin_count"] > 1)

    return {
        "crawl_date": crawl_date,
        "domain_count": len(snapshot_rows),
        "rejected_count": len(rejected),
        "multi_origin_domain_count": multi_origin_domains,
        "snapshot_path": str(out_path),
    }


def main():
    parser = argparse.ArgumentParser(description="Aggregate an extract into a domain-level snapshot.")
    parser.add_argument("--crawl-date", required=True)
    args = parser.parse_args()
    result = build_snapshot(args.crawl_date)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
