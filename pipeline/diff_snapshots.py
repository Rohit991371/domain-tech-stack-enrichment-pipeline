"""diff_snapshots.py -- the change-events feed.

For each domain present in both the current and previous snapshot:
    added   = current_technologies - previous_technologies
    dropped = previous_technologies - current_technologies

Domains that only exist in the current snapshot are reported separately as
"new_domain" events (every one of their current technologies counts as
newly observed, but that's a different signal than "an existing customer's
stack changed" -- a sales team cares about these differently, so I don't
collapse them into ordinary added/dropped events). Domains that only exist
in the previous snapshot are "dropped_domain" events -- worth surfacing
(site went dark / dropped out of the crawl's ranked set) but, again, a
distinct signal from a same-domain tech swap.

A technology is identified by (technology name, sorted categories) --
the same key used in build_snapshot.py -- so a reclassification of the same
tech name into different categories shows up explicitly rather than being
silently treated as "no change."
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT


def _tech_set(row: dict) -> set[tuple]:
    return {(t["technology"], tuple(sorted(t.get("categories") or []))) for t in row.get("tech", [])}


def _load_snapshot(crawl_date: str) -> dict[str, dict]:
    path = REPO_ROOT / CFG["paths"]["snapshots_dir"] / f"snapshot_{crawl_date}.json"
    rows = json.loads(path.read_text())
    return {r["domain"]: r for r in rows}


def diff_snapshots(current_date: str, previous_date: str) -> dict:
    current = _load_snapshot(current_date)
    previous = _load_snapshot(previous_date)

    events = []

    common_domains = set(current) & set(previous)
    for domain in common_domains:
        cur_techs = _tech_set(current[domain])
        prev_techs = _tech_set(previous[domain])
        added = cur_techs - prev_techs
        dropped = prev_techs - cur_techs
        if not added and not dropped:
            continue
        events.append({
            "domain": domain,
            "event_type": "tech_change",
            "crawl_date": current_date,
            "previous_crawl_date": previous_date,
            "added": sorted(t for t, _cats in added),
            "dropped": sorted(t for t, _cats in dropped),
        })

    for domain in set(current) - set(previous):
        events.append({
            "domain": domain,
            "event_type": "new_domain",
            "crawl_date": current_date,
            "previous_crawl_date": previous_date,
            "added": sorted(t["technology"] for t in current[domain].get("tech", [])),
            "dropped": [],
        })

    for domain in set(previous) - set(current):
        events.append({
            "domain": domain,
            "event_type": "dropped_domain",
            "crawl_date": current_date,
            "previous_crawl_date": previous_date,
            "added": [],
            "dropped": sorted(t["technology"] for t in previous[domain].get("tech", [])),
        })

    events.sort(key=lambda e: (e["event_type"] != "tech_change", e["domain"]))

    out_dir = REPO_ROOT / CFG["paths"]["change_events_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"change_events_{previous_date}_to_{current_date}.json"
    out_path.write_text(json.dumps(events, indent=2))

    summary = {
        "previous_crawl_date": previous_date,
        "current_crawl_date": current_date,
        "tech_change_events": sum(1 for e in events if e["event_type"] == "tech_change"),
        "new_domain_events": sum(1 for e in events if e["event_type"] == "new_domain"),
        "dropped_domain_events": sum(1 for e in events if e["event_type"] == "dropped_domain"),
        "total_events": len(events),
        "output_path": str(out_path),
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description="Diff two monthly snapshots into a change-events feed.")
    parser.add_argument("--current-crawl-date", required=True)
    parser.add_argument("--previous-crawl-date", required=True)
    args = parser.parse_args()
    result = diff_snapshots(args.current_crawl_date, args.previous_crawl_date)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
