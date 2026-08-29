import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.diff_snapshots import diff_snapshots

PREV_DATE = "2099-01-01"
CUR_DATE = "2099-02-01"


def _write_snapshot(crawl_date: str, rows: list[dict]):
    snap_dir = REPO_ROOT / CFG["paths"]["snapshots_dir"]
    snap_dir.mkdir(parents=True, exist_ok=True)
    (snap_dir / f"snapshot_{crawl_date}.json").write_text(json.dumps(rows))


def setup_module(module):
    _write_snapshot(PREV_DATE, [
        {"domain": "abc.com", "url": "https://abc.com/", "rank": 100, "crawl_date": PREV_DATE,
         "tech": [{"technology": "WordPress", "categories": ["CMS"], "version": None},
                  {"technology": "Google Analytics", "categories": ["Analytics"], "version": "4"}]},
        {"domain": "stable.com", "url": "https://stable.com/", "rank": 200, "crawl_date": PREV_DATE,
         "tech": [{"technology": "Cloudflare", "categories": ["CDN"], "version": None}]},
        {"domain": "gone.com", "url": "https://gone.com/", "rank": 300, "crawl_date": PREV_DATE,
         "tech": [{"technology": "Magento", "categories": ["Ecommerce"], "version": None}]},
    ])
    _write_snapshot(CUR_DATE, [
        {"domain": "abc.com", "url": "https://abc.com/", "rank": 100, "crawl_date": CUR_DATE,
         "tech": [{"technology": "Shopify", "categories": ["Ecommerce"], "version": None},
                  {"technology": "Google Analytics", "categories": ["Analytics"], "version": "4"}]},
        {"domain": "stable.com", "url": "https://stable.com/", "rank": 200, "crawl_date": CUR_DATE,
         "tech": [{"technology": "Cloudflare", "categories": ["CDN"], "version": None}]},
        {"domain": "new.com", "url": "https://new.com/", "rank": 400, "crawl_date": CUR_DATE,
         "tech": [{"technology": "HubSpot", "categories": ["CRM"], "version": None}]},
    ])


def _events():
    result = diff_snapshots(CUR_DATE, PREV_DATE)
    return json.loads(Path(result["output_path"]).read_text())


def test_added_and_dropped_detected_for_changed_domain():
    events = _events()
    e = next(ev for ev in events if ev["domain"] == "abc.com")
    assert e["event_type"] == "tech_change"
    assert e["added"] == ["Shopify"]
    assert e["dropped"] == ["WordPress"]


def test_unchanged_domain_produces_no_event():
    events = _events()
    assert not any(ev["domain"] == "stable.com" for ev in events)


def test_new_domain_event():
    events = _events()
    e = next(ev for ev in events if ev["domain"] == "new.com")
    assert e["event_type"] == "new_domain"
    assert e["added"] == ["HubSpot"]
    assert e["dropped"] == []


def test_dropped_domain_event():
    events = _events()
    e = next(ev for ev in events if ev["domain"] == "gone.com")
    assert e["event_type"] == "dropped_domain"
    assert e["dropped"] == ["Magento"]
    assert e["added"] == []
