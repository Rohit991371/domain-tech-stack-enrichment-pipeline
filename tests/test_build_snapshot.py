import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.build_snapshot import build_snapshot


CRAWL_DATE = "2099-01-01"  # a date that will never collide with real fixtures


def _write_fixture(name: str, rows: list[dict]):
    fixtures_dir = REPO_ROOT / CFG["paths"]["raw_dir"]
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    (fixtures_dir / name).write_text(json.dumps(rows))


def setup_module(module):
    # Two origins of the same registrable domain, disagreeing on tech --
    # the exact scenario the assignment calls out as "the grain trap."
    universe_rows = [
        {"root_page": "https://www.example.com/", "rank": 500, "crawl_date": CRAWL_DATE, "tech_count": 1},
        {"root_page": "https://shop.example.com/", "rank": None, "crawl_date": CRAWL_DATE, "tech_count": 1},
        {"root_page": "https://solo.io/", "rank": 999, "crawl_date": CRAWL_DATE, "tech_count": 0},
    ]
    extract_rows = [
        {"root_page": "https://www.example.com/", "rank": 500, "crawl_date": CRAWL_DATE,
         "technology": "WordPress", "categories": ["CMS"], "version_info": []},
        {"root_page": "https://shop.example.com/", "rank": None, "crawl_date": CRAWL_DATE,
         "technology": "Shopify", "categories": ["Ecommerce"], "version_info": []},
    ]
    _write_fixture(f"extract_{CRAWL_DATE}.json", extract_rows)
    _write_fixture(f"universe_{CRAWL_DATE}.json", universe_rows)


def test_multi_origin_domain_unions_technologies():
    result = build_snapshot(CRAWL_DATE)
    snapshot = json.loads(Path(result["snapshot_path"]).read_text())
    by_domain = {r["domain"]: r for r in snapshot}

    assert "example.com" in by_domain
    tech_names = {t["technology"] for t in by_domain["example.com"]["tech"]}
    assert tech_names == {"WordPress", "Shopify"}, "union rule should keep both origins' tech"
    assert by_domain["example.com"]["origin_count"] == 2


def test_zero_tech_domain_still_present():
    result = build_snapshot(CRAWL_DATE)
    snapshot = json.loads(Path(result["snapshot_path"]).read_text())
    by_domain = {r["domain"]: r for r in snapshot}
    assert "solo.io" in by_domain
    assert by_domain["solo.io"]["tech"] == []


def test_best_rank_wins_for_url_field():
    result = build_snapshot(CRAWL_DATE)
    snapshot = json.loads(Path(result["snapshot_path"]).read_text())
    by_domain = {r["domain"]: r for r in snapshot}
    assert by_domain["example.com"]["rank"] == 500
    assert by_domain["example.com"]["url"] == "https://www.example.com/"
