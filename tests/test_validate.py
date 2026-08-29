import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.validate import validate_snapshot

GOOD_DATE = "2099-03-01"
PREV_DATE = "2099-02-15"  # reuse a distinct fake previous date per test to avoid cross-test bleed
TINY_DATE = "2099-04-01"
SPIKE_DATE = "2099-05-01"
DROP_DATE = "2099-06-01"


def _write_snapshot(crawl_date: str, rows: list[dict]):
    snap_dir = REPO_ROOT / CFG["paths"]["snapshots_dir"]
    snap_dir.mkdir(parents=True, exist_ok=True)
    (snap_dir / f"snapshot_{crawl_date}.json").write_text(json.dumps(rows))


def _n_domains(n: int, crawl_date: str, with_tech: bool = True):
    return [
        {"domain": f"d{i}.com", "url": f"https://d{i}.com/", "rank": i, "crawl_date": crawl_date,
         "tech": [{"technology": "WordPress", "categories": ["CMS"], "version": None}] if with_tech else []}
        for i in range(n)
    ]


def setup_module(module):
    _write_snapshot(GOOD_DATE, _n_domains(300, GOOD_DATE))
    _write_snapshot(PREV_DATE, _n_domains(300, PREV_DATE))
    _write_snapshot(TINY_DATE, _n_domains(5, TINY_DATE))  # below min_domain_count_mock


def test_healthy_snapshot_passes():
    result = validate_snapshot(GOOD_DATE, mock=True)
    assert result.passed


def test_missing_snapshot_fails():
    result = validate_snapshot("2099-12-01", mock=True)
    assert not result.passed
    assert result.checks[0]["check"] == "snapshot_exists"
    assert not result.checks[0]["passed"]


def test_too_few_domains_fails():
    result = validate_snapshot(TINY_DATE, mock=True)
    assert not result.passed


def test_row_count_spike_vs_previous_fails():
    # 300 -> 2000 domains is a >300% spike, over max_row_count_spike_pct
    _write_snapshot(SPIKE_DATE, _n_domains(2000, SPIKE_DATE))
    result = validate_snapshot(SPIKE_DATE, PREV_DATE, mock=True)
    drift_check = next(c for c in result.checks if c["check"] == "row_count_drift")
    assert not drift_check["passed"]
    assert not result.passed


def test_row_count_drop_vs_previous_fails():
    # 300 -> 50 domains is an ~83% drop, over max_row_count_drop_pct (30%)
    _write_snapshot(DROP_DATE, _n_domains(50, DROP_DATE))
    result = validate_snapshot(DROP_DATE, PREV_DATE, mock=True)
    drift_check = next(c for c in result.checks if c["check"] == "row_count_drift")
    assert not drift_check["passed"]
    assert not result.passed


def test_zero_tech_coverage_fails():
    empty_date = "2099-07-01"
    _write_snapshot(empty_date, _n_domains(300, empty_date, with_tech=False))
    result = validate_snapshot(empty_date, mock=True)
    coverage_check = next(c for c in result.checks if c["check"] == "tech_coverage_ratio")
    assert not coverage_check["passed"]
    assert not result.passed
