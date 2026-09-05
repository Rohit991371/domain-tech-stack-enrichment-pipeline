"""Tests for the agentic autopilot layer. These deliberately do NOT need a
real LLM key -- every test passes use_llm=False (the deterministic
fallback path in llm_client.py) so the circuit breaker's own logic, which
is what actually matters for safety, is what's under test, not any given
model's behavior.
"""
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.orchestrator_agent import (
    STATE_DIR,
    _decide_retry_or_escalate,
    _load_state,
    _save_state,
    proceed_to_load,
)

FAKE_CRAWL_DATE = "2099-08-01"


def _reset_state():
    path = STATE_DIR / f"{FAKE_CRAWL_DATE}.json"
    if path.exists():
        path.unlink()


def setup_function(_fn):
    _reset_state()


def teardown_module(_mod):
    _reset_state()


def test_first_failure_recommends_retry_not_escalate():
    state = _load_state(FAKE_CRAWL_DATE)
    arrival = {"arrived": False, "row_count": 3, "reason": "row_count_3_below_min"}
    decision = _decide_retry_or_escalate(FAKE_CRAWL_DATE, arrival, state, use_llm=False)
    assert decision["recommendation"] == "retry_later"
    assert decision["retry_minutes"] is not None


def test_circuit_breaker_forces_escalate_after_max_retries():
    ag = CFG["agentic"]
    state = _load_state(FAKE_CRAWL_DATE)
    state["retry_count"] = ag["max_retries"] - 1  # one more attempt hits the ceiling
    arrival = {"arrived": False, "row_count": 3, "reason": "row_count_3_below_min"}
    decision = _decide_retry_or_escalate(FAKE_CRAWL_DATE, arrival, state, use_llm=False)
    assert decision["recommendation"] == "escalate"
    assert "circuit_breaker" in decision["likely_cause"] or "circuit breaker" in decision["reasoning"].lower()


def test_circuit_breaker_is_enforced_even_if_llm_disagrees(monkeypatch):
    """Simulates a model that ignores the instruction and recommends
    retry_later despite the breaker limit -- the code must override it."""
    import pipeline.orchestrator_agent as oa

    def _bad_call_llm(*_args, **_kwargs):
        return {
            "text": json.dumps({
                "likely_cause": "model_thinks_its_fine",
                "recommendation": "retry_later",
                "retry_minutes": 60,
                "reasoning": "model ignored the breaker instruction",
            }),
            "trace_id": "fake-trace",
            "model": "fake-model",
        }

    monkeypatch.setattr(oa, "call_llm", _bad_call_llm)

    ag = CFG["agentic"]
    state = _load_state(FAKE_CRAWL_DATE)
    state["retry_count"] = ag["max_retries"]  # already at/over the ceiling
    arrival = {"arrived": False, "row_count": 3, "reason": "row_count_3_below_min"}
    decision = oa._decide_retry_or_escalate(FAKE_CRAWL_DATE, arrival, state, use_llm=True)
    assert decision["recommendation"] == "escalate", (
        "circuit breaker must win even when the LLM output disagrees"
    )


def test_state_persists_retry_count_across_calls():
    state = _load_state(FAKE_CRAWL_DATE)
    assert state["retry_count"] == 0
    state["retry_count"] = 2
    _save_state(state)
    reloaded = _load_state(FAKE_CRAWL_DATE)
    assert reloaded["retry_count"] == 2


def test_proceed_to_load_refuses_when_no_snapshot_exists():
    """No snapshot file at all -> validate_snapshot() fails the very first
    check ("snapshot_exists") -> proceed_to_load() must refuse (exit code
    2), regardless of anything else."""
    never_used_crawl_date = "2099-12-01"
    code = proceed_to_load(never_used_crawl_date, None, mock=True, land=False, use_llm=False)
    assert code == 2


def test_proceed_to_load_refuses_when_snapshot_too_small(tmp_path):
    """A snapshot exists but is far below min_domain_count_mock -- fresh
    validate_snapshot() call inside proceed_to_load() must still refuse,
    proving it doesn't just trust that it was called at all."""
    crawl_date = "2099-11-01"
    snap_dir = REPO_ROOT / CFG["paths"]["snapshots_dir"]
    snap_dir.mkdir(parents=True, exist_ok=True)
    tiny_rows = [
        {"domain": "d0.com", "url": "https://d0.com/", "rank": 1, "crawl_date": crawl_date,
         "tech": [{"technology": "WordPress", "categories": ["CMS"], "version": None}]}
    ]
    path = snap_dir / f"snapshot_{crawl_date}.json"
    path.write_text(json.dumps(tiny_rows))
    try:
        code = proceed_to_load(crawl_date, None, mock=True, land=False, use_llm=False)
        assert code == 2
    finally:
        path.unlink(missing_ok=True)
