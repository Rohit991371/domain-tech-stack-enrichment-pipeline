"""orchestrator_agent.py -- the autopilot layer (v2).

WHAT THIS FILE IS NOT: it is not a replacement for run_pipeline.py's
deterministic stage sequence (arrival -> extract -> build -> validate ->
load -> diff). Every one of those stages is still called exactly as before,
in the same order, with the same hard guardrails. This file adds exactly
two things on top:

  1. Bounded agentic judgment at the ONE place a judgment call actually
     helps: deciding whether an arrival-check failure is "try again soon"
     or "stop and page a human," using an LLM to reason over the specific
     failure history -- but capped by a deterministic circuit breaker
     (max_retries / max_days_past_expected from config.yaml) that the
     agent cannot override. See `_decide_retry_or_escalate()`.
  2. A human-in-the-loop approval checkpoint between "validation passed"
     and "production write happened." `run_scheduled_check()` NEVER calls
     `proceed_to_load()` itself. `proceed_to_load()` is only ever invoked
     by a second, separate entrypoint (`approve` subcommand below), which
     in the GitHub Actions setup runs as a second job gated behind an
     environment that requires a human reviewer's click. Even then,
     `proceed_to_load()` re-runs validate_snapshot() itself before doing
     anything -- it does not trust any state file, LLM output, or the fact
     that it was invoked at all. That is the one line that must never move:

         proceed_to_load() is a no-op unless validate.py returns PASS,
         checked fresh, every single time it is called.

Two run modes, matching the two GitHub Actions jobs in
.github/workflows/autopilot.yml:

    # Job 1 -- scheduled (cron) or manual, unattended, safe to run any time.
    python pipeline/orchestrator_agent.py check --crawl-date 2026-08-01 \\
        --previous-crawl-date 2026-07-01 [--mock] [--no-llm]

    # Job 2 -- only runs after a human clicks "approve" in the GitHub
    # Actions UI (environment protection rule). Re-validates before doing
    # anything.
    python pipeline/orchestrator_agent.py approve --crawl-date 2026-08-01 \\
        --previous-crawl-date 2026-07-01 [--mock] [--land]

Exit codes for `check` (what the GitHub Actions workflow branches on):
    0  validation PASSED -- snapshot built and validated, sitting in
       pending_approval, waiting for a human to run `approve`
    1  not arrived yet -- agent scheduled a retry, nothing touched,
       normal/expected outcome, no alert needed
    2  circuit breaker tripped or agent recommended escalate -- ALERT,
       a human should look at this before the next scheduled run
    3  validation FAILED (source arrived but the built snapshot didn't
       pass quality gates) -- ALERT, previous snapshot untouched
    4  unexpected error in an earlier stage

Exit codes for `approve`:
    0  loaded (and landed, if --land)
    2  refused -- validate_snapshot() did not return PASS when re-checked;
       nothing was loaded, regardless of what the state file said
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.arrival_check import check_arrival
from pipeline.build_snapshot import build_snapshot
from pipeline.diff_snapshots import diff_snapshots
from pipeline.extract import extract
from pipeline.llm_client import call_llm
from pipeline.validate import validate_snapshot

STATE_DIR = REPO_ROOT / "data" / "orchestrator_state"
DIAGNOSIS_PROMPT = "orchestrator_diagnosis"
DIAGNOSIS_VERSION = "v1"


# ---------------------------------------------------------------------------
# State persistence -- one JSON file per crawl_date. This is what lets a
# scheduled cron run (which is a fresh process every time) know how many
# times it has already tried, without needing to sleep across days.
# ---------------------------------------------------------------------------

def _state_path(crawl_date: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{crawl_date}.json"


def _load_state(crawl_date: str) -> dict:
    path = _state_path(crawl_date)
    if path.exists():
        return json.loads(path.read_text())
    return {
        "crawl_date": crawl_date,
        "attempts": [],
        "retry_count": 0,
        "circuit_breaker_tripped": False,
        "escalated": False,
        "validation_passed": None,
        "pending_approval": False,
        "approval_summary": None,
        "loaded": False,
    }


def _save_state(state: dict):
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    _state_path(state["crawl_date"]).write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Tool set the agent's reasoning is scoped to. Every one of these is a thin,
# already-existing, already-tested pipeline function -- the agent does not
# get any capability that didn't already exist deterministically.
# ---------------------------------------------------------------------------

def run_arrival_check(crawl_date: str, mock: bool) -> dict:
    return asdict(check_arrival(crawl_date, mock=mock))


def run_extraction(crawl_date: str, mock: bool, source: str, limit_rows: int | None, dry_run: bool = False) -> dict:
    return extract(crawl_date, mock=mock, dry_run=dry_run, source=source, limit_rows=limit_rows)


def run_snapshot_build(label: str) -> dict:
    return build_snapshot(label)


def run_validation(label: str, previous: str | None, mock: bool):
    return validate_snapshot(label, previous, mock=mock)


def run_diff(label: str, previous: str) -> dict:
    return diff_snapshots(label, previous)


# ---------------------------------------------------------------------------
# The one bounded judgment call: retry vs. escalate on an arrival-check
# failure. The circuit breaker is computed and enforced HERE, in plain
# Python, before the LLM is even asked -- the LLM's job is to explain and
# pick a retry interval within the window the breaker allows, never to
# decide whether the breaker itself applies.
# ---------------------------------------------------------------------------

def _days_past_expected(crawl_date: str) -> int:
    expected_start_offset = CFG["source"]["min_days_after_month_start_before_checking"]
    month_start = date.fromisoformat(crawl_date)
    today = date.today()
    elapsed = (today - month_start).days
    return max(0, elapsed - expected_start_offset)


def _decide_retry_or_escalate(crawl_date: str, arrival: dict, state: dict, use_llm: bool) -> dict:
    ag = CFG.get("agentic", {})
    max_retries = ag.get("max_retries", 5)
    max_days_past_expected = ag.get("max_days_past_expected", 20)
    default_retry_minutes = ag.get("retry_base_minutes", 240)

    retry_count_after_this = state["retry_count"] + 1
    days_past = _days_past_expected(crawl_date)

    # --- Deterministic circuit breaker, enforced before any LLM call. ---
    breaker_tripped = retry_count_after_this >= max_retries or days_past >= max_days_past_expected
    if breaker_tripped:
        decision = {
            "likely_cause": "circuit_breaker_limit_reached",
            "recommendation": "escalate",
            "retry_minutes": None,
            "reasoning": (
                f"Deterministic circuit breaker: retry_count_after_this="
                f"{retry_count_after_this} (max {max_retries}), days_past_expected="
                f"{days_past} (max {max_days_past_expected}). Escalation is forced "
                f"regardless of any LLM output."
            ),
            "trace_id": None,
            "model": "circuit-breaker-deterministic",
        }
        return decision

    # --- Under the breaker's limits: let the agent reason about *when*,
    # not *whether*, using only the facts below. ---
    context = {
        "crawl_date": crawl_date,
        "row_count": arrival.get("row_count"),
        "reason": arrival.get("reason"),
        "attempt_number": retry_count_after_this,
        "retry_count_after_this": retry_count_after_this,
        "max_retries": max_retries,
        "days_past_expected": days_past,
        "max_days_past_expected": max_days_past_expected,
        "known_release_pattern": (
            "HTTP Archive partitions are dated the 1st of the month but the crawl "
            "typically doesn't finish until 1-2 weeks after the 2nd Tuesday."
        ),
    }
    result = call_llm(DIAGNOSIS_PROMPT, DIAGNOSIS_VERSION, context, use_llm=use_llm)
    try:
        decision = json.loads(result["text"])
    except json.JSONDecodeError:
        # Malformed model output is treated the same as "no LLM available":
        # fall back to a safe default, never crash the scheduled run over it.
        decision = {
            "likely_cause": "llm_output_unparseable",
            "recommendation": "retry_later",
            "retry_minutes": default_retry_minutes,
            "reasoning": f"Could not parse model output as JSON: {result['text'][:200]!r}",
        }
    decision["trace_id"] = result["trace_id"]
    decision["model"] = result["model"]

    # Second, redundant enforcement of the same breaker -- belt-and-braces
    # against a model that ignores the instruction and recommends
    # retry_later anyway. This is the actual non-negotiable guarantee, not
    # the prompt wording above.
    if decision.get("recommendation") != "escalate" and breaker_tripped:
        decision["recommendation"] = "escalate"
        decision["reasoning"] += " [overridden: circuit breaker limit reached]"

    return decision


# ---------------------------------------------------------------------------
# Job 1: the scheduled/manual "check" entrypoint.
# ---------------------------------------------------------------------------

def run_scheduled_check(crawl_date: str, previous_crawl_date: str | None, mock: bool,
                         source: str, limit_rows: int | None, use_llm: bool, dry_run: bool = False) -> int:
    label = crawl_date
    state = _load_state(crawl_date)

    print(f"\n=== [orchestrator] scheduled check for {crawl_date} ===")
    arrival = run_arrival_check(crawl_date, mock)
    print(json.dumps(arrival, indent=2, default=str))

    if not arrival["arrived"]:
        decision = _decide_retry_or_escalate(crawl_date, arrival, state, use_llm)
        state["retry_count"] += 1
        state["attempts"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "arrival": arrival,
            "decision": decision,
        })
        print(f"\n[orchestrator] decision: {json.dumps(decision, indent=2, default=str)}")

        if decision["recommendation"] == "escalate":
            state["escalated"] = True
            state["circuit_breaker_tripped"] = "circuit_breaker" in decision.get("likely_cause", "")
            _save_state(state)
            print(f"\nESCALATE: {crawl_date} has not arrived after {state['retry_count']} "
                  f"attempt(s). {decision['reasoning']} A human should look at this before "
                  f"the next scheduled run.")
            return 2

        _save_state(state)
        retry_minutes = decision.get("retry_minutes") or CFG.get("agentic", {}).get("retry_base_minutes", 240)
        print(f"\nRETRY_LATER: {crawl_date} not arrived yet (attempt {state['retry_count']}). "
              f"{decision['reasoning']} Recommended next check in ~{retry_minutes} min "
              f"(actual next check is whenever the cron schedule next fires -- see "
              f".github/workflows/autopilot.yml). Nothing was touched.")
        return 1

    # Arrived -- proceed through the deterministic stages exactly as
    # run_pipeline.py does. No agentic judgment applies past this point;
    # extraction, aggregation, and validation are pure rules.
    try:
        extract_result = run_extraction(crawl_date, mock, source, limit_rows, dry_run=dry_run)
        print(json.dumps({"extract": extract_result}, indent=2, default=str))
        if extract_result.get("dry_run_only"):
            print(f"\nDRY_RUN_ONLY: scan-size estimate printed above, nothing extracted, "
                  f"nothing written, nothing loaded. Re-run without --dry-run to actually pull data.")
            return 5
        snapshot_result = run_snapshot_build(label)
        print(json.dumps({"build_snapshot": snapshot_result}, indent=2, default=str))
    except Exception as exc:
        state["attempts"].append({"timestamp": datetime.now(timezone.utc).isoformat(), "error": str(exc)})
        _save_state(state)
        print(f"\nERROR during extract/build: {exc}")
        return 4

    use_small_thresholds = mock or bool(limit_rows)
    validation = run_validation(label, previous_crawl_date, use_small_thresholds)
    print(json.dumps({"validate": {"passed": validation.passed, "checks": validation.checks}}, indent=2, default=str))

    state["validation_passed"] = validation.passed

    if not validation.passed:
        state["pending_approval"] = False
        _save_state(state)
        failed = [c for c in validation.checks if not c["passed"]]
        print(f"\nVALIDATION FAILED for {crawl_date}: {json.dumps(failed, indent=2)}\n"
              f"Snapshot written to disk for inspection, NOT promoted. Previous snapshot "
              f"untouched. ALERT a human.")
        return 3

    # PASS -- generate a plain-language summary and STOP. This is the
    # hand-off point to Job 2 (human approval). No load happens from here.
    summary_context = {
        "crawl_date": crawl_date,
        "domain_count": snapshot_result.get("domain_count"),
        "validation_checks": validation.checks,
    }
    summary_result = call_llm(
        "pass_summary", "v1",
        summary_context,
        use_llm=use_llm,
    ) if use_llm else None

    deterministic_summary = (
        f"{crawl_date}: arrived, extracted, built, and VALIDATED "
        f"({snapshot_result.get('domain_count')} domains, all {len(validation.checks)} checks passed). "
        f"Waiting for human approval to write to the production warehouse."
    )
    # Prefer the model's plain-language summary when a real (non-fallback) call
    # succeeded; otherwise use the deterministic sentence above. Either way the
    # underlying facts (domain_count, checks passed) are identical -- this is
    # purely a phrasing choice, never a decision, so falling back costs nothing.
    if summary_result and not summary_result.get("fallback"):
        approval_summary = summary_result["text"].strip()
    else:
        approval_summary = deterministic_summary
    state["pending_approval"] = True
    state["approval_summary"] = approval_summary
    state["loaded"] = False
    _save_state(state)

    print(f"\nPASS -- PENDING_APPROVAL: {approval_summary}\n"
          f"Run `python pipeline/orchestrator_agent.py approve --crawl-date {crawl_date}"
          f"{' --previous-crawl-date ' + previous_crawl_date if previous_crawl_date else ''}` "
          f"(or approve the GitHub Actions environment) to load.")
    return 0


# ---------------------------------------------------------------------------
# Job 2: the approval entrypoint. THE SAFETY-CRITICAL FUNCTION IN THIS FILE.
# ---------------------------------------------------------------------------

def proceed_to_load(crawl_date: str, previous_crawl_date: str | None, mock: bool, land: bool,
                     use_llm: bool) -> int:
    """Hard rule: this function does not trust the state file, does not
    trust that a human clicked approve, and does not trust that
    run_scheduled_check() previously said PASS. It re-runs validate_snapshot()
    itself, right now, and only proceeds if that fresh check returns PASS.
    Everything else (state file, GitHub environment approval) is a
    convenience layer on top of this; this function is the actual gate."""
    label = crawl_date
    use_small_thresholds = mock

    print(f"\n=== [orchestrator] approve/load for {crawl_date} -- re-validating fresh ===")
    validation = run_validation(label, previous_crawl_date, use_small_thresholds)
    print(json.dumps({"validate": {"passed": validation.passed, "checks": validation.checks}}, indent=2, default=str))

    if not validation.passed:
        print(f"\nREFUSED: validate_snapshot() did not return PASS when re-checked just now. "
              f"proceed_to_load() will not run, regardless of any prior state or approval click.")
        return 2

    state = _load_state(crawl_date)

    diff_result = None
    if previous_crawl_date:
        prev_path = REPO_ROOT / CFG["paths"]["snapshots_dir"] / f"snapshot_{previous_crawl_date}.json"
        if prev_path.exists():
            diff_result = run_diff(label, previous_crawl_date)
            print(json.dumps({"diff": diff_result}, indent=2, default=str))

            if use_llm and diff_result and diff_result.get("output_path"):
                from pipeline.summarize_change_event import summarize_change_events_file
                summarized = summarize_change_events_file(
                    Path(diff_result["output_path"]), limit=CFG.get("agentic", {}).get("max_summaries_per_run", 10),
                    use_llm=use_llm,
                )
                print(f"\n[orchestrator] summarized {len(summarized)} change events "
                      f"(see traces in data/llm_traces/traces.jsonl)")

    if land and not mock:
        from pipeline.land_to_warehouse import land_snapshot, land_change_events
        land_result = land_snapshot(label)
        print(json.dumps({"land_snapshot": land_result}, indent=2, default=str))
        if diff_result and diff_result.get("output_path"):
            events_result = land_change_events(label, previous_crawl_date)
            print(json.dumps({"land_change_events": events_result}, indent=2, default=str))

    state["loaded"] = True
    state["pending_approval"] = False
    _save_state(state)
    print(f"\nLOADED: {crawl_date} snapshot promoted"
          f"{' and landed to warehouse' if (land and not mock) else ' (local only, --land not passed)'}.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Agentic autopilot layer on top of run_pipeline.py's deterministic stages.")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--crawl-date", required=True, help="YYYY-MM-01")
    common.add_argument("--previous-crawl-date", default=None)
    common.add_argument("--mock", action="store_true")
    common.add_argument("--source", choices=["production", "sample"], default="production")
    common.add_argument("--limit-rows", type=int, default=None)
    common.add_argument("--no-llm", action="store_true", help="Use deterministic fallback instead of a real LLM call")

    p_check = sub.add_parser("check", parents=[common], help="Job 1: scheduled/manual unattended check")
    p_check.add_argument("--dry-run", action="store_true",
                          help="Stop after the extract cost-estimate check (production source only); nothing extracted or loaded")

    p_approve = sub.add_parser("approve", parents=[common], help="Job 2: human-approved load (re-validates before doing anything)")
    p_approve.add_argument("--land", action="store_true", help="Also land to the real BigQuery warehouse")

    args = parser.parse_args()
    use_llm = not args.no_llm

    if args.command == "check":
        code = run_scheduled_check(args.crawl_date, args.previous_crawl_date, args.mock,
                                    args.source, args.limit_rows, use_llm, dry_run=args.dry_run)
    else:
        code = proceed_to_load(args.crawl_date, args.previous_crawl_date, args.mock, args.land, use_llm)

    sys.exit(code)


if __name__ == "__main__":
    main()
