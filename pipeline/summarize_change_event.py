"""summarize_change_event.py -- LLM call site #2.

Takes ONE event from diff_snapshots.py's output (a dict with domain,
event_type, added, dropped) and produces a short, grounded, one-sentence
note. This is deliberately the smallest possible LLM feature:

  - No scoring. No priority tier. No "this looks like a hot lead."
  - No invented facts about the company (industry, size, intent) beyond
    what the technology names themselves commonly imply -- see
    prompts/change_event_summary_v1.txt for the exact constraint given to
    the model.
  - Purely a summarization convenience over data that already exists and
    is already correct -- if this call fails or is disabled, the
    change-events feed itself is complete and correct without it. This is
    the same "additive, not load-bearing" posture as the rest of the
    agentic layer: nothing here is allowed to be a dependency for
    correctness.

This is intentionally framed (see docs/design_doc.md) as a signal-shaping
layer that could plug into Firmable's existing Signals / Signal Agent
Actions surface downstream -- as design rationale only. Nothing in this
file pushes to a CRM, a webhook, or any external system.

Usage (batch, over a change-events file already on disk):
    python pipeline/summarize_change_event.py \
        --change-events-file data/change_events/change_events_2026-07-01_to_2026-08-01.json \
        --limit 10
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT
from pipeline.llm_client import call_llm

PROMPT_NAME = "change_event_summary"
PROMPT_VERSION = "v1"


def summarize_change_event(event: dict, use_llm: bool = True) -> dict:
    """event: one item from diff_snapshots.py's output list.
    Returns the event dict with a "summary" key added. Never mutates
    added/dropped/domain/event_type -- those came from the deterministic
    diff and are the ground truth; the summary is purely descriptive
    metadata layered on top."""
    result = call_llm(
        prompt_name=PROMPT_NAME,
        version=PROMPT_VERSION,
        context={"event": event},
        use_llm=use_llm,
    )
    out = dict(event)
    out["summary"] = result["text"].strip()
    out["summary_trace_id"] = result["trace_id"]
    out["summary_model"] = result["model"]
    return out


def summarize_change_events_file(path: Path, limit: int | None = None, use_llm: bool = True) -> list[dict]:
    events = json.loads(path.read_text())
    # Sales value is concentrated in same-domain tech swaps; cap to keep
    # cost/latency bounded on a batch run rather than summarizing every
    # new_domain/dropped_domain event by default.
    tech_change_events = [e for e in events if e["event_type"] == "tech_change"]
    if limit:
        tech_change_events = tech_change_events[:limit]
    return [summarize_change_event(e, use_llm=use_llm) for e in tech_change_events]


def main():
    parser = argparse.ArgumentParser(description="Summarize change events with a grounded, single-sentence LLM note.")
    parser.add_argument("--change-events-file", required=True)
    parser.add_argument("--limit", type=int, default=20, help="Max tech_change events to summarize (cost control)")
    parser.add_argument("--no-llm", action="store_true", help="Use the deterministic fallback instead of calling a real LLM")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    summarized = summarize_change_events_file(Path(args.change_events_file), limit=args.limit, use_llm=not args.no_llm)
    out_path = Path(args.out) if args.out else Path(args.change_events_file).with_name(
        Path(args.change_events_file).stem + "_summarized.json"
    )
    out_path.write_text(json.dumps(summarized, indent=2))
    print(json.dumps({"summarized_count": len(summarized), "output_path": str(out_path)}, indent=2))


if __name__ == "__main__":
    main()
