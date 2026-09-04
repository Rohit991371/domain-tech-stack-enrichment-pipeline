"""llm_client.py -- the ONLY place in this repo that talks to an LLM API.

Every call goes through `call_llm()` below, which does three things every
single time, no exceptions:

  1. Loads the prompt template from prompts/<name>_v<version>.txt on disk
     (never an inline f-string in the caller) -- this is what "prompt
     versioning" means in practice: the version number in the filename is
     the version number in the trace log, always in sync by construction.
  2. Calls the configured provider (Groq by default -- free tier,
     OpenAI-compatible chat-completions endpoint; Anthropic Claude Messages
     API supported too, since the two call sites were prototyped against
     Claude first and switching provider is a one-line config change, not
     a rewrite).
  3. Appends one line to data/llm_traces/traces.jsonl with everything an
     incident review would need: request id, model, prompt_version, the
     rendered input, the raw output, latency, token counts (when the
     provider returns them), and an estimated cost.

If no API key is configured (GROQ_API_KEY / ANTHROPIC_API_KEY), or
--no-llm is passed, call_llm() does NOT fail the pipeline -- it falls back
to a deterministic, rule-based stand-in and traces that fact explicitly
(model="fallback-deterministic"). This mirrors how the rest of the
pipeline treats missing infrastructure: degrade to a safe, inspectable
default rather than crash. The fallback is never used to decide anything
safety-critical -- see orchestrator_agent.py's circuit breaker, which is
enforced in Python regardless of what any LLM (or the fallback) says.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import CFG, REPO_ROOT

PROMPTS_DIR = REPO_ROOT / "prompts"
TRACES_PATH = REPO_ROOT / "data" / "llm_traces" / "traces.jsonl"

# Rough per-token cost table for the trace's cost estimate. Not billing-grade
# precision -- just enough to keep the cost-math habit visible in every
# trace line, per the brief's "prompt versioning and cost math" ask.
_COST_PER_1K_TOKENS_USD = {
    "groq": {"input": 0.0, "output": 0.0},  # free tier at time of writing
    "anthropic": {"input": 0.003, "output": 0.015},  # claude-sonnet-4-6 ballpark
    "fallback-deterministic": {"input": 0.0, "output": 0.0},
}


def _load_prompt(prompt_name: str, version: str) -> str:
    path = PROMPTS_DIR / f"{prompt_name}_{version}.txt"
    if not path.exists():
        raise FileNotFoundError(
            f"No prompt file at {path}. Prompts must be versioned files on "
            f"disk, not inline strings -- see prompts/ and design_doc.md."
        )
    return path.read_text()


def _write_trace(entry: dict):
    TRACES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TRACES_PATH, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def _call_groq(rendered_prompt: str, model: str) -> tuple[str, dict]:
    import urllib.error
    import urllib.request

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")
    api_key = api_key.strip().strip('"').strip("'")  # defensive against stray quotes/whitespace in .env

    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": rendered_prompt}],
        "temperature": 0,
    }).encode()
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            # Groq's API sits behind Cloudflare, which blocks urllib's default
            # "Python-urllib/3.x" User-Agent as a bot signature (Cloudflare
            # error 1010) before the request ever reaches Groq's own auth
            # check. A normal-looking UA is enough to pass that check.
            "User-Agent": "Mozilla/5.0 (compatible; tech-stack-pipeline/1.0; +orchestrator_agent.py)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # Groq puts the actual reason (invalid key, revoked key, model access
        # denied, etc.) in the response body -- surface it, don't just raise
        # the bare status line, or every failure looks like "403: Forbidden"
        # with no way to tell which of those it actually was.
        detail = e.read().decode(errors="replace")
        raise RuntimeError(f"Groq API {e.code} {e.reason}: {detail}") from None
    text = payload["choices"][0]["message"]["content"]
    usage = payload.get("usage", {})
    return text, {"input_tokens": usage.get("prompt_tokens"), "output_tokens": usage.get("completion_tokens")}


def _call_anthropic(rendered_prompt: str, model: str) -> tuple[str, dict]:
    import urllib.error
    import urllib.request

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    api_key = api_key.strip().strip('"').strip("'")

    body = json.dumps({
        "model": model,
        "max_tokens": 500,
        "messages": [{"role": "user", "content": rendered_prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; tech-stack-pipeline/1.0; +orchestrator_agent.py)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        raise RuntimeError(f"Anthropic API {e.code} {e.reason}: {detail}") from None
    text = "".join(b.get("text", "") for b in payload.get("content", []) if b.get("type") == "text")
    usage = payload.get("usage", {})
    return text, {"input_tokens": usage.get("input_tokens"), "output_tokens": usage.get("output_tokens")}


def _fallback(prompt_name: str, context: dict) -> str:
    """Deterministic, rule-based stand-in used when no LLM key is configured
    (or --no-llm is passed). Never used for anything the circuit breaker or
    validation gate depends on -- see orchestrator_agent.py."""
    if prompt_name == "orchestrator_diagnosis":
        retry_count_after = context.get("retry_count_after_this", 0)
        max_retries = context.get("max_retries", 0)
        days_past = context.get("days_past_expected", 0)
        max_days = context.get("max_days_past_expected", 0)
        if retry_count_after >= max_retries or days_past >= max_days:
            return json.dumps({
                "likely_cause": "circuit_breaker_limit_reached",
                "recommendation": "escalate",
                "retry_minutes": None,
                "reasoning": "Deterministic fallback: retry/day limit reached, escalation forced.",
            })
        return json.dumps({
            "likely_cause": "partition_likely_not_landed_yet",
            "recommendation": "retry_later",
            "retry_minutes": 240,
            "reasoning": "Deterministic fallback (no LLM configured): within expected arrival "
                         "window and under retry limits, so retrying is the safe default.",
        })
    if prompt_name == "pass_summary":
        n_checks = len(context.get("validation_checks", []))
        return (f"{context.get('crawl_date', 'unknown date')}: all stages passed "
                f"({context.get('domain_count', '?')} domains, {n_checks}/{n_checks} validation checks passed).")
    if prompt_name == "change_event_summary":
        event = context.get("event", {})
        added = ", ".join(event.get("added", [])) or "none"
        dropped = ", ".join(event.get("dropped", [])) or "none"
        return (f"{event.get('domain', 'unknown domain')}: {event.get('event_type', 'change')} "
                f"-- added [{added}], dropped [{dropped}].")
    return "fallback: no summary available"


def call_llm(prompt_name: str, version: str, context: dict[str, Any], provider: str | None = None,
             model: str | None = None, use_llm: bool = True) -> dict:
    """Render prompts/<prompt_name>_<version>.txt with `context`, call the
    configured provider, trace the call, and return:
        {"text": <raw model output str>, "trace_id": ..., "model": ..., "fallback": bool}
    Callers are responsible for parsing `text` (JSON or plain sentence,
    per the prompt's own contract) -- this module never parses model output,
    it only calls and traces.
    """
    agentic_cfg = CFG.get("agentic", {})
    provider = provider or agentic_cfg.get("llm_provider", "groq")
    model = model or agentic_cfg.get("llm_model", "openai/gpt-oss-20b")

    template = _load_prompt(prompt_name, version)
    rendered = template.format(input_json=json.dumps(context, indent=2, default=str))

    trace_id = str(uuid.uuid4())
    start = time.monotonic()
    used_fallback = False
    error = None

    text = None
    if use_llm:
        key_env_var = "GROQ_API_KEY" if provider == "groq" else "ANTHROPIC_API_KEY"
        if not os.environ.get(key_env_var):
            print(f"[llm_client] WARNING: {key_env_var} not found in environment -- "
                  f"falling back to deterministic response for '{prompt_name}'. "
                  f"If you have a .env file, confirm config.py calls load_dotenv() "
                  f"BEFORE this import runs.")
        try:
            if provider == "groq":
                text, usage = _call_groq(rendered, model)
            elif provider == "anthropic":
                text, usage = _call_anthropic(rendered, model)
            else:
                raise RuntimeError(f"unknown llm_provider: {provider}")
        except Exception as exc:
            error = str(exc)
            text = None
            print(f"[llm_client] WARNING: real LLM call to provider='{provider}' "
                  f"model='{model}' failed, falling back to deterministic response. "
                  f"Error: {error}")

    if text is None:
        used_fallback = True
        text = _fallback(prompt_name, context)
        usage = {"input_tokens": None, "output_tokens": None}
        model_for_trace = "fallback-deterministic"
        provider_for_trace = "fallback-deterministic"
    else:
        model_for_trace = model
        provider_for_trace = provider

    latency_ms = int((time.monotonic() - start) * 1000)
    cost_table = _COST_PER_1K_TOKENS_USD.get(provider_for_trace, {"input": 0.0, "output": 0.0})
    in_tok = usage.get("input_tokens") or 0
    out_tok = usage.get("output_tokens") or 0
    est_cost_usd = round((in_tok / 1000) * cost_table["input"] + (out_tok / 1000) * cost_table["output"], 6)

    trace_entry = {
        "trace_id": trace_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "prompt_name": prompt_name,
        "prompt_version": version,
        "provider": provider_for_trace,
        "model": model_for_trace,
        "used_fallback": used_fallback,
        "error": error,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "estimated_cost_usd": est_cost_usd,
        "latency_ms": latency_ms,
        "request_context": context,
        "response_text": text,
    }
    _write_trace(trace_entry)

    return {"text": text, "trace_id": trace_id, "model": model_for_trace, "fallback": used_fallback}
