"""Thin loader around config.yaml. Import `CFG` everywhere instead of
re-parsing YAML in every module.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Populates os.environ from a .env file in the repo root (if present) before
# anything below -- or any other module that does `from config import CFG`,
# e.g. pipeline/llm_client.py -- reads GROQ_API_KEY / ANTHROPIC_API_KEY.
# Safe to call even with no .env file: load_dotenv() is then a no-op.
load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent


def _load() -> dict[str, Any]:
    cfg_path = REPO_ROOT / "config.yaml"
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Allow env vars to override the GCP project without editing yaml,
    # e.g. for CI or a teammate's own project.
    if os.environ.get("GCP_PROJECT_ID"):
        cfg["gcp"]["project_id"] = os.environ["GCP_PROJECT_ID"]

    # Same override for the GCS staging bucket -- config.yaml's committed
    # value is a scrubbed placeholder ("GCS bucket name"), never a real
    # bucket. land_to_warehouse.py reads warehouse.gcs_bucket directly, so
    # without this, --land would try to upload to a bucket that doesn't
    # exist. Set GCS_BUCKET in .env locally or as a GitHub Actions secret.
    if os.environ.get("GCS_BUCKET"):
        cfg["warehouse"]["gcs_bucket"] = os.environ["GCS_BUCKET"]

    return cfg


CFG: dict[str, Any] = _load()


def path_for(key: str) -> Path:
    """Resolve a paths.* config entry to an absolute Path, creating it if needed."""
    rel = CFG["paths"][key]
    p = REPO_ROOT / rel
    p.mkdir(parents=True, exist_ok=True)
    return p


def state_file_path() -> Path:
    p = REPO_ROOT / CFG["paths"]["state_file"]
    p.parent.mkdir(parents=True, exist_ok=True)
    return p