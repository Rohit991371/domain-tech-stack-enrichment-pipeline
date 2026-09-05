"""Thin loader around config.yaml. Import `CFG` everywhere instead of
re-parsing YAML in every module.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


from dotenv import load_dotenv
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
