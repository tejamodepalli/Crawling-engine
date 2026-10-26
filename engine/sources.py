"""
Loads and validates the authorized source registry (sources.yaml).

Two modes:
  strict=True  (default) - raise ValueError on the first problem. Good for
                           tooling/tests that must not run on a bad registry.
  strict=False           - log each invalid entry and skip ONLY that entry.
                           The scheduler uses this so one typo in one source
                           can't stop every other source from being crawled.

The parsed registry is cached and only re-read when sources.yaml changes on
disk, so a 30-second scheduler tick doesn't re-parse YAML every time.
"""
import copy
import logging
import os
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_FILE = os.path.join(PROJECT_ROOT, "sources.yaml")

REQUIRED_FIELDS = ["name", "url", "source_type", "priority", "interval_minutes"]
VALID_PRIORITIES = {"fast", "standard", "bulk"}

_cache: dict = {"key": None, "sources": None}


def validate_source(src: dict) -> list[str]:
    """Return a list of problems with one source entry (empty list = valid)."""
    if not isinstance(src, dict):
        return [f"entry is not a mapping: {src!r}"]

    problems = []

    missing = [f for f in REQUIRED_FIELDS if src.get(f) in (None, "")]
    if missing:
        problems.append(f"missing fields: {missing}")

    priority = src.get("priority")
    if priority is not None and priority not in VALID_PRIORITIES:
        problems.append(f"invalid priority '{priority}' (must be one of {sorted(VALID_PRIORITIES)})")

    url = src.get("url")
    if url:
        parsed = urlparse(str(url))
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            problems.append(f"url must be an absolute http(s) URL, got {url!r}")

    for field in ("interval_minutes", "timeout_seconds"):
        if field in src:
            try:
                if int(src[field]) <= 0:
                    problems.append(f"{field} must be a whole number greater than 0")
            except (TypeError, ValueError):
                problems.append(f"{field} must be a whole number, got {src[field]!r}")

    return problems


def load_sources(path: str = SOURCES_FILE, strict: bool = True) -> list[dict]:
    mtime = os.path.getmtime(path)
    key = (path, mtime, strict)
    if _cache["key"] == key:
        return copy.deepcopy(_cache["sources"])

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    valid = []
    seen_names = set()

    for src in data.get("sources") or []:
        label = src.get("name", "<unnamed>") if isinstance(src, dict) else "<invalid entry>"
        problems = validate_source(src)

        if not problems and src["name"] in seen_names:
            problems.append(f"duplicate source name: {src['name']}")

        if problems:
            message = f"Source {label}: " + "; ".join(problems)
            if strict:
                raise ValueError(message)
            logger.error("Skipping invalid source — %s", message)
            continue

        seen_names.add(src["name"])
        src.setdefault("enabled", True)
        src.setdefault("timeout_seconds", 120)
        src["interval_minutes"] = int(src["interval_minutes"])
        src["timeout_seconds"] = int(src["timeout_seconds"])
        valid.append(src)

    _cache["key"] = key
    _cache["sources"] = valid
    return copy.deepcopy(valid)


def get_source(name: str, path: str = SOURCES_FILE) -> dict | None:
    for src in load_sources(path):
        if src["name"] == name:
            return src
    return None
