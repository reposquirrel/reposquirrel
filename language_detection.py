"""Utilities for detecting programming languages of files without relying on cloc.

This module reuses the language metadata shipped with ocloc and replicates its
language-detection heuristics (extensions, special filenames, and simple
shebang parsing). It is intentionally lightweight so scripts such as summery.py
can classify files without shelling out to an external tool.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Dict, Iterable, Optional

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "ocloc_languages.json")
DEFAULT_IGNORE_DIRS = {
    ".git",
    "node_modules",
    ".venv",
    "__pycache__",
    "vendor",
    "target",
    "build",
    "dist",
}


@lru_cache(maxsize=1)
def _load_language_tables() -> tuple[list[dict], Dict[str, str], Dict[str, str]]:
    if not os.path.isfile(DATA_PATH):
        raise FileNotFoundError(f"Missing language data file: {DATA_PATH}")

    with open(DATA_PATH, "r", encoding="utf-8") as fh:
        specs = json.load(fh)

    ext_map: Dict[str, str] = {}
    special_map: Dict[str, str] = {}
    for spec in specs:
        lang_name = spec.get("name")
        if not lang_name:
            continue
        for ext in spec.get("extensions", []) or []:
            ext_map[ext.lower()] = lang_name
        for fname in spec.get("special_filenames", []) or []:
            special_map[fname.lower()] = lang_name
    return specs, ext_map, special_map


def _language_from_shebang(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            first_line = fh.readline()
    except (OSError, UnicodeDecodeError):
        return None

    stripped = first_line.strip()
    if not stripped.startswith("#!"):
        return None

    tokens = stripped[2:].strip().split()
    if not tokens:
        return None
    cmd = tokens[0]
    if cmd.endswith("env") and len(tokens) > 1:
        cmd = tokens[1]
    cmd_lower = cmd.lower()

    if "python" in cmd_lower:
        return "Python"
    if any(shell in cmd_lower for shell in ("bash", "zsh", "ksh", "fish")) or cmd_lower in {"sh"}:
        return "Shell"
    if "node" in cmd_lower or "deno" in cmd_lower:
        return "JavaScript"
    if "perl" in cmd_lower:
        return "Perl"
    if "ruby" in cmd_lower:
        return "Ruby"
    if "php" in cmd_lower:
        return "PHP"
    return None


def detect_language_for_path(path: str) -> Optional[str]:
    """Return the language name for a given file path, or None if unknown."""
    if not path:
        return None

    _, ext_map, special_map = _load_language_tables()
    filename = os.path.basename(path).lower()

    if filename in special_map:
        return special_map[filename]

    if "." in filename:
        ext = filename.rsplit(".", 1)[-1]
        if ext in ext_map:
            return ext_map[ext]

    # Only attempt shebang parsing for extension-less files
    if "." not in os.path.basename(path):
        return _language_from_shebang(path)

    return None


def build_language_map(
    repo_path: str,
    ignore_dirs: Optional[Iterable[str]] = None,
) -> Dict[str, str]:
    """Walk a repository and return {relative_path: language_name} mapping."""
    mapping: Dict[str, str] = {}
    if not repo_path or not os.path.isdir(repo_path):
        return mapping

    ignore_set = {d.lower() for d in (ignore_dirs or DEFAULT_IGNORE_DIRS)}

    for dirpath, dirnames, filenames in os.walk(repo_path):
        dirnames[:] = [
            d
            for d in dirnames
            if d and d.lower() not in ignore_set and not os.path.islink(os.path.join(dirpath, d))
        ]

        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            if not os.path.isfile(full_path):
                continue
            rel_path = os.path.relpath(full_path, repo_path).replace(os.sep, "/")
            lang = detect_language_for_path(full_path) or "Unknown"
            mapping[rel_path] = lang
            mapping[f"./{rel_path}"] = lang

    return mapping
