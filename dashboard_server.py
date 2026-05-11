#!/usr/bin/env python3
import os
import json
import queue
import threading
import time
import sys
import argparse
import subprocess
import logging
import re
import shlex
import multiprocessing
import shutil
import tempfile
import copy
import calendar
import configparser
import unicodedata
import uuid
import signal
import glob
import atexit
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Any, List, Tuple, Optional, Set, Iterable
from collections import defaultdict, Counter, deque

from flask import Flask, jsonify, send_from_directory, render_template, abort, request, Response, stream_with_context

import subsystem_metrics
from subsystem_metrics import (
    compute_dead_subsystems,
    compute_subsystem_top_maintainers,
    compute_subsystem_maintainer_timeline,
    compute_subsystem_significant_ownership,
    compute_subsystem_size_rankings,
)
import pagerduty_sync

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "configuration")
STATS_ROOT = os.environ.get("REPO_SQUIRREL_STATS_ROOT") or os.path.join(BASE_DIR, "stats")
REPO_ROOT = os.path.join(BASE_DIR, "repos")
CLOC_CACHE_FILE = os.path.join(STATS_ROOT, "cloc_cache.json")
BADGE_CACHE_FILE = os.path.join(STATS_ROOT, "badges_summary.json")
PAGERDUTY_STATS_DIR = os.path.join(STATS_ROOT, "pagerduty")
PAGERDUTY_OVERVIEW_FILE = os.path.join(PAGERDUTY_STATS_DIR, "overview.json")
PAGERDUTY_INCIDENTS_FILE = os.path.join(PAGERDUTY_STATS_DIR, "incidents_last_year.json")
OWNERSHIP_DISTRIBUTION_FILE = os.path.join(STATS_ROOT, "ownership_distribution.json")
TEAM_OVERVIEW_CACHE_FILE = os.path.join(STATS_ROOT, "team_overview_last3months.json")
SSH_KNOWN_HOSTS_FILE = os.path.join(CONFIG_DIR, "known_hosts")
ALIASES_FILE = os.path.join(CONFIG_DIR, "alias.json")
IGNORE_USERS_FILE = os.path.join(CONFIG_DIR, "ignore_user.txt")
TEAMS_CONFIG_FILE = os.path.join(CONFIG_DIR, "teams.json")
SERVICES_CONFIG_FILE = os.path.join(CONFIG_DIR, "services.json")
CAPACITY_CONFIG_FILE = os.path.join(CONFIG_DIR, "capacity_config.json")
TEAM_RESPONSIBILITIES_FILE = os.path.join(CONFIG_DIR, "team_subsystem_responsibilities.json")

REPO_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
CLONE_TASK_RETENTION_SECONDS = 1800

_repo_clone_tasks: Dict[str, Dict[str, Any]] = {}
_repo_clone_lock = threading.Lock()

TEAM_METRIC_FIELDS = [
    "total_commits",
    "total_additions",
    "total_deletions",
    "total_lines_changed",
    "subsystems_touched",
]

MONTH_ABBREVIATIONS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

def get_default_rolling_months(reference: Optional[datetime] = None) -> int:
    ref = reference or datetime.now(timezone.utc)
    current_month = max(1, min(12, ref.month))
    return 12 + current_month



def _detect_parallel_default() -> int:
    try:
        affinity = os.sched_getaffinity(0)
        if affinity:
            return max(1, len(affinity) * 2)
    except (AttributeError, OSError):
        pass
    return max(1, multiprocessing.cpu_count() * 2)


ROOT_SUBSYSTEM_KEY = "__root__"
MASTER_PROGRESS_PREFIX = "[[MASTER_PROGRESS]]"

# Caches for expensive operations
_SERVICES_CONFIG_CACHE: Optional[Dict[str, Dict[str, List[str]]]] = None
_REPO_LANGUAGE_CACHE: Dict[str, Dict[str, Any]] = {}
_REPO_LANGUAGE_SNAPSHOT_CACHE: Dict[Tuple[str, Optional[int], Optional[int]], Optional[Dict[str, int]]] = {}
_SERVICE_LANGUAGE_CACHE: Dict[Tuple[str, str], Dict[str, int]] = {}
_CLOC_CACHE_DATA: Optional[Dict[str, Dict[str, Any]]] = None
_BADGE_CACHE_DATA: Optional[Dict[str, Any]] = None
_BADGE_CACHE_MTIME: Optional[float] = None

_SUBSYSTEM_TOUCH_COUNT_CACHE: Dict[int, Dict[str, int]] = {}
_OWNERSHIP_DATA_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {}
_SUBSYSTEM_MANIFEST: Optional[Dict[str, Any]] = None
_ALIAS_VARIANTS_CACHE: Optional[Dict[str, List[str]]] = None
_LANGUAGE_DISTRIBUTION_CACHE: Dict[Tuple[str, int, int], Optional[Dict[str, int]]] = {}
_USER_ACTIVITY_CACHE: Dict[Tuple[str, str, str, Tuple[str, ...]], Dict[str, Any]] = {}
_SUBSYSTEM_PRIMARY_LANGUAGE_CACHE: Dict[Tuple[str, str], Optional[str]] = {}
_REPO_PRIMARY_LANGUAGE_CACHE: Dict[str, Optional[str]] = {}

_USER_SLUG_INDEX: Optional[Dict[str, str]] = None


def load_badge_cache(force_refresh: bool = False) -> Dict[str, Any]:
    global _BADGE_CACHE_DATA, _BADGE_CACHE_MTIME
    if force_refresh:
        _BADGE_CACHE_DATA = None
        _BADGE_CACHE_MTIME = None
    if _BADGE_CACHE_DATA is not None:
        return _BADGE_CACHE_DATA
    if not os.path.isfile(BADGE_CACHE_FILE):
        return {}
    try:
        mtime = os.path.getmtime(BADGE_CACHE_FILE)
        cache = load_json(BADGE_CACHE_FILE, default={})
        _BADGE_CACHE_DATA = cache
        _BADGE_CACHE_MTIME = mtime
        return cache
    except Exception:
        _BADGE_CACHE_DATA = None
        _BADGE_CACHE_MTIME = None
        return {}


def save_badge_cache(payload: Dict[str, Any]) -> Dict[str, Any]:
    save_json(BADGE_CACHE_FILE, payload)
    global _BADGE_CACHE_DATA, _BADGE_CACHE_MTIME
    _BADGE_CACHE_DATA = payload
    try:
        _BADGE_CACHE_MTIME = os.path.getmtime(BADGE_CACHE_FILE)
    except OSError:
        _BADGE_CACHE_MTIME = None
    return payload


def refresh_badge_cache() -> Dict[str, Any]:
    cache_data = build_badge_cache_data()
    if not cache_data:
        return {}
    return save_badge_cache(cache_data)


def _reset_user_slug_index() -> None:
    global _USER_SLUG_INDEX
    _USER_SLUG_INDEX = None


def load_json(path: str, default: Optional[Any] = None) -> Any:
    if default is None:
        default = {}
    if not os.path.exists(path):
        return copy.deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        return copy.deepcopy(default)


def save_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def is_pagerduty_configured() -> bool:
    data = load_json(INTEGRATIONS_FILE, default={})
    pagerduty = data.get("pagerduty") if isinstance(data, dict) else {}
    if isinstance(pagerduty, dict):
        token = pagerduty.get("api_token")
        if isinstance(token, str):
            return bool(token.strip())
    return False


def load_team_subsystem_responsibilities() -> Dict[str, List[str]]:
    path = os.path.join(BASE_DIR, "configuration", "team_subsystem_responsibilities.json")
    data = load_json(path, default={})
    cleaned: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for team_id, subsystems in data.items():
            if not isinstance(team_id, str):
                continue
            if isinstance(subsystems, list):
                cleaned[team_id] = [s for s in subsystems if isinstance(s, str)]
            elif isinstance(subsystems, str):
                cleaned[team_id] = [subsystems]
    return cleaned


def get_team_responsible_subsystems(team_id: str) -> List[str]:
    responsibilities = load_team_subsystem_responsibilities()
    subsystems = responsibilities.get(team_id, [])
    return subsystems if isinstance(subsystems, list) else []


def _infer_period_bounds(period_label: str) -> Tuple[str, str, bool]:
    if not period_label:
        return ("", "", False)
    if "_" in period_label:
        start, end = period_label.split("_", 1)
        return (start, end, False)
    if len(period_label) == 4 and period_label.isdigit():
        return (f"{period_label}-01-01", f"{period_label}-12-31", True)
    if len(period_label) == 7 and period_label[4] == "-":
        year = int(period_label[:4])
        month = int(period_label[5:7])
        last_day = calendar.monthrange(year, month)[1]
        return (f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}", False)
    return (period_label, period_label, False)


def _format_period_label(label: str) -> str:
    if not label:
        return ""
    if "_" in label:
        start, end = label.split("_", 1)
        return f"{start} → {end}"
    if len(label) == 7 and label[4] == "-":
        return label
    return label


def list_user_months() -> Dict[str, List[Dict[str, Any]]]:
    users_root = os.path.join(STATS_ROOT, "users")
    if not os.path.isdir(users_root):
        return {}

    def _add_period(periods: List[Dict[str, Any]], label: str, folder: str) -> None:
        if not label or not folder:
            return
        start, end, is_yearly = _infer_period_bounds(label)
        periods.append({
            "folder": folder,
            "label": _format_period_label(label),
            "from": start or label,
            "to": end or label,
            "is_yearly": is_yearly,
        })

    results: Dict[str, List[Dict[str, Any]]] = {}
    for slug in sorted(os.listdir(users_root)):
        slug_path = os.path.join(users_root, slug)
        if not os.path.isdir(slug_path):
            continue
        periods: List[Dict[str, Any]] = []

        # Support legacy flat files directly under the user directory
        for filename in sorted(os.listdir(slug_path)):
            file_path = os.path.join(slug_path, filename)
            if os.path.isfile(file_path) and filename.endswith(".json"):
                _add_period(periods, filename[:-5], filename)

        # Support nested year/month layout (e.g., 2025/01.json, 2025/yearly.json)
        for entry in sorted(os.listdir(slug_path)):
            entry_path = os.path.join(slug_path, entry)
            if not os.path.isdir(entry_path):
                continue
            year_part: Optional[int] = None
            if entry.isdigit():
                try:
                    year_part = int(entry)
                except ValueError:
                    year_part = None
            for filename in sorted(os.listdir(entry_path)):
                if not filename.endswith(".json"):
                    continue
                rel_folder = os.path.join(entry, filename)
                label: Optional[str] = None
                name_part = filename[:-5]
                if filename == "yearly.json" and year_part:
                    label = f"{year_part:04d}"
                elif year_part and name_part.isdigit():
                    try:
                        month_val = int(name_part)
                        label = f"{year_part:04d}-{month_val:02d}"
                    except ValueError:
                        label = f"{entry}-{name_part}"
                elif year_part:
                    label = f"{entry}-{name_part}"
                else:
                    label = name_part
                _add_period(periods, label, rel_folder)

        if periods:
            periods.sort(key=lambda item: (item["from"], item["to"]), reverse=True)
            results[slug] = periods
    return results


def build_team_periods_from_members(
    members: List[str],
    user_month_lookup: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    if not members:
        return []
    lookup = user_month_lookup or list_user_months()
    aggregated: Dict[Tuple[str, str, bool], Dict[str, Any]] = {}
    for member in members:
        lookup_key = resolve_user_slug(member) or member
        for period in lookup.get(lookup_key, []) or []:
            start = period.get("from")
            end = period.get("to")
            if not start or not end:
                continue
            key = (start, end, bool(period.get("is_yearly")))
            if key in aggregated:
                continue
            label = period.get("label") or (
                _format_period_label(f"{start}_{end}") if not bool(period.get("is_yearly")) else start[:4]
            )
            aggregated[key] = {
                "from": start,
                "to": end,
                "label": label,
                "is_yearly": bool(period.get("is_yearly")),
            }
    return sorted(aggregated.values(), key=lambda item: item["from"])


def resolve_user_period_summary_path(user_slug: str, period: Dict[str, Any]) -> Optional[str]:
    users_root = os.path.join(STATS_ROOT, "users", user_slug)
    if not os.path.isdir(users_root):
        return None
    folder = period.get("folder")
    if not folder:
        return None
    candidate = os.path.join(users_root, folder)
    return candidate if os.path.exists(candidate) else None


def find_user_summary(user_slug: str, from_date: str, to_date: str) -> Optional[str]:
    periods = list_user_months().get(user_slug, [])
    for period in periods:
        if period.get("from") == from_date and period.get("to") == to_date:
            path = resolve_user_period_summary_path(user_slug, period)
            if path:
                return path
    users_root = os.path.join(STATS_ROOT, "users", user_slug)
    if not os.path.isdir(users_root):
        return None
    for filename in sorted(os.listdir(users_root)):
        if filename.endswith(".json"):
            path = os.path.join(users_root, filename)
            if from_date in filename and to_date in filename:
                return path
    json_files = [f for f in os.listdir(users_root) if f.endswith(".json")]
    if json_files:
        return os.path.join(users_root, sorted(json_files)[-1])
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _pick_number(data: Optional[Dict[str, Any]], *keys: str, default: int = 0) -> int:
    if not isinstance(data, dict):
        return default
    for key in keys:
        if key in data and data[key] is not None:
            return _safe_int(data[key], default)
    return default


def _build_member_contribution_entry(member_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(member_data, dict):
        member_data = {}
    commits = _safe_int(member_data.get("total_commits"), 0)
    additions = _pick_number(member_data, "total_lines_added", "total_additions")
    deletions = _pick_number(member_data, "total_lines_deleted", "total_deletions")
    per_repo = member_data.get("per_repo") if isinstance(member_data, dict) else {}
    languages = member_data.get("languages") if isinstance(member_data, dict) else {}

    subsystems_touched = 0
    subsystem_keys: List[str] = []
    if isinstance(per_repo, dict):
        for repo_name, repo_data in per_repo.items():
            if not isinstance(repo_data, dict):
                continue
            if (
                _safe_int(repo_data.get("commits"), 0)
                or _safe_int(repo_data.get("additions"), 0)
                or _safe_int(repo_data.get("deletions"), 0)
            ):
                subsystems_touched += 1
                repo_label = str(repo_name).strip()
                if repo_label and repo_label not in subsystem_keys:
                    subsystem_keys.append(repo_label)

    languages_used = 0
    language_keys: List[str] = []
    if isinstance(languages, dict):
        for language_name, lang_data in languages.items():
            if not isinstance(lang_data, dict):
                continue
            if (
                _safe_int(lang_data.get("net_lines"), 0)
                or _safe_int(lang_data.get("additions"), 0)
                or _safe_int(lang_data.get("deletions"), 0)
            ):
                languages_used += 1
                language_label = str(language_name).strip()
                if language_label and language_label not in language_keys:
                    language_keys.append(language_label)

    return {
        "commits": commits,
        "additions": additions,
        "deletions": deletions,
        "lines_changed": additions + deletions,
        "net_lines": additions - deletions,
        "subsystems_touched": subsystems_touched,
        "languages_used": languages_used,
        "subsystem_keys": subsystem_keys,
        "language_keys": language_keys,
    }


def _ensure_member_contributions(
    members: List[str],
    from_date: Optional[str],
    to_date: Optional[str],
    existing: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    contributions: Dict[str, Dict[str, Any]] = dict(existing or {})
    if not members or not from_date or not to_date:
        return contributions
    for member in members:
        try:
            member_data = aggregate_user_data_for_period(member, from_date, to_date)
        except Exception:
            member_data = {}
        computed_entry = _build_member_contribution_entry(member_data)
        previous_entry = contributions.get(member, {})
        merged_entry = dict(previous_entry)
        merged_entry.update(computed_entry)
        contributions[member] = merged_entry
    return contributions


def _extract_team_metrics(summary: Dict[str, Any]) -> Dict[str, int]:
    total_commits = _safe_int(summary.get("total_commits"), _safe_int(summary.get("commits"), 0))
    additions = _safe_int(summary.get("total_additions"), _safe_int(summary.get("lines_added"), 0))
    deletions = _safe_int(summary.get("total_deletions"), _safe_int(summary.get("lines_deleted"), 0))
    subsystems_value = summary.get("subsystems_touched")
    if subsystems_value is None:
        subsystems_value = len((summary.get("subsystems") or {}))
    subsystems_touched = _safe_int(subsystems_value, 0)
    total_lines_changed = additions + deletions
    return {
        "total_commits": total_commits,
        "total_additions": additions,
        "total_deletions": deletions,
        "total_lines_changed": total_lines_changed,
        "subsystems_touched": subsystems_touched,
    }


def _load_teams_config() -> Dict[str, Any]:
    if not os.path.isfile(TEAMS_CONFIG_FILE):
        return {}
    try:
        with open(TEAMS_CONFIG_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _aggregate_team_metrics_for_period(
    team_info: Optional[Dict[str, Any]],
    from_date: str,
    to_date: str,
    alias_lookup: Optional[Dict[str, str]] = None,
) -> Dict[str, int]:
    alias_lookup = alias_lookup or load_alias_lookup()
    members = team_info.get("members", []) if isinstance(team_info, dict) else []
    canonical_members: List[str] = []
    seen: Set[str] = set()
    for member in members:
        canonical = canonicalize_slug(member, alias_lookup) or member
        if canonical and canonical not in seen:
            seen.add(canonical)
            canonical_members.append(canonical)
    totals = {
        "total_commits": 0,
        "total_additions": 0,
        "total_deletions": 0,
        "total_lines_changed": 0,
        "subsystems_touched": 0,
    }
    if not canonical_members:
        return totals
    subsystems_seen: Set[str] = set()
    for member in canonical_members:
        try:
            member_data = aggregate_user_data_for_period(member, from_date, to_date)
        except Exception:
            member_data = None
        if not member_data:
            continue
        totals["total_commits"] += _safe_int(member_data.get("total_commits"), 0)
        additions = _safe_int(member_data.get("total_lines_added"), _safe_int(member_data.get("total_additions"), 0))
        deletions = _safe_int(member_data.get("total_lines_deleted"), _safe_int(member_data.get("total_deletions"), 0))
        totals["total_additions"] += additions
        totals["total_deletions"] += deletions
        per_repo = member_data.get("per_repo") or {}
        if isinstance(per_repo, dict):
            for repo_name, repo_stats in per_repo.items():
                if not isinstance(repo_stats, dict):
                    continue
                if (
                    _safe_int(repo_stats.get("commits"), 0)
                    or _safe_int(repo_stats.get("additions"), 0)
                    or _safe_int(repo_stats.get("deletions"), 0)
                ):
                    subsystems_seen.add(str(repo_name))
    totals["subsystems_touched"] = len(subsystems_seen)
    totals["total_lines_changed"] = totals["total_additions"] + totals["total_deletions"]
    return totals


def _build_team_peer_rankings(rows: List[Tuple[str, Dict[str, Any]]], target_team_id: str) -> Dict[str, Dict[str, Any]]:
    rows_map = {team_id: metrics or {} for team_id, metrics in rows}
    total = len(rows_map)
    if total == 0 or target_team_id not in rows_map:
        return {}
    rankings: Dict[str, Dict[str, Any]] = {}
    for metric in TEAM_METRIC_FIELDS:
        metric_values: List[Tuple[str, float]] = [
            (team_id, float(rows_map.get(team_id, {}).get(metric, 0))) for team_id in rows_map
        ]
        metric_values.sort(key=lambda item: item[1], reverse=True)
        prev_value: Optional[float] = None
        current_rank = 0
        for index, (team_key, value) in enumerate(metric_values, start=1):
            if prev_value is None or value != prev_value:
                current_rank = index
                prev_value = value
            if team_key == target_team_id:
                percentile = 100.0 if total == 0 else round((current_rank / total) * 100, 1)
                rankings[metric] = {
                    "rank": current_rank,
                    "value": value,
                    "total": total,
                    "percentile": percentile,
                }
                break
    return rankings


def compute_team_peer_rankings(
    team_id: str,
    from_date: Optional[str],
    to_date: Optional[str],
    target_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    if not from_date and not to_date:
        return {}
    if not from_date:
        from_date = to_date
    if not to_date:
        to_date = from_date
    if not from_date or not to_date:
        return {}
    teams_config = _load_teams_config()
    if not teams_config and not target_metrics:
        return {}
    alias_lookup = load_alias_lookup()
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for other_team_id, team_info in teams_config.items():
        if other_team_id == team_id and target_metrics is not None:
            metrics = target_metrics
        else:
            metrics = _aggregate_team_metrics_for_period(team_info, from_date, to_date, alias_lookup)
        rows.append((other_team_id, metrics))
    if team_id not in teams_config and target_metrics is not None:
        rows.append((team_id, target_metrics))
    return _build_team_peer_rankings(rows, team_id)


def _build_badge_display_name_map(
    target_slugs: Set[str],
    user_months: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, str]:
    display_names = {slug: slug for slug in target_slugs}
    if not target_slugs:
        return display_names
    months_lookup = user_months or list_user_months()
    for slug in target_slugs:
        months = months_lookup.get(slug) or []
        if not months:
            continue
        period = months[0]
        start = period.get("from")
        end = period.get("to")
        summary_path = None
        if start and end:
            summary_path = find_user_summary(slug, start, end)
        if summary_path and os.path.isfile(summary_path):
            try:
                summary_data = load_json(summary_path)
                display_names[slug] = summary_data.get("author_name", slug)
            except Exception:
                continue
    return display_names


def _build_badge_rankings(per_user: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    rankings: Dict[str, Dict[str, int]] = {}
    if not per_user:
        return rankings
    sortable: List[Tuple[str, int]] = []
    for slug, entry in per_user.items():
        badges = entry.get("badges") if isinstance(entry, dict) else None
        count = len(badges or [])
        sortable.append((slug, count))
    sortable.sort(key=lambda item: item[1], reverse=True)
    last_count: Optional[int] = None
    last_rank = 0
    for idx, (slug, count) in enumerate(sortable, start=1):
        if count != last_count:
            last_rank = idx
            last_count = count
        rankings[slug] = {"rank": last_rank, "count": count}
    return rankings


def build_badge_cache_data() -> Optional[Dict[str, Any]]:
    """Aggregate badge data for all developers for fast API responses."""
    try:
        badges_by_user = analyze_developer_badges()
        user_months = list_user_months()
        display_names = _build_badge_display_name_map(set(badges_by_user.keys()), user_months)
        badge_type_counts: Counter = Counter()
        top_badge_holders: List[Dict[str, Any]] = []
        top_ownership_holders: List[Dict[str, Any]] = []
        per_user_payload: Dict[str, Dict[str, Any]] = {}
        total_badges = 0

        for slug, badges in badges_by_user.items():
            total_badges += len(badges)
            type_counts = Counter(badge.get("type", "unknown") for badge in badges)
            badge_type_counts.update(type_counts)
            type_counts_dict = {k: int(v) for k, v in type_counts.items()}
            display_name = display_names.get(slug, slug)
            top_badge_holders.append({
                "slug": slug,
                "display_name": display_name,
                "badge_count": len(badges),
                "type_counts": type_counts_dict,
            })
            ownership_badge_count = type_counts_dict.get("ownership_percentage", 0)
            if ownership_badge_count > 0:
                subsystems = sorted({
                    badge.get("subsystem")
                    for badge in badges
                    if badge.get("type") == "ownership_percentage" and badge.get("subsystem")
                })
                top_ownership_holders.append({
                    "slug": slug,
                    "display_name": display_name,
                    "ownership_badge_count": ownership_badge_count,
                    "subsystems": subsystems,
                })
            per_user_payload[slug] = {
                "display_name": display_name,
                "badges": badges,
            }

        badge_types_dict = {k: int(v) for k, v in badge_type_counts.items()}
        for key in ["productivity", "maintainer", "ownership", "ownership_percentage"]:
            badge_types_dict.setdefault(key, 0)

        summary = {
            "users_with_badges": len(badges_by_user),
            "total_badges": total_badges,
            "badge_types": badge_types_dict,
            "total_users": len(user_months),
        }

        top_badge_holders.sort(
            key=lambda entry: (
                -entry["badge_count"],
                -entry["type_counts"].get("productivity", 0),
                entry["slug"],
            )
        )
        top_ownership_holders.sort(
            key=lambda entry: (-entry["ownership_badge_count"], entry["slug"])
        )

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "summary": summary,
            "top_badge_holders": top_badge_holders[:50],
            "top_ownership_holders": top_ownership_holders[:50],
            "per_user": per_user_payload,
        }
    except Exception as exc:
        print(f"Error building badge cache: {exc}")
        return None


def analyze_developer_badges() -> Dict[str, List[Dict[str, Any]]]:
    """Analyze blame and activity data to determine developer badges."""
    badges: Dict[str, List[Dict[str, Any]]] = {}
    try:
        ownership_badges = analyze_ownership_badges()
        maintainer_badges = analyze_maintainer_badges()
        productive_badge = analyze_most_productive_badge()
        ownership_percentage_badges = analyze_ownership_percentage_badges()

        for dev_slug in ownership_badges:
            badges.setdefault(dev_slug, []).extend(ownership_badges[dev_slug])
        for dev_slug in maintainer_badges:
            badges.setdefault(dev_slug, []).extend(maintainer_badges[dev_slug])
        for dev_slug in ownership_percentage_badges:
            badges.setdefault(dev_slug, []).extend(ownership_percentage_badges[dev_slug])

        if productive_badge:
            dev_slug, badge = productive_badge
            badges.setdefault(dev_slug, []).append(badge)

        for dev_slug in badges:
            badges[dev_slug].sort(
                key=lambda b: (b["type"], -b.get("share", b.get("commits", b.get("lines_added", 0))))
            )
        return badges
    except Exception as exc:
        print(f"Error in analyze_developer_badges: {exc}")
        return {}


def analyze_ownership_badges() -> Dict[str, List[Dict[str, Any]]]:
    """Analyze blame data for ownership badges."""
    badges: Dict[str, List[Dict[str, Any]]] = {}
    try:
        ownership_data = _get_ownership_data()
        service_keys = set(ownership_data.get("service", {}).keys())
        repo_top_map: Dict[str, Optional[str]] = {}

        for key, entry in ownership_data.get("repo", {}).items():
            if key in service_keys:
                continue
            slug, lines, share = _find_top_developer(entry)
            if not slug or lines <= 0:
                continue
            repo_top_map[key] = slug
            badges.setdefault(slug, []).append({
                "type": "ownership",
                "badge_type": "repository_owner",
                "title": f"Top Owner: {entry['name']}",
                "subtitle": f"{lines:,} lines ({share*100:.1f}%)",
                "subsystem": entry["name"],
                "repo_path": entry["repo_path"],
                "lines": lines,
                "share": share,
            })

        for key, entry in ownership_data.get("service", {}).items():
            slug, lines, share = _find_top_developer(entry)
            if not slug or lines <= 0:
                continue
            if repo_top_map.get(key) == slug:
                continue
            badges.setdefault(slug, []).append({
                "type": "ownership",
                "badge_type": "service_owner",
                "title": f"Top Owner: {entry['name']}",
                "subtitle": f"{lines:,} lines ({share*100:.1f}%)",
                "subsystem": entry["name"],
                "repo_path": entry["repo_path"],
                "lines": lines,
                "share": share,
            })

        return badges
    except Exception as exc:
        print(f"Error analyzing ownership badges: {exc}")
        return badges


def analyze_maintainer_badges() -> Dict[str, List[Dict[str, Any]]]:
    """Analyze recent commit activity (last 3 months) to determine top maintainers."""
    badges: Dict[str, List[Dict[str, Any]]] = {}
    lookback_days = 90

    subsystem_entries = _discover_subsystems()
    subsystem_names = sorted({entry.get("name") for entry in subsystem_entries if entry.get("name")})
    if not subsystem_names:
        return badges

    for subsystem_name in subsystem_names:
        try:
            maintainer_payload = compute_subsystem_top_maintainers(
                STATS_ROOT,
                subsystem_name,
                lookback_days,
            )
        except Exception:
            continue

        maintainers = maintainer_payload.get("maintainers") or []
        if not maintainers:
            continue

        top_entry = maintainers[0]
        top_dev_slug = top_entry.get("slug")
        top_commits = _safe_int(top_entry.get("commits"))
        if not top_dev_slug or top_commits < 3:
            continue

        badges.setdefault(top_dev_slug, []).append({
            "type": "maintainer",
            "badge_type": "top_maintainer",
            "title": f"Top Maintainer: {subsystem_name}",
            "subtitle": f"{top_commits} commits (last 3 months)",
            "subsystem": subsystem_name,
            "commits": top_commits,
            "period": "3 months",
        })

    return badges


def _iter_summary_developers(summary_data: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    developers = summary_data.get("developers")
    if isinstance(developers, dict) and developers:
        for slug, payload in developers.items():
            yield slug, payload
        return
    months = summary_data.get("months")
    if not isinstance(months, dict):
        return
    for month_payload in months.values():
        month_devs = month_payload.get("developers")
        if not isinstance(month_devs, dict):
            continue
        for slug, payload in month_devs.items():
            yield slug, payload


def _collect_repo_yearly_developer_totals(
    preferred_year: Optional[int],
    allow_fallback: bool = True,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
    repo_root = os.path.join(STATS_ROOT, "repos")
    if not os.path.isdir(repo_root):
        return {}, None
    year_files: Dict[int, List[str]] = defaultdict(list)
    for owner in os.listdir(repo_root):
        owner_path = os.path.join(repo_root, owner)
        if not os.path.isdir(owner_path):
            continue
        for repo_name in os.listdir(owner_path):
            repo_path = os.path.join(owner_path, repo_name)
            if not os.path.isdir(repo_path):
                continue
            summary_root = os.path.join(repo_path, "summary")
            if not os.path.isdir(summary_root):
                continue
            for year_entry in os.listdir(summary_root):
                if not year_entry.isdigit():
                    continue
                yearly_file = os.path.join(summary_root, year_entry, "yearly.json")
                if os.path.isfile(yearly_file):
                    year_files[int(year_entry)].append(yearly_file)
    if not year_files:
        return {}, None
    if preferred_year is not None and preferred_year in year_files:
        target_year = preferred_year
    elif allow_fallback:
        target_year = max(year_files.keys())
    else:
        return {}, None
    developer_totals: Dict[str, Dict[str, Any]] = {}
    for file_path in year_files[target_year]:
        try:
            summary_data = load_json(file_path)
        except Exception:
            continue
        for dev_slug, dev_data in _iter_summary_developers(summary_data):
            lines_added = _safe_int(dev_data.get("lines_added"))
            if lines_added <= 0:
                continue
            entry = developer_totals.setdefault(dev_slug, {
                "lines_added": 0,
                "display_name": dev_data.get("display_name", dev_slug),
            })
            entry["lines_added"] += lines_added
            if not entry.get("display_name") and dev_data.get("display_name"):
                entry["display_name"] = dev_data["display_name"]
    return developer_totals, target_year


def _collect_subsystem_yearly_developer_totals(
    preferred_year: Optional[int],
    allow_fallback: bool = True,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
    subsystems_path = os.path.join(STATS_ROOT, "subsystems")
    if not os.path.isdir(subsystems_path):
        return {}, None
    year_files: Dict[int, List[str]] = defaultdict(list)
    for subsystem_name in os.listdir(subsystems_path):
        subsystem_path = os.path.join(subsystems_path, subsystem_name)
        if not os.path.isdir(subsystem_path):
            continue
        for period_dir in os.listdir(subsystem_path):
            if len(period_dir) != 21 or "_" not in period_dir:
                continue
            try:
                year = int(period_dir[:4])
            except ValueError:
                continue
            summary_file = os.path.join(subsystem_path, period_dir, "summary.json")
            if os.path.isfile(summary_file):
                year_files[year].append(summary_file)
    if not year_files:
        return {}, None
    if preferred_year is not None and preferred_year in year_files:
        target_year = preferred_year
    elif allow_fallback:
        target_year = max(year_files.keys())
    else:
        return {}, None
    developer_totals: Dict[str, Dict[str, Any]] = {}
    for file_path in year_files[target_year]:
        try:
            summary_data = load_json(file_path)
        except Exception:
            continue
        for dev_slug, dev_data in _iter_summary_developers(summary_data):
            lines_added = _safe_int(dev_data.get("lines_added"))
            if lines_added <= 0:
                continue
            entry = developer_totals.setdefault(dev_slug, {
                "lines_added": 0,
                "display_name": dev_data.get("display_name", dev_slug),
            })
            entry["lines_added"] += lines_added
            if not entry.get("display_name") and dev_data.get("display_name"):
                entry["display_name"] = dev_data["display_name"]
    return developer_totals, target_year


def analyze_most_productive_badge() -> Optional[Tuple[str, Dict[str, Any]]]:
    """Find the most productive developer based on yearly lines added."""
    current_year = datetime.now().year
    using_repo_stats = True
    developer_totals, analysis_year = _collect_repo_yearly_developer_totals(current_year)
    if not developer_totals:
        using_repo_stats = False
        developer_totals, analysis_year = _collect_subsystem_yearly_developer_totals(current_year)
    if not developer_totals:
        return None
    if analysis_year is None:
        analysis_year = current_year

    def _find_top(payload: Dict[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        slug = max(payload.keys(), key=lambda candidate: payload[candidate]["lines_added"])
        return slug, payload[slug]

    most_productive_slug, most_productive_data = _find_top(developer_totals)
    searched_years: Set[int] = set()
    if analysis_year:
        searched_years.add(analysis_year)

    while most_productive_data["lines_added"] < 1000 and analysis_year and analysis_year > 1900:
        next_year = analysis_year - 1
        if next_year in searched_years or next_year < 1900:
            break
        searched_years.add(next_year)
        if using_repo_stats:
            totals, year_candidate = _collect_repo_yearly_developer_totals(next_year, allow_fallback=False)
        else:
            totals, year_candidate = _collect_subsystem_yearly_developer_totals(next_year, allow_fallback=False)
        if not totals or year_candidate is None:
            continue
        developer_totals = totals
        analysis_year = year_candidate
        most_productive_slug, most_productive_data = _find_top(developer_totals)

    if most_productive_data["lines_added"] < 1000:
        return None

    badge = {
        "type": "productivity",
        "badge_type": "most_productive",
        "title": "🚀 Most Productive Developer",
        "subtitle": f"{most_productive_data['lines_added']:,} lines added ({analysis_year})",
        "lines_added": most_productive_data["lines_added"],
        "year": analysis_year,
        "description": (
            f"Sum of all lines added across all subsystems during {analysis_year}. "
            "Calculated by aggregating lines_added from yearly subsystem summaries."
        ),
    }
    return most_productive_slug, badge


def analyze_ownership_percentage_badges() -> Dict[str, List[Dict[str, Any]]]:
    """Create badges for developers who own more than 10% of a subsystem."""
    badges: Dict[str, List[Dict[str, Any]]] = {}

    def _process_entry(entry: Dict[str, Any], badge_type: str) -> None:
        total_lines = _safe_int(entry.get("total_lines"))
        if total_lines <= 0:
            return
        developers = entry.get("developers") or {}
        for dev_slug, dev_data in developers.items():
            dev_lines = _extract_ownership_lines(dev_data)
            if dev_lines <= 0:
                continue
            ownership_share = dev_lines / total_lines
            if ownership_share <= 0.10:
                continue
            badges.setdefault(dev_slug, []).append({
                "type": "ownership_percentage",
                "badge_type": badge_type,
                "title": f"Significant Owner: {entry['name']}",
                "subtitle": f"{ownership_share*100:.1f}% ownership ({dev_lines:,} lines)",
                "subsystem": entry["name"],
                "repo_path": entry["repo_path"],
                "lines": dev_lines,
                "share": ownership_share,
            })

    try:
        ownership_data = _get_ownership_data()
        repo_entries = ownership_data.get("repo", {})
        service_entries = ownership_data.get("service", {})
        skip_service_keys: Set[str] = set()

        for key, entry in repo_entries.items():
            service_entry = service_entries.get(key)
            if service_entry:
                repo_total = _safe_int(entry.get("total_lines"))
                service_total = _safe_int(service_entry.get("total_lines"))
                if repo_total >= service_total:
                    _process_entry(entry, "significant_owner")
                    skip_service_keys.add(key)
                continue
            _process_entry(entry, "significant_owner")

        for key, entry in service_entries.items():
            if key in skip_service_keys:
                continue
            _process_entry(entry, "significant_service_owner")

        return badges
    except Exception as exc:
        print(f"Error in analyze_ownership_percentage_badges: {exc}")
        return {}


def _extract_ownership_lines(dev_data: Any) -> int:
    if isinstance(dev_data, dict):
        return _safe_int(dev_data.get("lines"))
    return _safe_int(dev_data)


def _maybe_store_ownership_entry(bucket: Dict[str, Dict[str, Any]], key: str, entry: Dict[str, Any]) -> None:
    existing = bucket.get(key)
    if not existing or entry.get("total_lines", 0) > existing.get("total_lines", 0):
        bucket[key] = entry


def _normalize_ownership_developers(raw_devs: Any, alias_lookup: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    normalized: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_devs, dict):
        iterable = raw_devs.items()
    elif isinstance(raw_devs, list):
        iterable = []
        for entry in raw_devs:
            if not isinstance(entry, dict):
                continue
            slug_source = entry.get("slug") or entry.get("user") or entry.get("author")
            if not slug_source:
                continue
            iterable.append((slug_source, entry))
    else:
        return normalized

    for raw_slug, payload in iterable:
        slug_candidate = str(raw_slug).strip()
        if not slug_candidate:
            continue
        resolved_slug = (
            resolve_user_slug(slug_candidate)
            or canonicalize_slug(slugify_identifier(slug_candidate), alias_lookup)
            or slugify_identifier(slug_candidate)
            or slug_candidate
        )
        lines = _extract_ownership_lines(payload)
        if lines <= 0:
            continue
        entry = normalized.setdefault(resolved_slug, {"lines": 0})
        entry["lines"] += lines
    return normalized


def _find_latest_blame_snapshot(blame_dir: str) -> Optional[str]:
    if not os.path.isdir(blame_dir):
        return None
    latest_candidate = os.path.join(blame_dir, "latest.json")
    if os.path.isfile(latest_candidate):
        return latest_candidate
    newest_path: Optional[str] = None
    newest_mtime = -1.0
    for root, _dirs, files in os.walk(blame_dir):
        for name in files:
            if not name.endswith(".json"):
                continue
            path = os.path.join(root, name)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if mtime > newest_mtime:
                newest_mtime = mtime
                newest_path = path
    return newest_path


def _build_ownership_data(stats_root: Optional[str] = None) -> Dict[str, Dict[str, Dict[str, Any]]]:
    repo_entries: Dict[str, Dict[str, Any]] = {}
    service_entries: Dict[str, Dict[str, Any]] = {}
    stats_root = os.path.abspath(stats_root or STATS_ROOT)
    repos_path = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_path):
        return {"repo": repo_entries, "service": service_entries}

    alias_lookup = load_alias_lookup()

    for vendor in os.listdir(repos_path):
        vendor_dir = os.path.join(repos_path, vendor)
        if not os.path.isdir(vendor_dir):
            continue
        for repo_name in os.listdir(vendor_dir):
            repo_dir = os.path.join(vendor_dir, repo_name)
            if not os.path.isdir(repo_dir):
                continue
            blame_dir = os.path.join(repo_dir, "blame")
            blame_file = _find_latest_blame_snapshot(blame_dir)
            if not blame_file:
                continue
            try:
                blame_data = load_json(blame_file)
            except Exception:
                continue

            repo_field = str(blame_data.get("repo") or "").strip()
            repo_name_only = repo_field.split("/")[-1] if repo_field else repo_name
            repo_path_label = repo_field or f"{vendor}/{repo_name}" if vendor else repo_name

            repo_devs = _normalize_ownership_developers(blame_data.get("developers"), alias_lookup)
            if repo_devs:
                total_lines = _safe_int(blame_data.get("total_lines"))
                if total_lines <= 0:
                    total_lines = sum(_safe_int(dev.get("lines")) for dev in repo_devs.values())
                entry = {
                    "name": repo_name_only,
                    "repo_path": repo_path_label,
                    "developers": repo_devs,
                    "total_lines": total_lines,
                }
                _maybe_store_ownership_entry(repo_entries, repo_name_only.lower(), entry)

            services = blame_data.get("services")
            if not isinstance(services, dict):
                continue
            for service_name, service_data in services.items():
                if not isinstance(service_data, dict):
                    continue
                service_devs = _normalize_ownership_developers(service_data.get("developers"), alias_lookup)
                if not service_devs:
                    continue
                total_lines = _safe_int(service_data.get("total_lines"))
                if total_lines <= 0:
                    total_lines = sum(_safe_int(dev.get("lines")) for dev in service_devs.values())
                entry = {
                    "name": service_name,
                    "repo_path": repo_path_label,
                    "developers": service_devs,
                    "total_lines": total_lines,
                }
                _maybe_store_ownership_entry(service_entries, service_name.lower(), entry)

    return {"repo": repo_entries, "service": service_entries}


def _get_ownership_data(
    stats_root: Optional[str] = None,
    force_refresh: bool = False,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    global _OWNERSHIP_DATA_CACHE
    root = os.path.abspath(stats_root or STATS_ROOT)
    if force_refresh or root not in _OWNERSHIP_DATA_CACHE:
        _OWNERSHIP_DATA_CACHE[root] = _build_ownership_data(root)
    return _OWNERSHIP_DATA_CACHE[root]


def _get_ownership_entry(subsystem_name: str) -> Optional[Dict[str, Any]]:
    if not subsystem_name:
        return None
    ownership_data = _get_ownership_data()
    target = subsystem_name.lower()
    entry = ownership_data.get("service", {}).get(target)
    if entry:
        return entry
    entry = ownership_data.get("repo", {}).get(target)
    if entry:
        return entry
    for bucket in ("service", "repo"):
        for candidate in ownership_data.get(bucket, {}).values():
            if target in (candidate.get("name") or "").lower():
                return candidate
    return None


def build_ownership_distribution_snapshot(
    stats_root: Optional[str] = None,
    threshold: float = 0.10,
) -> Dict[str, Any]:
    root = os.path.abspath(stats_root or STATS_ROOT)
    ownership_data = _get_ownership_data(root) or {}
    owners: Dict[str, Dict[str, Any]] = {}
    covered_subsystems: Set[str] = set()
    total_subsystems: Set[str] = set()
    total_relationships = 0

    for bucket_name in ("service", "repo"):
        bucket = ownership_data.get(bucket_name, {}) or {}
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            subsystem_name = entry.get("name") or entry.get("repo_path")
            if not subsystem_name:
                continue
            total_subsystems.add(subsystem_name)
            developers = entry.get("developers") or {}
            total_lines = _safe_int(entry.get("total_lines"))
            if total_lines <= 0:
                total_lines = sum(_extract_ownership_lines(dev) for dev in developers.values())
            if total_lines <= 0:
                continue
            subsystem_had_owner = False
            for slug, payload in developers.items():
                lines = _extract_ownership_lines(payload)
                if lines <= 0:
                    continue
                share = lines / total_lines if total_lines else 0
                if share <= threshold:
                    continue
                if isinstance(payload, dict):
                    display_name = payload.get("display_name") or slug
                else:
                    display_name = slug

                owner_entry = owners.setdefault(
                    slug,
                    {
                        "slug": slug,
                        "display_name": display_name,
                        "ownerships": [],
                        "total_percentage": 0.0,
                        "total_lines": 0,
                        "ownership_count": 0,
                    },
                )
                owner_entry["display_name"] = owner_entry.get("display_name") or display_name
                owner_entry["ownerships"].append(
                    {
                        "subsystem": subsystem_name,
                        "lines": lines,
                        "percentage": round(share * 100, 1),
                        "share": share,
                        "repo_path": entry.get("repo_path"),
                        "source": bucket_name,
                    }
                )
                owner_entry["total_percentage"] += share * 100
                owner_entry["total_lines"] += lines
                owner_entry["ownership_count"] += 1
                total_relationships += 1
                subsystem_had_owner = True
            if subsystem_had_owner:
                covered_subsystems.add(subsystem_name)

    owner_list: List[Dict[str, Any]] = []
    for owner in owners.values():
        owner["ownerships"].sort(key=lambda item: item["percentage"], reverse=True)
        owner["total_percentage"] = round(owner["total_percentage"], 1)
        owner_list.append(owner)

    owner_list.sort(
        key=lambda item: (item.get("ownership_count", 0), item.get("total_percentage", 0)),
        reverse=True,
    )

    totals = {
        "threshold": threshold,
        "users_with_ownership": len(owner_list),
        "total_ownerships": total_relationships,
        "covered_subsystems": len(covered_subsystems),
        "total_subsystems": len(total_subsystems) or len(covered_subsystems),
        "avg_per_owner": (total_relationships / len(owner_list)) if owner_list else 0.0,
    }

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "owners": owner_list,
        "totals": totals,
    }


def _find_top_developer(entry: Dict[str, Any]) -> Tuple[Optional[str], int, float]:
    developers = entry.get("developers") or {}
    total_lines = entry.get("total_lines") or 0
    best_slug: Optional[str] = None
    best_lines = 0
    for slug, payload in developers.items():
        lines = _extract_ownership_lines(payload)
        if lines > best_lines:
            best_slug = slug
            best_lines = lines
    share = (best_lines / total_lines) if total_lines else 0.0
    return best_slug, best_lines, share


def _merge_repo_stats(
    repo_map: Dict[str, Dict[str, int]],
    key: Optional[str],
    commits: int = 0,
    additions: int = 0,
    deletions: int = 0,
    files_changed: Optional[int] = None,
) -> None:
    if not key:
        return
    bucket = repo_map.setdefault(
        key,
        {
            "commits": 0,
            "additions": 0,
            "deletions": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
        },
    )
    bucket["commits"] += commits
    bucket["additions"] += additions
    bucket["lines_added"] += additions
    bucket["deletions"] += deletions
    bucket["lines_deleted"] += deletions
    bucket["net_lines"] += additions - deletions
    if files_changed is not None:
        bucket["files_changed"] = bucket.get("files_changed", 0) + files_changed


def _merge_repo_map(target: Dict[str, Dict[str, int]], source: Dict[str, Any]) -> None:
    for repo_name, stats in (source or {}).items():
        if not isinstance(stats, dict):
            continue
        files_changed = stats.get("files_changed")
        _merge_repo_stats(
            target,
            repo_name,
            _safe_int(stats.get("commits")),
            _safe_int(stats.get("additions") or stats.get("lines_added")),
            _safe_int(stats.get("deletions") or stats.get("lines_deleted")),
            _safe_int(files_changed) if files_changed is not None else None,
        )


def _merge_daily_entry(target: Dict[str, Dict[str, int]], date_str: str, stats: Dict[str, Any]) -> None:
    if not date_str or not isinstance(stats, dict):
        return
    commits = _safe_int(stats.get("commits"))
    additions = _safe_int(stats.get("additions") or stats.get("lines_added"))
    deletions = _safe_int(stats.get("deletions") or stats.get("lines_deleted"))
    entry = target.setdefault(
        date_str,
        {
            "commits": 0,
            "additions": 0,
            "deletions": 0,
            "net_lines": 0,
        },
    )
    entry["commits"] += commits
    entry["additions"] += additions
    entry["deletions"] += deletions
    entry["net_lines"] += additions - deletions


def _merge_daily_map(target: Dict[str, Dict[str, int]], source: Dict[str, Any]) -> None:
    for date_str, stats in (source or {}).items():
        _merge_daily_entry(target, date_str, stats if isinstance(stats, dict) else {})


def _merge_hour_entry(target: Dict[str, Dict[str, int]], hour_key: Any, stats: Dict[str, Any]) -> None:
    if not isinstance(stats, dict):
        return
    try:
        hour_int = int(hour_key)
    except (TypeError, ValueError):
        return
    if hour_int < 0 or hour_int > 23:
        return
    normalized_key = f"{hour_int:02d}"
    bucket = target.setdefault(
        normalized_key,
        {
            "commits": 0,
            "additions": 0,
            "deletions": 0,
            "net_lines": 0,
        },
    )
    additions = _safe_int(stats.get("additions") or stats.get("lines_added"))
    deletions = _safe_int(stats.get("deletions") or stats.get("lines_deleted"))
    bucket["commits"] += _safe_int(stats.get("commits"))
    bucket["additions"] += additions
    bucket["deletions"] += deletions
    bucket["net_lines"] += additions - deletions


def _merge_hour_map(target: Dict[str, Dict[str, int]], source: Dict[str, Any]) -> None:
    for hour_key, stats in (source or {}).items():
        _merge_hour_entry(target, hour_key, stats if isinstance(stats, dict) else {})


def _compute_weekday_breakdown(per_date: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    weekday_map: Dict[str, Dict[str, int]] = {}
    for date_str, payload in per_date.items():
        if not isinstance(payload, dict):
            continue
        try:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        weekday_name = calendar.day_name[parsed_date.weekday()]
        bucket = weekday_map.setdefault(
            weekday_name,
            {"commits": 0, "additions": 0, "deletions": 0},
        )
        bucket["commits"] += _safe_int(payload.get("commits"))
        bucket["additions"] += _safe_int(payload.get("additions"))
        bucket["deletions"] += _safe_int(payload.get("deletions"))
    return weekday_map


def _calculate_commits_per_week(summary: Dict[str, Any]) -> float:
    total_commits = _safe_int(summary.get("total_commits"))

    def _parse_date(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    period = summary.get("period") if isinstance(summary.get("period"), dict) else {}
    from_date = summary.get("from") or period.get("from")
    to_date = summary.get("to") or period.get("to")

    year = _safe_int(summary.get("year"))
    month = _safe_int(summary.get("month"))
    is_yearly = bool(summary.get("is_yearly"))

    if year and (is_yearly or not month):
        from_date = from_date or f"{year:04d}-01-01"
        to_date = to_date or f"{year:04d}-12-31"
    elif year and month:
        last_day = calendar.monthrange(year, month)[1]
        from_date = from_date or f"{year:04d}-{month:02d}-01"
        to_date = to_date or f"{year:04d}-{month:02d}-{last_day:02d}"

    if not from_date or not to_date:
        per_date = summary.get("per_date")
        if isinstance(per_date, dict) and per_date:
            valid_dates: List[str] = []
            for date_str in per_date.keys():
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                    valid_dates.append(date_str)
                except (ValueError, TypeError):
                    continue
            if valid_dates:
                valid_dates.sort()
                from_date = from_date or valid_dates[0]
                to_date = to_date or valid_dates[-1]

    start = _parse_date(from_date)
    end = _parse_date(to_date)
    if not start or not end:
        return float(total_commits)

    day_span = max(1, (end - start).days + 1)
    weeks = day_span / 7.0
    if weeks <= 0:
        return float(total_commits)
    return float(total_commits) / weeks


USER_METRIC_FIELDS: Dict[str, Dict[str, Any]] = {
    "total_commits": {},
    "total_lines_added": {"fallback": ["total_additions", "lines_added"]},
    "total_lines_deleted": {"fallback": ["total_deletions", "lines_deleted", "lines_removed"]},
    "net_lines": {"fallback": ["lines_net"]},
    "commits_per_week": {"compute": _calculate_commits_per_week},
}


def _extract_metric_value(summary: Dict[str, Any], metric: str, config: Dict[str, Any]) -> float:
    compute_fn = config.get("compute")
    if callable(compute_fn):
        try:
            computed = compute_fn(summary)
        except Exception:
            computed = None
        return float(computed or 0.0)

    value = summary.get(metric)
    if value is None:
        for fallback_key in config.get("fallback", []):
            fallback_value = summary.get(fallback_key)
            if fallback_value is not None:
                value = fallback_value
                break
        if value is None:
            nested = summary.get("summary")
            if isinstance(nested, dict):
                totals_block = nested.get("totals") if isinstance(nested.get("totals"), dict) else None
                for block in (nested, totals_block):
                    if not isinstance(block, dict):
                        continue
                    if block.get(metric) is not None:
                        value = block.get(metric)
                        break
                    for fallback_key in config.get("fallback", []):
                        if block.get(fallback_key) is not None:
                            value = block.get(fallback_key)
                            break
                    if value is not None:
                        break
    return float(_safe_int(value))


def _load_user_month_rows(from_date: str, to_date: str) -> List[Tuple[str, Dict[str, Any]]]:
    users_root = os.path.join(STATS_ROOT, "users")
    rows: List[Tuple[str, Dict[str, Any]]] = []
    if not os.path.isdir(users_root):
        return rows
    for user_slug in os.listdir(users_root):
        slug_dir = os.path.join(users_root, user_slug)
        if not os.path.isdir(slug_dir):
            continue
        summary_path = find_user_summary(user_slug, from_date, to_date)
        if not summary_path or not os.path.isfile(summary_path):
            continue
        try:
            rows.append((user_slug, load_user_summary_file(summary_path, augment=True)))
        except Exception:
            continue
    return rows


def _resolve_user_year_summary_path(user_dir: str, year: int) -> Optional[str]:
    candidates = [
        os.path.join(user_dir, f"{year:04d}", "yearly.json"),
        os.path.join(user_dir, str(year), "yearly.json"),
        os.path.join(user_dir, "year", f"{year}.json"),
        os.path.join(user_dir, f"{year}.json"),
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return candidate
    return None


def _load_user_year_rows(year: int) -> List[Tuple[str, Dict[str, Any]]]:
    users_root = os.path.join(STATS_ROOT, "users")
    rows: List[Tuple[str, Dict[str, Any]]] = []
    if not os.path.isdir(users_root):
        return rows
    for user_slug in os.listdir(users_root):
        user_dir = os.path.join(users_root, user_slug)
        if not os.path.isdir(user_dir):
            continue
        summary_path = _resolve_user_year_summary_path(user_dir, year)
        if not summary_path:
            continue
        try:
            rows.append((user_slug, load_user_summary_file(summary_path, augment=True)))
        except Exception:
            continue
    return rows


def _build_peer_rankings(
    rows: List[Tuple[str, Dict[str, Any]]],
    target_slug: str,
    metrics_for_distribution: Optional[Set[str]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    total = len(rows)
    if total == 0:
        return {}, {}
    metrics_for_distribution = metrics_for_distribution or set()
    rows_map = {slug: summary for slug, summary in rows}
    if target_slug not in rows_map:
        return {}, {}
    rankings: Dict[str, Dict[str, Any]] = {}
    distributions: Dict[str, List[Dict[str, Any]]] = {}
    for metric, config in USER_METRIC_FIELDS.items():
        metric_values = [(slug, _extract_metric_value(summary, metric, config)) for slug, summary in rows]
        metric_values.sort(key=lambda item: item[1], reverse=True)
        prev_value: Optional[float] = None
        current_rank = 0
        target_info: Optional[Dict[str, Any]] = None
        for index, (slug, value) in enumerate(metric_values, start=1):
            if prev_value is None or value != prev_value:
                current_rank = index
                prev_value = value
            if slug == target_slug:
                percentile = 100.0 if total == 0 else round((current_rank / total) * 100, 1)
                target_info = {
                    "rank": current_rank,
                    "value": value,
                    "total": total,
                    "percentile": percentile,
                }
                break
        if target_info:
            rankings[metric] = target_info
        if metric in metrics_for_distribution:
            distributions[metric] = [
                {"slug": slug, "value": value}
                for slug, value in metric_values
            ]
    return rankings, distributions


def compute_user_month_peer_rankings(user_slug: str, from_date: str, to_date: str) -> Dict[str, Dict[str, Any]]:
    rows = _load_user_month_rows(from_date, to_date)
    rankings, _ = _build_peer_rankings(rows, user_slug)
    return rankings


def _get_subsystem_touch_counts(year: int) -> Dict[str, int]:
    cached = _SUBSYSTEM_TOUCH_COUNT_CACHE.get(year)
    if cached is not None:
        return cached
    users_root = os.path.join(STATS_ROOT, "users")
    counts: Dict[str, int] = {}
    if os.path.isdir(users_root):
        for user_slug in os.listdir(users_root):
            user_dir = os.path.join(users_root, user_slug)
            if not os.path.isdir(user_dir):
                continue
            summary_path = _resolve_user_year_summary_path(user_dir, year)
            if not summary_path or not os.path.isfile(summary_path):
                continue
            data = load_json(summary_path, default={})
            summary_block = data.get("summary") if isinstance(data.get("summary"), dict) else None
            value = None
            if summary_block:
                totals_block = summary_block.get("totals") if isinstance(summary_block.get("totals"), dict) else None
                if totals_block and totals_block.get("subsystems_touched") is not None:
                    value = totals_block.get("subsystems_touched")
                elif summary_block.get("subsystems_touched") is not None:
                    value = summary_block.get("subsystems_touched")
            if value is None:
                value = data.get("subsystems_touched")
            counts[user_slug] = _safe_int(value)
    _SUBSYSTEM_TOUCH_COUNT_CACHE[year] = counts
    return counts


def get_subsystem_touch_rank(
    year: int,
    user_slug: str,
    population_slugs: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    counts = _get_subsystem_touch_counts(year)
    population = list(dict.fromkeys(population_slugs)) if population_slugs else list(counts.keys())
    if user_slug not in population:
        population.append(user_slug)
    total = len(population)
    if total == 0:
        return None
    values: List[Tuple[str, int]] = [(slug, counts.get(slug, 0)) for slug in population]
    values.sort(key=lambda item: item[1], reverse=True)
    prev_value: Optional[int] = None
    current_rank = 0
    for index, (slug, value) in enumerate(values, start=1):
        if prev_value is None or value != prev_value:
            current_rank = index
            prev_value = value
        if slug == user_slug:
            percentile = 100.0 if total == 0 else round((current_rank / total) * 100, 1)
            return {
                "rank": current_rank,
                "value": value,
                "total": total,
                "percentile": percentile,
            }
    return None


def compute_user_year_peer_rankings(
    user_slug: str,
    year: int,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    rows = _load_user_year_rows(year)
    rankings, distributions = _build_peer_rankings(rows, user_slug, {"total_commits"})
    population_slugs = [slug for slug, _ in rows]
    subsystem_rank = get_subsystem_touch_rank(year, user_slug, population_slugs or None)
    if subsystem_rank:
        rankings["subsystems_touched"] = subsystem_rank
    return rankings, distributions


def _collect_user_month_payloads(user_slug: str, year: int) -> Dict[str, Dict[str, Any]]:
    months: Dict[str, Dict[str, Any]] = {}
    user_dir = os.path.join(STATS_ROOT, "users", user_slug)
    if not os.path.isdir(user_dir):
        return months

    def _store_month(month_key: str, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        key = str(month_key).zfill(2)
        months[key] = _normalize_user_summary_payload(copy.deepcopy(payload), allow_nested=False)

    year_dirs = [os.path.join(user_dir, f"{year:04d}"), os.path.join(user_dir, str(year))]
    for year_dir in year_dirs:
        if not os.path.isdir(year_dir):
            continue
        yearly_file = os.path.join(year_dir, "yearly.json")
        if os.path.isfile(yearly_file):
            yearly_data = load_json(yearly_file, default={})
            months_block = yearly_data.get("months")
            if isinstance(months_block, dict):
                for month_key, payload in months_block.items():
                    _store_month(month_key, payload)
        for filename in os.listdir(year_dir):
            if filename == "yearly.json" or not filename.endswith(".json"):
                continue
            month_key = filename[:-5]
            if not month_key.isdigit():
                continue
            month_path = os.path.join(year_dir, filename)
            months[month_key.zfill(2)] = load_user_summary_file(month_path, augment=False)
        if months:
            return months

    legacy_dir = os.path.join(user_dir, "year")
    legacy_file = os.path.join(legacy_dir, f"{year}.json")
    if os.path.isfile(legacy_file):
        legacy_data = load_user_summary_file(legacy_file, augment=True)
        months_block = legacy_data.get("months")
        if isinstance(months_block, dict):
            for month_key, payload in months_block.items():
                _store_month(month_key, payload)
    return months


def build_user_subsystem_activity(user_slug: str, year: int) -> Dict[str, Any]:
    month_payloads = _collect_user_month_payloads(user_slug, year)

    def _empty_month_entry(month: int) -> Dict[str, Any]:
        label = f"{year:04d}-{month:02d}"
        last_day = calendar.monthrange(year, month)[1]
        display_label = f"{MONTH_ABBREVIATIONS[month - 1]} {year}" if 1 <= month <= len(MONTH_ABBREVIATIONS) else label
        return {
            "month": label,
            "label": label,
            "display_label": display_label,
            "short_label": MONTH_ABBREVIATIONS[month - 1] if 1 <= month <= len(MONTH_ABBREVIATIONS) else label,
            "from": f"{year:04d}-{month:02d}-01",
            "to": f"{year:04d}-{month:02d}-{last_day:02d}",
            "total_commits": 0,
            "total_lines_added": 0,
            "total_lines_deleted": 0,
            "total_changed_lines": 0,
            "subsystems": [],
            "has_activity": False,
            "other_subsystems_count": 0,
            "dominant_subsystem": None,
        }

    timeline: List[Dict[str, Any]] = []
    subsystem_totals: Dict[Tuple[str, str], Dict[str, Any]] = {}
    total_changed_lines_year = 0
    total_commits_year = 0
    months_active = 0

    for month in range(1, 13):
        entry = _empty_month_entry(month)
        payload = month_payloads.get(f"{month:02d}") or month_payloads.get(str(month))
        if payload:
            lines_added = _safe_int(payload.get("total_lines_added"))
            lines_deleted = _safe_int(payload.get("total_lines_deleted"))
            changed_lines = _safe_int(payload.get("total_changed_lines"))
            if not changed_lines and (lines_added or lines_deleted):
                changed_lines = lines_added + lines_deleted
            commits = _safe_int(payload.get("total_commits"))
            entry["total_lines_added"] = lines_added
            entry["total_lines_deleted"] = lines_deleted
            entry["total_changed_lines"] = changed_lines
            entry["total_commits"] = commits
            entry["has_activity"] = bool(commits or changed_lines)
            subsystem_sources: List[List[Dict[str, Any]]] = []
            if isinstance(payload.get("subsystems"), list):
                subsystem_sources.append(payload.get("subsystems"))
            summary_block = payload.get("summary") if isinstance(payload.get("summary"), dict) else None
            if summary_block and isinstance(summary_block.get("subsystems"), list):
                subsystem_sources.append(summary_block.get("subsystems"))
            seen: Set[Tuple[str, str]] = set()
            subsystems_list: List[Dict[str, Any]] = []
            for source in subsystem_sources:
                for raw in source:
                    if not isinstance(raw, dict):
                        continue
                    name = raw.get("subsystem") or raw.get("name") or raw.get("repo")
                    if not name:
                        continue
                    repo = raw.get("repo") or ""
                    key = (name, repo)
                    if key in seen:
                        continue
                    seen.add(key)
                    commits_sub = _safe_int(raw.get("commits"))
                    lines_added_sub = _safe_int(raw.get("lines_added") or raw.get("additions"))
                    lines_deleted_sub = _safe_int(raw.get("lines_removed") or raw.get("lines_deleted") or raw.get("deletions"))
                    changed_lines_sub = raw.get("changed_lines")
                    if changed_lines_sub is None:
                        changed_lines_sub = lines_added_sub + lines_deleted_sub
                    net_lines_sub = raw.get("net_lines")
                    if net_lines_sub is None:
                        net_lines_sub = lines_added_sub - lines_deleted_sub
                    if not any((commits_sub, lines_added_sub, lines_deleted_sub, changed_lines_sub)):
                        continue
                    entry_data = {
                        "name": name,
                        "repo": raw.get("repo"),
                        "commits": commits_sub,
                        "lines_added": lines_added_sub,
                        "lines_deleted": lines_deleted_sub,
                        "changed_lines": _safe_int(changed_lines_sub),
                        "net_lines": _safe_int(net_lines_sub),
                    }
                    subsystems_list.append(entry_data)
                    totals_entry = subsystem_totals.setdefault(
                        (name, repo),
                        {
                            "name": name,
                            "repo": raw.get("repo"),
                            "commits": 0,
                            "lines_added": 0,
                            "lines_deleted": 0,
                            "changed_lines": 0,
                            "net_lines": 0,
                            "months": set(),
                        },
                    )
                    totals_entry["commits"] += commits_sub
                    totals_entry["lines_added"] += lines_added_sub
                    totals_entry["lines_deleted"] += lines_deleted_sub
                    totals_entry["changed_lines"] += _safe_int(changed_lines_sub)
                    totals_entry["net_lines"] += _safe_int(net_lines_sub)
                    totals_entry["months"].add(f"{month:02d}")
            if subsystems_list:
                subsystems_list.sort(key=lambda item: item.get("changed_lines", 0), reverse=True)
                entry["subsystems"] = subsystems_list
                entry["other_subsystems_count"] = max(0, len(subsystems_list) - 1)
                dominant = subsystems_list[0]
                line_sum = sum(sub.get("changed_lines", 0) for sub in subsystems_list)
                if entry["total_changed_lines"] <= 0 and line_sum:
                    entry["total_changed_lines"] = line_sum
                total_lines = entry["total_changed_lines"]
                if line_sum:
                    total_lines = max(total_lines, line_sum)
                if total_lines <= 0:
                    total_lines = dominant.get("changed_lines", 0)
                share = 0.0
                if total_lines > 0:
                    share = round((dominant.get("changed_lines", 0) / total_lines) * 100, 1)
                entry["dominant_subsystem"] = {**dominant, "share_percent": share}
            if entry["has_activity"]:
                months_active += 1
        total_changed_lines_year += entry["total_changed_lines"]
        total_commits_year += entry["total_commits"]
        timeline.append(entry)

    top_subsystems: List[Dict[str, Any]] = []
    for stats in subsystem_totals.values():
        months_field = stats.pop("months", set())
        stats["months_active"] = len(months_field)
        top_subsystems.append(stats)
    top_subsystems.sort(key=lambda item: item.get("changed_lines", 0), reverse=True)

    summary = {
        "months_active": months_active,
        "subsystems_touched": len(subsystem_totals),
        "total_changed_lines": total_changed_lines_year,
        "total_commits": total_commits_year,
        "top_subsystems": top_subsystems[:5],
        "most_active_subsystem": top_subsystems[0] if top_subsystems else None,
        "has_activity": any(entry["has_activity"] for entry in timeline),
    }

    subsystem_rank = get_subsystem_touch_rank(year, user_slug)
    if subsystem_rank:
        summary["peer_rankings"] = {"subsystems_touched": subsystem_rank}

    return {
        "user": user_slug,
        "year": year,
        "timeline": timeline,
        "summary": summary,
    }


def _normalize_user_summary_payload(data: Dict[str, Any], allow_nested: bool = True) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    summary_block = data.get("summary") if isinstance(data.get("summary"), dict) else None
    totals_block = None
    if summary_block and isinstance(summary_block.get("totals"), dict):
        totals_block = summary_block.get("totals")

    def pick_stat(keys: List[str], default: int = 0) -> int:
        for block in (totals_block, summary_block, data):
            if not isinstance(block, dict):
                continue
            for key in keys:
                if block.get(key) is not None:
                    return _safe_int(block.get(key))
        return default

    commits = pick_stat(["commits", "total_commits"])
    additions = pick_stat(["lines_added", "total_lines_added", "additions", "total_additions"])
    deletions = pick_stat(["lines_removed", "lines_deleted", "total_lines_deleted", "deletions", "total_deletions"])
    net_lines = pick_stat(["net_lines", "lines_net", "total_net_lines"])
    changed_lines = pick_stat(["total_changed_lines", "lines_changed"])

    if not net_lines and (additions or deletions):
        net_lines = additions - deletions
    if not changed_lines and (additions or deletions):
        changed_lines = additions + deletions

    data["total_commits"] = commits
    data["total_lines_added"] = additions
    data["total_lines_deleted"] = deletions
    data["net_lines"] = net_lines
    data["total_changed_lines"] = changed_lines
    data.setdefault("commits", commits)
    data.setdefault("lines_added", additions)
    data.setdefault("lines_deleted", deletions)
    data.setdefault("lines_net", net_lines)

    if not data.get("author_name"):
        for candidate in (
            data.get("user"),
            data.get("author"),
            summary_block.get("user") if summary_block else None,
            summary_block.get("author") if summary_block else None,
        ):
            if candidate:
                data["author_name"] = candidate
                break

    per_repo: Dict[str, Dict[str, int]] = data.get("per_repo") if isinstance(data.get("per_repo"), dict) else {}
    if not isinstance(per_repo, dict):
        per_repo = {}

    subsystem_sources: List[List[Dict[str, Any]]] = []
    if summary_block and isinstance(summary_block.get("subsystems"), list):
        subsystem_sources.append(summary_block.get("subsystems"))
    if isinstance(data.get("subsystems"), list):
        subsystem_sources.append(data.get("subsystems"))

    for source in subsystem_sources:
        for entry in source:
            if not isinstance(entry, dict):
                continue
            key = entry.get("subsystem") or entry.get("repo") or entry.get("name")
            commits_val = _safe_int(entry.get("commits"))
            additions_val = _safe_int(entry.get("lines_added") or entry.get("additions"))
            deletions_val = _safe_int(entry.get("lines_removed") or entry.get("lines_deleted") or entry.get("deletions"))
            files_changed_val = entry.get("files_changed")
            if not any((commits_val, additions_val, deletions_val, files_changed_val)):
                continue
            _merge_repo_stats(
                per_repo,
                key,
                commits_val,
                additions_val,
                deletions_val,
                _safe_int(files_changed_val) if files_changed_val is not None else None,
            )

    months = data.get("months") if isinstance(data.get("months"), dict) else None
    aggregated_repos: Dict[str, Dict[str, int]] = {}
    aggregated_dates: Dict[str, Dict[str, int]] = {}
    aggregated_hours: Dict[str, Dict[str, int]] = {}
    aggregated_commits = 0
    aggregated_additions = 0
    aggregated_deletions = 0

    if allow_nested and months:
        for month_data in months.values():
            if not isinstance(month_data, dict):
                continue
            _normalize_user_summary_payload(month_data, allow_nested=False)
            aggregated_commits += _safe_int(month_data.get("total_commits"))
            aggregated_additions += _safe_int(month_data.get("total_lines_added"))
            aggregated_deletions += _safe_int(month_data.get("total_lines_deleted"))
            _merge_repo_map(aggregated_repos, month_data.get("per_repo") or {})
            _merge_daily_map(aggregated_dates, month_data.get("per_date") or {})
            _merge_hour_map(aggregated_hours, month_data.get("per_hour") or {})

        if aggregated_repos:
            _merge_repo_map(per_repo, aggregated_repos)
        if aggregated_dates:
            existing_dates = data.get("per_date") if isinstance(data.get("per_date"), dict) else {}
            if not existing_dates:
                data["per_date"] = aggregated_dates
            else:
                _merge_daily_map(existing_dates, aggregated_dates)
                data["per_date"] = existing_dates
        if aggregated_hours:
            existing_hours = data.get("per_hour") if isinstance(data.get("per_hour"), dict) else {}
            if not existing_hours:
                data["per_hour"] = aggregated_hours
            else:
                _merge_hour_map(existing_hours, aggregated_hours)
                data["per_hour"] = existing_hours
        if aggregated_commits:
            data["total_commits"] = aggregated_commits
        if aggregated_additions:
            data["total_lines_added"] = aggregated_additions
        if aggregated_deletions:
            data["total_lines_deleted"] = aggregated_deletions
        if aggregated_additions or aggregated_deletions:
            data["net_lines"] = data.get("total_lines_added", 0) - data.get("total_lines_deleted", 0)
            data["total_changed_lines"] = data.get("total_lines_added", 0) + data.get("total_lines_deleted", 0)

    if per_repo:
        data["per_repo"] = per_repo
    else:
        data.pop("per_repo", None)

    per_date = data.get("per_date") if isinstance(data.get("per_date"), dict) else {}
    if per_date:
        normalized_dates: Dict[str, Dict[str, int]] = {}
        for date_str, stats in per_date.items():
            if not isinstance(stats, dict):
                continue
            additions_val = _safe_int(stats.get("additions") or stats.get("lines_added"))
            deletions_val = _safe_int(stats.get("deletions") or stats.get("lines_deleted"))
            normalized_dates[date_str] = {
                "commits": _safe_int(stats.get("commits")),
                "additions": additions_val,
                "deletions": deletions_val,
                "net_lines": additions_val - deletions_val,
            }
        data["per_date"] = normalized_dates
    else:
        data["per_date"] = {}

    if data["per_date"]:
        data["per_weekday"] = _compute_weekday_breakdown(data["per_date"])
    else:
        data.setdefault("per_weekday", {})

    per_hour = data.get("per_hour") if isinstance(data.get("per_hour"), dict) else {}
    if per_hour:
        normalized_hours: Dict[str, Dict[str, int]] = {}
        for hour_key, stats in per_hour.items():
            _merge_hour_entry(normalized_hours, hour_key, stats if isinstance(stats, dict) else {})
        data["per_hour"] = normalized_hours
    else:
        data["per_hour"] = {}

    return data


def _extract_language_totals_from_payload(payload: Dict[str, Any]) -> Dict[str, int]:
    languages = payload.get("languages")
    normalized: Dict[str, int] = {}
    if isinstance(languages, list):
        for entry in languages:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name") or entry.get("language")
            if not isinstance(name, str) or not name:
                continue
            value = entry.get("code_lines")
            if value is None:
                value = entry.get("code")
            if value is None:
                value = entry.get("lines")
            if value is None:
                value = entry.get("net_lines")
            amount = max(0, _safe_int(value))
            if amount:
                normalized[name] = normalized.get(name, 0) + amount
    elif isinstance(languages, dict):
        for name, stats in languages.items():
            if not isinstance(name, str) or not name:
                continue
            amount = 0
            if isinstance(stats, dict):
                for key in ("code_lines", "code", "lines", "net_lines"):
                    if stats.get(key) is not None:
                        amount = max(0, _safe_int(stats.get(key)))
                        if amount:
                            break
            else:
                amount = max(0, _safe_int(stats))
            if amount:
                normalized[name] = normalized.get(name, 0) + amount
    return normalized


def _find_latest_language_snapshot(languages_dir: str) -> Optional[str]:
    if not os.path.isdir(languages_dir):
        return None
    year_dirs = sorted([name for name in os.listdir(languages_dir) if name.isdigit()], reverse=True)
    for year in year_dirs:
        year_path = os.path.join(languages_dir, year)
        if not os.path.isdir(year_path):
            continue
        month_files = sorted(
            [name for name in os.listdir(year_path) if name.endswith(".json") and name[:-5].isdigit()],
            reverse=True,
        )
        for filename in month_files:
            path = os.path.join(year_path, filename)
            if os.path.isfile(path):
                return path
        yearly_path = os.path.join(year_path, "yearly.json")
        if os.path.isfile(yearly_path):
            return yearly_path
    fallback = os.path.join(languages_dir, "languages.json")
    if os.path.isfile(fallback):
        return fallback
    json_files = sorted([name for name in os.listdir(languages_dir) if name.endswith(".json")], reverse=True)
    for filename in json_files:
        path = os.path.join(languages_dir, filename)
        if os.path.isfile(path):
            return path
    return None


def _load_language_snapshot_from_file(path: str, target_month: Optional[int]) -> Optional[Dict[str, int]]:
    payload = load_json(path, default={})
    if target_month is not None:
        months_block = payload.get("months")
        if isinstance(months_block, dict):
            month_key_candidates = {str(int(target_month))}
            month_key_candidates.add(f"{int(target_month):02d}")
            for key in month_key_candidates:
                month_payload = months_block.get(key)
                if isinstance(month_payload, dict):
                    snapshot = _extract_language_totals_from_payload(month_payload)
                    if snapshot:
                        return snapshot
    snapshot = _extract_language_totals_from_payload(payload)
    return snapshot or None


def _load_repo_language_snapshot(repo_full_name: Optional[str], year: Optional[int], month: Optional[int]) -> Optional[Dict[str, int]]:
    if not repo_full_name:
        return None
    repo_norm = repo_full_name.replace("\\", "/").strip("/")
    if not repo_norm:
        return None
    cache_key = (repo_norm, year, month)
    if cache_key in _REPO_LANGUAGE_SNAPSHOT_CACHE:
        return _REPO_LANGUAGE_SNAPSHOT_CACHE[cache_key]

    repo_parts = repo_norm.split("/")
    repo_path = os.path.join(STATS_ROOT, "repos", *repo_parts)
    languages_dir = os.path.join(repo_path, "languages")
    snapshot: Optional[Dict[str, int]] = None
    if os.path.isdir(languages_dir):
        candidate_paths: List[Tuple[str, Optional[int]]] = []
        if year and month:
            candidate_paths.append((os.path.join(languages_dir, f"{int(year):04d}", f"{int(month):02d}.json"), None))
        if year:
            candidate_paths.append((os.path.join(languages_dir, f"{int(year):04d}", "yearly.json"), month))
        candidate_paths.append((os.path.join(languages_dir, "languages.json"), None))
        latest_snapshot = _find_latest_language_snapshot(languages_dir)
        if latest_snapshot:
            candidate_paths.append((latest_snapshot, None))
        seen_paths: Set[str] = set()
        for path, target_month in candidate_paths:
            if not path or not os.path.isfile(path) or path in seen_paths:
                continue
            seen_paths.add(path)
            data = _load_language_snapshot_from_file(path, target_month)
            if data:
                snapshot = data
                break
    _REPO_LANGUAGE_SNAPSHOT_CACHE[cache_key] = snapshot
    return snapshot


def _collect_user_language_contributions(summary_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    contributions: List[Dict[str, Any]] = []

    def _append_entries(entries: List[Dict[str, Any]], year: Optional[int], month_value: Optional[int]) -> None:
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            repo = entry.get("repo") or entry.get("repository")
            if not repo:
                continue
            additions = _safe_int(entry.get("lines_added") or entry.get("additions"))
            deletions = _safe_int(entry.get("lines_removed") or entry.get("lines_deleted") or entry.get("deletions"))
            changed = entry.get("changed_lines")
            if changed is None:
                changed = additions + deletions
            changed = _safe_int(changed)
            if changed <= 0 and additions <= 0 and deletions <= 0:
                continue
            contributions.append(
                {
                    "repo": repo,
                    "subsystem": entry.get("subsystem") or entry.get("name") or repo,
                    "additions": additions,
                    "deletions": deletions,
                    "changed_lines": changed,
                    "year": year,
                    "month": month_value,
                }
            )

    months_block = summary_data.get("months")
    if isinstance(months_block, dict) and months_block:
        summary_year = _safe_int(summary_data.get("year")) or None
        for month_key, payload in months_block.items():
            if not isinstance(payload, dict):
                continue
            subsystems = payload.get("subsystems")
            if not isinstance(subsystems, list) or not subsystems:
                continue
            month_year = _safe_int(payload.get("year")) or summary_year
            month_value = _safe_int(payload.get("month")) or _safe_int(month_key)
            month_value = month_value or None
            _append_entries(subsystems, month_year or None, month_value)
    else:
        subsystems = summary_data.get("subsystems")
        if isinstance(subsystems, list) and subsystems:
            year = _safe_int(summary_data.get("year")) or None
            month_value = _safe_int(summary_data.get("month")) or None
            _append_entries(subsystems, year, month_value)

    return contributions


def _build_repo_languages_entry(repo_full_name: Optional[str], subsystem_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not repo_full_name:
        return None
    repo_norm = repo_full_name.replace("\\", "/").strip("/")
    if not repo_norm:
        return None
    repo_parts = repo_norm.split("/")
    repo_path = os.path.join(STATS_ROOT, "repos", *repo_parts)
    if not os.path.isdir(repo_path):
        return None
    entry: Dict[str, Any] = {
        "repo_rel": repo_norm,
        "display_name": subsystem_name or repo_parts[-1],
    }
    if subsystem_name:
        entry["languages_dir"] = os.path.join(repo_path, subsystem_name, "languages")
    else:
        entry["languages_dir"] = os.path.join(repo_path, "languages")
    return entry


def _resolve_manifest_entry_for_contribution(repo_full_name: Optional[str], subsystem_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not subsystem_name:
        return None
    manifest_getter = getattr(subsystem_metrics, "_get_subsystem_entries", None)
    if not callable(manifest_getter):
        return None
    manifest = manifest_getter(STATS_ROOT)
    entries = manifest.get("entries", [])
    target = subsystem_name.lower()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("display_name") or "").lower()
        if name != target:
            continue
        repo_rel = entry.get("repo_rel")
        if repo_full_name and repo_rel and repo_rel != repo_full_name:
            continue
        return entry
    by_name = manifest.get("by_name", {}).get(subsystem_name)
    if isinstance(by_name, list) and by_name:
        return by_name[0]
    return None


def _load_primary_language_for_entry(entry: Optional[Dict[str, Any]]) -> Optional[str]:
    if not entry:
        return None
    snapshot = _normalize_language_payload(_load_subsystem_language_snapshot(entry))
    languages = snapshot.get("languages", {})
    primary_language = None
    best_lines = -1
    for name, stats in languages.items():
        code_lines = int(stats.get("code_lines", 0))
        if code_lines > best_lines:
            best_lines = code_lines
            primary_language = name
    if best_lines <= 0:
        return None
    return primary_language


def _resolve_repo_primary_language(repo_full_name: Optional[str]) -> Optional[str]:
    if not repo_full_name:
        return None
    cached = _REPO_PRIMARY_LANGUAGE_CACHE.get(repo_full_name)
    if cached is not None:
        return cached
    entry = _build_repo_languages_entry(repo_full_name, None)
    primary = _load_primary_language_for_entry(entry)
    _REPO_PRIMARY_LANGUAGE_CACHE[repo_full_name] = primary
    return primary


def _resolve_primary_language_for_contribution(repo_full_name: Optional[str], subsystem_name: Optional[str]) -> Optional[str]:
    key = (repo_full_name or "", subsystem_name or "")
    if key in _SUBSYSTEM_PRIMARY_LANGUAGE_CACHE:
        return _SUBSYSTEM_PRIMARY_LANGUAGE_CACHE[key]
    primary = None
    entry = _resolve_manifest_entry_for_contribution(repo_full_name, subsystem_name)
    if not entry and repo_full_name:
        entry = _build_repo_languages_entry(repo_full_name, subsystem_name)
    if entry:
        primary = _load_primary_language_for_entry(entry)
    if not primary and subsystem_name:
        primary = _resolve_repo_primary_language(repo_full_name)
    _SUBSYSTEM_PRIMARY_LANGUAGE_CACHE[key] = primary
    return primary


def _estimate_language_breakdown_from_primary(contributions: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    if not contributions:
        return {}
    aggregates: Dict[str, Dict[str, int]] = {}
    for entry in contributions:
        additions = max(0, entry.get("additions", 0))
        deletions = max(0, entry.get("deletions", 0))
        if additions <= 0 and deletions <= 0:
            continue
        language = _resolve_primary_language_for_contribution(entry.get("repo"), entry.get("subsystem"))
        if not language:
            continue
        lang_entry = aggregates.setdefault(language, {"additions": 0, "deletions": 0})
        lang_entry["additions"] += additions
        lang_entry["deletions"] += deletions
    normalized: Dict[str, Dict[str, int]] = {}
    for lang, stats in aggregates.items():
        additions = stats.get("additions", 0)
        deletions = stats.get("deletions", 0)
        if additions <= 0 and deletions <= 0:
            continue
        normalized[lang] = {
            "additions": additions,
            "deletions": deletions,
            "net_lines": additions - deletions,
        }
    return normalized


def _compute_language_breakdown_for_contributions(contributions: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    if not contributions:
        return {}
    aggregates: Dict[str, Dict[str, float]] = {}
    for entry in contributions:
        additions = max(0, entry.get("additions", 0))
        deletions = max(0, entry.get("deletions", 0))
        if additions <= 0 and deletions <= 0:
            continue
        repo = entry.get("repo")
        languages = _load_repo_language_snapshot(repo, entry.get("year"), entry.get("month"))
        shares: Dict[str, float] = {}
        if languages:
            total_code = sum(max(0, lines) for lines in languages.values())
            if total_code > 0:
                for lang, code_lines in languages.items():
                    if code_lines <= 0:
                        continue
                    shares[lang] = code_lines / total_code
        if not shares:
            fallback = entry.get("subsystem") or repo
            if fallback:
                shares = {fallback: 1.0}
        for lang, share in shares.items():
            if share <= 0:
                continue
            lang_entry = aggregates.setdefault(lang, {"additions": 0.0, "deletions": 0.0})
            lang_entry["additions"] += additions * share
            lang_entry["deletions"] += deletions * share
    normalized: Dict[str, Dict[str, int]] = {}
    for lang, stats in aggregates.items():
        additions = int(round(stats["additions"]))
        deletions = int(round(stats["deletions"]))
        if additions <= 0 and deletions <= 0:
            continue
        normalized[lang] = {
            "additions": additions,
            "deletions": deletions,
            "net_lines": additions - deletions,
        }
    return normalized


def _ensure_user_language_breakdown(summary_data: Dict[str, Any]) -> None:
    if not isinstance(summary_data, dict):
        return
    existing = summary_data.get("languages")
    if isinstance(existing, dict) and existing:
        return
    contributions = _collect_user_language_contributions(summary_data)
    if not contributions:
        return
    languages = _estimate_language_breakdown_from_primary(contributions)
    if not languages:
        languages = _compute_language_breakdown_for_contributions(contributions)
    if languages:
        summary_data["languages"] = languages


def load_user_summary_file(path: str, augment: bool = False) -> Dict[str, Any]:
    data = load_json(path, default={})
    if not isinstance(data, dict):
        data = {}
    normalized = _normalize_user_summary_payload(data)
    if augment:
        normalized.setdefault("total_commits", 0)
        normalized.setdefault("total_lines_added", 0)
        normalized.setdefault("total_lines_deleted", 0)
        normalized.setdefault("per_repo", {})
        normalized.setdefault("languages", {})
        normalized.setdefault("per_date", {})
        _ensure_user_language_breakdown(normalized)
    return normalized


def _list_subsystem_periods(summary_root: Optional[str]) -> List[Dict[str, Any]]:
    periods: List[Dict[str, Any]] = []
    if not summary_root or not os.path.isdir(summary_root):
        return periods

    base_dir = summary_root
    nested_summary = os.path.join(summary_root, "summary")
    if os.path.isdir(nested_summary):
        base_dir = nested_summary

    year_dirs = [name for name in os.listdir(base_dir) if name.isdigit() and os.path.isdir(os.path.join(base_dir, name))]
    if year_dirs:
        for year in sorted(year_dirs):
            year_path = os.path.join(base_dir, year)
            for filename in sorted(os.listdir(year_path)):
                if not filename.endswith(".json"):
                    continue
                file_path = os.path.join(year_path, filename)
                if not os.path.isfile(file_path):
                    continue
                if filename == "yearly.json":
                    periods.append({
                        "folder": filename,
                        "label": year,
                        "from": f"{year}-01-01",
                        "to": f"{year}-12-31",
                        "is_yearly": True,
                    })
                    continue
                month_key = filename[:-5]
                if not month_key.isdigit():
                    continue
                month = int(month_key)
                try:
                    start = datetime(int(year), month, 1)
                except ValueError:
                    continue
                last_day = calendar.monthrange(int(year), month)[1]
                end = datetime(int(year), month, last_day)
                periods.append({
                    "folder": filename,
                    "label": f"{int(year):04d}-{month:02d}",
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                    "is_yearly": False,
                })
    else:
        for filename in sorted(os.listdir(base_dir)):
            if not filename.endswith(".json"):
                continue
            label = filename[:-5]
            start, end, is_yearly = _infer_period_bounds(label)
            periods.append({
                "folder": filename,
                "label": _format_period_label(label),
                "from": start or label,
                "to": end or label,
                "is_yearly": is_yearly,
            })

    periods.sort(key=lambda item: (item["from"], item["to"]), reverse=True)
    return periods


def _discover_subsystems() -> List[Dict[str, Any]]:
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")
    subsystems: List[Dict[str, Any]] = []

    if os.path.isdir(subsystems_root):
        for name in sorted(os.listdir(subsystems_root)):
            subsystem_path = os.path.join(subsystems_root, name)
            if not os.path.isdir(subsystem_path):
                continue
            periods = _list_subsystem_periods(subsystem_path)
            languages_file = os.path.join(subsystem_path, "languages.json")
            primary_language = None
            language_totals = {}
            if os.path.isfile(languages_file):
                language_payload = load_json(languages_file, default={})
                languages = language_payload.get("languages")
                if isinstance(languages, dict):
                    for lang, details in languages.items():
                        if isinstance(details, dict):
                            code_lines = details.get("code_lines", details.get("lines", 0))
                        else:
                            code_lines = int(details or 0)
                        language_totals[lang] = code_lines
                    if language_totals:
                        primary_language = max(language_totals.items(), key=lambda item: item[1])[0]
            subsystems.append({
                "name": name,
                "display_name": name,
                "primary_language": primary_language,
                "periods": periods,
            })
        if subsystems:
            return subsystems

    manifest_getter = getattr(subsystem_metrics, "_get_subsystem_entries", None)
    if callable(manifest_getter):
        manifest = manifest_getter(STATS_ROOT)
        for entry in manifest.get("entries", []):
            subsystems.append({
                "name": entry.get("display_name"),
                "display_name": entry.get("display_name"),
                "repo": entry.get("repo_rel"),
                "periods": _list_subsystem_periods(entry.get("summary_dir")),
            })
    return subsystems


def _find_subsystem_entry(subsystem_name: str) -> Optional[Dict[str, Any]]:
    if not subsystem_name:
        return None
    manifest_getter = getattr(subsystem_metrics, "_get_subsystem_entries", None)
    if not callable(manifest_getter):
        return None
    manifest = manifest_getter(STATS_ROOT)
    by_name = manifest.get("by_name", {})
    candidates = by_name.get(subsystem_name)
    if not candidates:
        normalized = subsystem_name.strip()
        candidates = by_name.get(normalized) or by_name.get(normalized.lower())
    if candidates:
        return candidates[0]
    for entry in manifest.get("entries", []):
        if entry.get("display_name") == subsystem_name:
            return entry
    return None


def _subsystem_summary_dir(entry: Optional[Dict[str, Any]]) -> Optional[str]:
    if not entry:
        return None
    candidates = [entry.get("summary_dir")]
    summary_dir = entry.get("summary_dir")
    if summary_dir:
        candidates.append(os.path.join(summary_dir, "summary"))
    repo_rel = entry.get("repo_rel")
    display_name = entry.get("display_name")
    if repo_rel:
        repo_path = os.path.join(STATS_ROOT, "repos", *repo_rel.split("/"))
        candidates.extend([
            os.path.join(repo_path, "summary"),
            os.path.join(repo_path, display_name or "", "summary"),
        ])
    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return candidate
    return None


def _subsystem_languages_dir(entry: Optional[Dict[str, Any]]) -> Optional[str]:
    if not entry:
        return None
    candidates = [entry.get("languages_dir")]
    repo_rel = entry.get("repo_rel")
    display_name = entry.get("display_name")
    if repo_rel:
        repo_path = os.path.join(STATS_ROOT, "repos", *repo_rel.split("/"))
        candidates.extend([
            os.path.join(repo_path, "languages"),
            os.path.join(repo_path, display_name or "", "languages"),
        ])
    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return candidate
    return None


def _resolve_subsystem_summary_file(entry: Optional[Dict[str, Any]], from_date: Optional[str], to_date: Optional[str], is_yearly: bool) -> Optional[str]:
    summary_dir = _subsystem_summary_dir(entry)
    if not summary_dir:
        return None
    base_dir = summary_dir
    year_dirs = [name for name in os.listdir(base_dir) if name.isdigit() and os.path.isdir(os.path.join(base_dir, name))]

    if is_yearly:
        year = None
        if from_date and len(from_date) >= 4:
            year = from_date[:4]
        elif to_date and len(to_date) >= 4:
            year = to_date[:4]
        if not year:
            return None
        if year_dirs:
            candidate = os.path.join(base_dir, year, "yearly.json")
            if os.path.isfile(candidate):
                return candidate
        else:
            candidate = os.path.join(base_dir, f"{year}.json")
            if os.path.isfile(candidate):
                return candidate
        return None

    # monthly period
    target_year = None
    target_month = None
    if from_date and len(from_date) >= 7:
        target_year = from_date[:4]
        target_month = from_date[5:7]
    elif to_date and len(to_date) >= 7:
        target_year = to_date[:4]
        target_month = to_date[5:7]
    if not (target_year and target_month):
        return None

    if year_dirs:
        candidate = os.path.join(base_dir, target_year, f"{target_month}.json")
        if os.path.isfile(candidate):
            return candidate
    fallback = os.path.join(base_dir, f"{target_year}-{target_month}.json")
    if os.path.isfile(fallback):
        return fallback
    legacy = os.path.join(base_dir, f"{target_year}{target_month}.json")
    if os.path.isfile(legacy):
        return legacy
    return None


def _load_subsystem_language_snapshot(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    languages_dir = _subsystem_languages_dir(entry)
    if not languages_dir:
        return {}
    year_dirs = [name for name in os.listdir(languages_dir) if name.isdigit() and os.path.isdir(os.path.join(languages_dir, name))]
    for year in sorted(year_dirs, reverse=True):
        year_path = os.path.join(languages_dir, year)
        month_files = [f for f in os.listdir(year_path) if f.endswith(".json") and f != "yearly.json"]
        if month_files:
            latest = sorted(month_files)[-1]
            data = load_json(os.path.join(year_path, latest), default={})
            if data:
                return data
        yearly_path = os.path.join(year_path, "yearly.json")
        if os.path.isfile(yearly_path):
            data = load_json(yearly_path, default={})
            summary = data.get("summary", {}).get("latest", {})
            if summary:
                total = summary.get("total", {})
                languages = summary.get("languages") or {}
                return {"languages": languages, "totals": total}
    fallback_file = os.path.join(languages_dir, "languages.json")
    if os.path.isfile(fallback_file):
        return load_json(fallback_file, default={})
    return {}


def _load_subsystem_summary(subsystem_name: str, from_date: Optional[str], to_date: Optional[str], is_yearly: bool) -> Dict[str, Any]:
    entry = _find_subsystem_entry(subsystem_name)
    path = _resolve_subsystem_summary_file(entry, from_date, to_date, is_yearly)
    if not path or not os.path.isfile(path):
        return {}
    data = load_json(path, default={})
    return _ensure_developer_rollup(data)
def _merge_developer_rollup(dest: Dict[str, Dict[str, Any]], slug: str, payload: Dict[str, Any]) -> None:
    if not slug or not isinstance(payload, dict):
        return
    commits = _safe_int(payload.get("commits"))
    if commits <= 0 and not any(payload.get(key) for key in ("lines_added", "lines_deleted", "changed_lines")):
        return
    entry = dest.setdefault(
        slug,
        {
            "slug": slug,
            "display_name": payload.get("display_name") or slug,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "lines_net": 0,
            "changed_lines": 0,
            "files_changed": 0,
        },
    )
    if payload.get("display_name") and not entry.get("display_name"):
        entry["display_name"] = payload.get("display_name")
    entry["commits"] += commits
    entry["lines_added"] += _safe_int(payload.get("lines_added"))
    entry["lines_deleted"] += _safe_int(payload.get("lines_deleted"))
    entry["lines_net"] += _safe_int(payload.get("lines_net"))
    changed = payload.get("changed_lines")
    if changed is None:
        changed = payload.get("lines_changed")
    if changed is None:
        changed = _safe_int(payload.get("lines_added")) + _safe_int(payload.get("lines_deleted"))
    entry["changed_lines"] += _safe_int(changed)
    entry["files_changed"] += _safe_int(payload.get("files_changed"))


def _harvest_developer_sources(source: Dict[str, Any], dest: Dict[str, Dict[str, Any]]) -> None:
    if not isinstance(source, dict):
        return
    developers = source.get("developers")
    if isinstance(developers, dict):
        for slug, payload in developers.items():
            if isinstance(payload, dict):
                _merge_developer_rollup(dest, slug, payload)
    repositories = source.get("repositories")
    if isinstance(repositories, dict):
        for repo_data in repositories.values():
            if not isinstance(repo_data, dict):
                continue
            repo_devs = repo_data.get("developers")
            if not isinstance(repo_devs, dict):
                continue
            for slug, payload in repo_devs.items():
                if isinstance(payload, dict):
                    _merge_developer_rollup(dest, slug, payload)


def _ensure_developer_rollup(summary_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(summary_data, dict):
        return summary_data
    developers = summary_data.get("developers")
    if isinstance(developers, dict) and developers:
        return summary_data
    aggregated: Dict[str, Dict[str, Any]] = {}
    months = summary_data.get("months")
    if isinstance(months, dict):
        for month_entry in months.values():
            if isinstance(month_entry, dict):
                _harvest_developer_sources(month_entry, aggregated)
    _harvest_developer_sources(summary_data, aggregated)
    if aggregated:
        summary_data["developers"] = aggregated
    return summary_data


def _normalize_language_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    languages = payload.get("languages")
    totals = payload.get("total") or payload.get("totals") or {}
    normalized: Dict[str, Dict[str, int]] = {}
    if isinstance(languages, list):
        for entry in languages:
            name = entry.get("name")
            if not name:
                continue
            normalized[name] = {
                "code_lines": int(entry.get("code") or entry.get("code_lines") or entry.get("lines") or 0),
                "comments": int(entry.get("comments", 0)),
                "blanks": int(entry.get("blanks", 0)),
            }
    elif isinstance(languages, dict):
        for name, stats in languages.items():
            if isinstance(stats, dict):
                normalized[name] = {
                    "code_lines": int(stats.get("code_lines") or stats.get("code") or stats.get("lines") or 0),
                    "comments": int(stats.get("comments", 0)),
                    "blanks": int(stats.get("blanks", 0)),
                }
            else:
                try:
                    normalized[name] = {"code_lines": int(stats or 0), "comments": 0, "blanks": 0}
                except Exception:
                    continue
    total_payload = {
        "files": totals.get("files"),
        "code_lines": int(totals.get("code_lines") or totals.get("code") or totals.get("lines") or 0),
        "comments": int(totals.get("comments", 0)),
        "blanks": int(totals.get("blanks", 0)),
    }
    return {"languages": normalized, "totals": total_payload}


def _extract_language_totals_from_entry(entry: Optional[Dict[str, Any]]) -> Dict[str, int]:
    if not entry:
        return {}
    snapshot = _normalize_language_payload(_load_subsystem_language_snapshot(entry))
    language_totals: Dict[str, int] = {}
    for lang, stats in (snapshot.get("languages") or {}).items():
        try:
            code_lines = int(stats.get("code_lines") or 0)
        except Exception:
            code_lines = 0
        if code_lines > 0:
            language_totals[lang] = code_lines
    return language_totals


def _get_repo_language_breakdown(repo_full_name: Optional[str]) -> Optional[Dict[str, int]]:
    entry = _build_repo_languages_entry(repo_full_name, None)
    if not entry:
        return None
    languages = _extract_language_totals_from_entry(entry)
    return languages or None


def _get_service_language_breakdown(repo_full_name: Optional[str], service_name: Optional[str]) -> Optional[Dict[str, int]]:
    entry = _build_repo_languages_entry(repo_full_name, service_name)
    if not entry:
        return None
    languages = _extract_language_totals_from_entry(entry)
    return languages or None


def _resolve_responsible_subsystem_languages(subsystem_name: str) -> Tuple[Dict[str, int], int]:
    languages: Dict[str, int] = {}
    total_lines = 0
    entry = _find_subsystem_entry(subsystem_name)
    if entry:
        languages = _extract_language_totals_from_entry(entry)
        total_lines = sum(languages.values())
        if total_lines <= 0:
            repo_rel = entry.get("repo_rel")
            service_name = None if entry.get("is_root") else entry.get("display_name")
            fallback_langs: Optional[Dict[str, int]] = None
            if repo_rel:
                if service_name:
                    fallback_langs = _get_service_language_breakdown(repo_rel, service_name)
                if not fallback_langs:
                    fallback_langs = _get_repo_language_breakdown(repo_rel)
            if fallback_langs:
                languages = {
                    lang: int(max(0, lines))
                    for lang, lines in fallback_langs.items()
                    if int(max(0, lines)) > 0
                }
                total_lines = sum(languages.values())
    return languages, total_lines


def _build_subsystem_loc_series(entry: Optional[Dict[str, Any]], year: int) -> List[Dict[str, Any]]:
    languages_dir = _subsystem_languages_dir(entry)
    if not languages_dir:
        return []
    year_path = os.path.join(languages_dir, str(year))
    if not os.path.isdir(year_path):
        return []
    series: List[Dict[str, Any]] = []
    for filename in sorted(os.listdir(year_path)):
        if not filename.endswith(".json") or filename == "yearly.json":
            continue
        month_key = filename[:-5]
        if not month_key.isdigit():
            continue
        data = load_json(os.path.join(year_path, filename), default={})
        totals = data.get("total") or data.get("totals") or {}
        code_lines = int(totals.get("code_lines") or totals.get("code") or totals.get("lines") or 0)
        series.append({"month": f"{year}-{int(month_key):02d}", "code_lines": code_lines})
    return series


_user_display_cache: Dict[str, str] = {}


def _get_user_display_name(user_slug: str, user_periods: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> str:
    cached = _user_display_cache.get(user_slug)
    if cached:
        return cached
    periods = (user_periods or {}).get(user_slug) if user_periods is not None else None
    if periods is None:
        periods = list_user_months().get(user_slug, [])
    for period in periods:
        path = resolve_user_period_summary_path(user_slug, period)
        if not path or not os.path.isfile(path):
            continue
        data = load_user_summary_file(path, augment=False)
        display_name = (
            data.get("user")
            or data.get("author")
            or data.get("summary", {}).get("author")
            or user_slug
        )
        _user_display_cache[user_slug] = display_name
        return display_name
    _user_display_cache[user_slug] = user_slug
    return user_slug


def _list_local_repositories() -> List[Dict[str, Any]]:
    repos: List[Dict[str, Any]] = []
    if not os.path.isdir(REPO_ROOT):
        return repos
    for org_name in sorted(os.listdir(REPO_ROOT)):
        org_path = os.path.join(REPO_ROOT, org_name)
        if not os.path.isdir(org_path):
            continue
        for repo_name in sorted(os.listdir(org_path)):
            repo_path = os.path.join(org_path, repo_name)
            git_dir = os.path.join(repo_path, ".git")
            if not os.path.isdir(git_dir):
                continue
            remote_url = None
            config_path = os.path.join(git_dir, "config")
            if os.path.exists(config_path):
                parser = configparser.ConfigParser()
                try:
                    parser.read(config_path)
                    for section in parser.sections():
                        if section.startswith('remote '):
                            remote_url = parser.get(section, 'url', fallback=None)
                            if remote_url:
                                break
                except Exception:
                    remote_url = None
            repos.append({
                "name": f"{org_name}/{repo_name}",
                "url": remote_url,
                "exists": True,
            })
    return repos


def _validate_repo_name_value(name: Any) -> str:
    value = (name or "").strip()
    if not value:
        raise ValueError("Repository name is required.")
    if not REPO_NAME_PATTERN.match(value):
        raise ValueError("Repository name must be in 'owner/repo' format.")
    return value


def _validate_repo_url_value(url: Any) -> str:
    value = (url or "").strip()
    if not value:
        raise ValueError("Repository URL is required.")
    if value.startswith(('-')):
        raise ValueError("Repository URL cannot start with '-'.")
    if value.lower().startswith("file://"):
        raise ValueError("file:// URLs are not allowed.")
    return value


def _repo_path_for_name(name: str) -> str:
    owner, repo = name.split('/', 1)
    return os.path.join(REPO_ROOT, owner, repo)


def _append_clone_message(progress_id: str, message: str) -> None:
    clean = (message or "").strip()
    if not clean:
        return
    with _repo_clone_lock:
        task = _repo_clone_tasks.get(progress_id)
        if not task:
            return
        messages = task.setdefault("messages", [])
        messages.append(clean)


def _update_clone_task(progress_id: str, **updates: Any) -> None:
    with _repo_clone_lock:
        task = _repo_clone_tasks.get(progress_id)
        if not task:
            return
        task.update(updates)


def _start_clone_task(repo_name: str, repo_url: str, target_path: str) -> str:
    progress_id = uuid.uuid4().hex
    payload = {
        "id": progress_id,
        "repo_name": repo_name,
        "repo_url": repo_url,
        "target_path": target_path,
        "status": "starting",
        "messages": [],
        "error": None,
        "started_at": time.time(),
        "last_sent_index": 0,
    }
    with _repo_clone_lock:
        _repo_clone_tasks[progress_id] = payload
    thread = threading.Thread(
        target=_clone_repository_worker,
        args=(progress_id,),
        name=f"repo-clone-{repo_name.replace('/', '-')}",
        daemon=True,
    )
    thread.start()
    return progress_id


def _clone_repository_worker(progress_id: str) -> None:
    process: Optional[subprocess.Popen] = None
    with _repo_clone_lock:
        task = _repo_clone_tasks.get(progress_id)
        if not task:
            return
        repo_name = task.get("repo_name")
        repo_url = task.get("repo_url")
        target_path = task.get("target_path")
    if not repo_name or not repo_url or not target_path:
        _update_clone_task(progress_id, status="failed", error="Invalid clone parameters.", completed_at=time.time())
        return

    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")

    command = ["git", "clone", "--progress", repo_url, target_path]
    last_line = ""
    try:
        _append_clone_message(progress_id, f"Starting git clone for {repo_name}")
        _update_clone_task(progress_id, status="cloning")
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        if process.stdout:
            for line in process.stdout:
                clean = line.strip()
                if not clean:
                    continue
                last_line = clean
                _append_clone_message(progress_id, clean)
        returncode = process.wait()
        if returncode == 0:
            _append_clone_message(progress_id, f"✅ Finished cloning {repo_name}")
            _update_clone_task(progress_id, status="completed", completed_at=time.time())
        else:
            error_message = last_line or f"git clone exited with status {returncode}"
            _append_clone_message(progress_id, f"❌ {error_message}")
            _update_clone_task(progress_id, status="failed", error=error_message, completed_at=time.time())
            if os.path.isdir(target_path):
                shutil.rmtree(target_path, ignore_errors=True)
    except Exception as exc:
        error_message = str(exc)
        _append_clone_message(progress_id, f"❌ {error_message}")
        _update_clone_task(progress_id, status="failed", error=error_message, completed_at=time.time())
        if os.path.isdir(target_path):
            shutil.rmtree(target_path, ignore_errors=True)
    finally:
        if process and process.stdout:
            process.stdout.close()
        _cleanup_clone_tasks()


def _clone_progress_payload(progress_id: str) -> Optional[Dict[str, Any]]:
    with _repo_clone_lock:
        task = _repo_clone_tasks.get(progress_id)
        if not task:
            return None
        messages = task.get("messages", [])
        last_sent = task.get("last_sent_index", 0)
        new_messages = messages[last_sent:]
        task["last_sent_index"] = len(messages)
        payload = {
            "status": task.get("status", "starting"),
            "error": task.get("error"),
            "progress_messages": new_messages,
            "elapsed_time": int(time.time() - task.get("started_at", time.time())),
        }
    return payload


def _cleanup_clone_tasks() -> None:
    now = time.time()
    with _repo_clone_lock:
        expired = [
            task_id
            for task_id, payload in _repo_clone_tasks.items()
            if payload.get("status") in {"completed", "failed"}
            and payload.get("completed_at")
            and now - payload["completed_at"] > CLONE_TASK_RETENTION_SECONDS
        ]
        for task_id in expired:
            _repo_clone_tasks.pop(task_id, None)


def _api_clone_repository(body: Dict[str, Any]):
    try:
        repo_name = _validate_repo_name_value(body.get("name"))
        repo_url = _validate_repo_url_value(body.get("url"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    target_path = _repo_path_for_name(repo_name)
    if os.path.exists(target_path):
        return jsonify({"error": "Repository already exists locally."}), 400

    if shutil.which("git") is None:
        return jsonify({"error": "git is not available on the server."}), 500

    progress_id = _start_clone_task(repo_name, repo_url, target_path)
    return jsonify({"progress_id": progress_id})


def _api_remove_repository(body: Dict[str, Any]):
    try:
        repo_name = _validate_repo_name_value(body.get("name"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    repo_path = _repo_path_for_name(repo_name)
    if not os.path.isdir(repo_path):
        return jsonify({"error": "Repository not found."}), 404

    try:
        shutil.rmtree(repo_path)
        owner_dir = os.path.dirname(repo_path)
        if owner_dir.startswith(REPO_ROOT) and os.path.isdir(owner_dir) and not os.listdir(owner_dir):
            os.rmdir(owner_dir)
    except Exception as exc:
        logger.error("Failed to remove repository %s: %s", repo_name, exc)
        return jsonify({"error": f"Failed to remove repository: {exc}"}), 500

    return jsonify({"success": True, "async": False})


app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["READ_ONLY_MODE"] = False
app.config["SHOW_LOGO"] = True


@app.route("/")
def root_index():
    return render_template(
        "index.html",
        read_only=bool(app.config.get("READ_ONLY_MODE", False)),
        kiosk_mode=False,
        show_logo=bool(app.config.get("SHOW_LOGO", True)),
    )


@app.route("/kiosk")
def kiosk_mode():
    return render_template(
        "index.html",
        read_only=bool(app.config.get("READ_ONLY_MODE", False)),
        kiosk_mode=True,
        show_logo=bool(app.config.get("SHOW_LOGO", True)),
    )


@app.route("/api/settings/repositories", methods=["GET", "POST"])
def api_settings_repositories():
    if request.method == "GET":
        return jsonify({"repositories": _list_local_repositories()})

    if app.config.get("READ_ONLY_MODE", False):
        return jsonify({"error": "Repository management is disabled in read-only mode."}), 403

    body = request.get_json(silent=True) or {}
    action = (body.get("action") or "").strip().lower()

    if action == "clone":
        return _api_clone_repository(body)
    if action == "remove":
        return _api_remove_repository(body)

    return jsonify({"error": "Unsupported repository action."}), 400


@app.route("/api/settings/repositories/clone-progress/<progress_id>")
def api_repository_clone_progress(progress_id: str):
    payload = _clone_progress_payload(progress_id)
    if payload is None:
        abort(404)
    _cleanup_clone_tasks()
    return jsonify(payload)


@app.route("/api/settings/integrations", methods=["GET", "POST"])
def api_settings_integrations():
    def _build_payload(data: Dict[str, Any]) -> Dict[str, Any]:
        pagerduty = data.get("pagerduty", {}) if isinstance(data, dict) else {}
        token = pagerduty.get("api_token")
        available = pagerduty.get("available_services") or []
        if not isinstance(available, list):
            available = []
        selected = pagerduty.get("selected_service_ids") or []
        if not isinstance(selected, list):
            selected = []
        return {
            "pagerduty": {
                "has_token": bool(token),
                "token_preview": (token[:4] + "…" + token[-2:]) if token and len(token) > 6 else None,
                "updated_at": pagerduty.get("updated_at"),
                "available_services": available,
                "selected_service_ids": [str(item) for item in selected if isinstance(item, str)],
                "services_fetched_at": pagerduty.get("services_fetched_at"),
            }
        }

    data = load_json(INTEGRATIONS_FILE, default={})
    if request.method == "GET":
        return jsonify(_build_payload(data))

    if app.config.get("READ_ONLY_MODE", False):
        return jsonify({"error": "Integrations are disabled in read-only mode."}), 403

    body = request.get_json(silent=True) or {}
    pagerduty_payload = body.get("pagerduty") or {}
    if not isinstance(pagerduty_payload, dict):
        return jsonify({"error": "Missing pagerduty payload"}), 400
    new_token = pagerduty_payload.get("api_token")
    new_selected = pagerduty_payload.get("selected_service_ids")
    if new_token is None and new_selected is None:
        return jsonify({"error": "Missing pagerduty payload"}), 400

    updated = load_json(INTEGRATIONS_FILE, default={}) if os.path.exists(INTEGRATIONS_FILE) else {}
    updated.setdefault("pagerduty", {})
    now_iso = datetime.utcnow().isoformat() + "Z"
    if new_token is not None:
        if new_token.strip():
            updated["pagerduty"]["api_token"] = new_token.strip()
        else:
            updated["pagerduty"].pop("api_token", None)
            updated["pagerduty"].pop("available_services", None)
            updated["pagerduty"].pop("selected_service_ids", None)
            updated["pagerduty"].pop("services_fetched_at", None)
        updated["pagerduty"]["updated_at"] = now_iso
    if new_selected is not None:
        if not isinstance(new_selected, list):
            return jsonify({"error": "selected_service_ids must be a list"}), 400
        cleaned: List[str] = []
        seen: Set[str] = set()
        for item in new_selected:
            if not isinstance(item, str):
                continue
            trimmed = item.strip()
            if trimmed and trimmed not in seen:
                seen.add(trimmed)
                cleaned.append(trimmed)
        updated["pagerduty"]["selected_service_ids"] = cleaned
    save_json(INTEGRATIONS_FILE, updated)
    return jsonify(_build_payload(updated))


@app.route("/api/integrations/pagerduty/services", methods=["POST"])
def api_integrations_pagerduty_services():
    if app.config.get("READ_ONLY_MODE", False):
        return jsonify({"error": "Integrations are disabled in read-only mode."}), 403
    token = pagerduty_sync._load_pagerduty_token(BASE_DIR)
    if not token:
        return jsonify({"error": "No PagerDuty token configured"}), 400
    try:
        services = pagerduty_sync._fetch_pagerduty_services(token)
    except Exception as exc:
        return jsonify({"error": f"Failed to fetch PagerDuty services: {exc}"}), 502

    updated = load_json(INTEGRATIONS_FILE, default={}) if os.path.exists(INTEGRATIONS_FILE) else {}
    updated.setdefault("pagerduty", {})
    now_iso = datetime.utcnow().isoformat() + "Z"
    updated["pagerduty"]["available_services"] = services
    updated["pagerduty"]["services_fetched_at"] = now_iso
    save_json(INTEGRATIONS_FILE, updated)

    selected = updated["pagerduty"].get("selected_service_ids") or []
    return jsonify({
        "services": services,
        "selected_service_ids": [str(item) for item in selected if isinstance(item, str)],
        "services_fetched_at": now_iso,
    })


@app.route("/api/integrations/pagerduty/sync", methods=["POST"])
def api_integrations_pagerduty_sync():
    if app.config.get("READ_ONLY_MODE", False):
        return jsonify({"error": "Integrations are disabled in read-only mode."}), 403
    if update_process_active:
        return jsonify({"error": "A full update is currently running. Please wait for it to finish."}), 409
    token = pagerduty_sync._load_pagerduty_token(BASE_DIR)
    if not token:
        return jsonify({"error": "No PagerDuty token configured"}), 400
    if getattr(pagerduty_sync, "requests", None) is None:
        return jsonify({"error": "Python 'requests' package is required for PagerDuty sync. Install it with 'pip install requests'."}), 500

    sync_logger = logging.getLogger("pagerduty_sync.integrations-route")
    sync_logger.setLevel(logging.INFO)
    captured_messages: List[str] = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                captured_messages.append(record.getMessage())
            except Exception:
                captured_messages.append(str(record.msg))

    handler = _CapturingHandler(level=logging.INFO)
    sync_logger.addHandler(handler)
    try:
        summary = pagerduty_sync.sync_pagerduty_data(
            BASE_DIR,
            BASE_DIR,
            lookback_days=getattr(pagerduty_sync, "DEFAULT_LOOKBACK_DAYS", 365),
            logger=sync_logger,
        )
    except Exception as exc:
        return jsonify({"error": f"PagerDuty sync failed: {exc}"}), 502
    finally:
        sync_logger.removeHandler(handler)

    if summary is None:
        detail = ""
        for message in reversed(captured_messages):
            lower = message.lower()
            if "warning" in lower or "failed" in lower or "error" in lower:
                detail = message
                break
        if not detail and captured_messages:
            detail = captured_messages[-1]
        if not detail:
            detail = "PagerDuty sync returned no data."
        return jsonify({"error": f"PagerDuty sync failed: {detail}"}), 502
    totals = summary.get("totals") or {}
    return jsonify({
        "status": "ok",
        "total_incidents": totals.get("total", 0),
        "completed_at": datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/update/last-run")
def api_update_last_run():
    settings = load_update_settings()
    state = get_background_state_snapshot()
    # Return flat structure that matches frontend expectations
    return jsonify({
        "last_update": settings.get("last_update"),
        "background_enabled": settings.get("background_enabled", False),
        "interval_hours": settings.get("interval_hours", 24),
        "background_running": state.get("running", False),
        "next_run": state.get("next_run"),
        "last_manual_completed_at": settings.get("last_manual_completed_at"),
        "last_background_completed_at": settings.get("last_background_completed_at"),
    })


@app.route("/api/users")
def api_users_list():
    user_months = list_user_months()
    users: List[Dict[str, Any]] = []
    for slug, periods in user_months.items():
        display_name = slug
        summary_path = None
        for period in periods:
            summary_path = resolve_user_period_summary_path(slug, period)
            if summary_path:
                break
        if summary_path and os.path.exists(summary_path):
            summary_data = load_user_summary_file(summary_path)
            display_name = summary_data.get("author_name") or summary_data.get("name") or slug
        users.append({
            "slug": slug,
            "display_name": display_name,
            "periods": periods,
            "months": periods,
        })
    return jsonify({"users": users})


@app.route("/api/users/overview")
def api_users_overview():
    return jsonify(build_users_overview_payload())


@app.route("/api/users/top-contributors")
def api_users_top_contributors():
    """Return top 20 contributors with detailed comparison stats (subsystems, languages, commits, etc.)."""
    limit = request.args.get("limit", 20, type=int)
    requested_year = request.args.get("year", None, type=int)

    user_periods = list_user_months()
    ignored = load_ignored_user_slugs()
    visible_users = [slug for slug in user_periods.keys() if slug not in ignored]

    # Collect all available yearly periods
    available_years = set()
    for slug in visible_users:
        for period in user_periods.get(slug, []):
            if period.get("is_yearly"):
                year_str = period.get("label") or period["from"][:4]
                try:
                    available_years.add(int(year_str))
                except (ValueError, TypeError):
                    pass

    available_years_sorted = sorted(available_years, reverse=True)

    # Find the target yearly period matching the requested year (or latest)
    target_period = None
    for slug in visible_users:
        for period in user_periods.get(slug, []):
            if not period.get("is_yearly"):
                continue
            if requested_year:
                year_str = period.get("label") or period["from"][:4]
                try:
                    if int(year_str) != requested_year:
                        continue
                except (ValueError, TypeError):
                    continue
            if not target_period or period["from"] > target_period["from"]:
                target_period = period

    if not target_period:
        return jsonify({"contributors": [], "period": None, "available_years": available_years_sorted})

    from_date = target_period["from"]
    to_date = target_period["to"]

    # Gather stats for all visible users
    contributors = []
    for slug in visible_users:
        member_data = aggregate_user_data_for_period(slug, from_date, to_date)
        entry = _build_member_contribution_entry(member_data)
        if entry["commits"] > 0:
            entry["slug"] = slug
            entry["display_name"] = _get_user_display_name(slug, user_periods)
            contributors.append(entry)

    # Sort by commits descending, take top N
    contributors.sort(key=lambda x: x["commits"], reverse=True)
    top = contributors[:limit]

    return jsonify({
        "contributors": top,
        "period": {
            "label": target_period.get("label", ""),
            "from": from_date,
            "to": to_date,
            "is_yearly": target_period.get("is_yearly", False),
        },
        "total_contributors": len(contributors),
        "available_years": available_years_sorted,
    })


@app.route("/api/users/badges-overview")
def api_users_badges_overview():
    cache = load_badge_cache()
    if not cache.get("summary"):
        cache = refresh_badge_cache()
    if not cache.get("summary"):
        return jsonify({"summary": None})
    return jsonify({
        "summary": cache.get("summary", {}),
        "top_badge_holders": cache.get("top_badge_holders", []),
        "top_ownership_holders": cache.get("top_ownership_holders", []),
        "generated_at": cache.get("generated_at"),
    })


@app.route("/api/users/ownership-distribution")
def api_users_ownership_distribution():
    snapshot = load_json(OWNERSHIP_DISTRIBUTION_FILE, default={})
    if snapshot.get("owners"):
        return jsonify(snapshot)
    payload = build_ownership_distribution_snapshot()
    save_json(OWNERSHIP_DISTRIBUTION_FILE, payload)
    return jsonify(payload)


@app.route("/api/users/<user_slug>/badges")
def api_user_badges(user_slug: str):
    cache = load_badge_cache()
    if not cache.get("per_user"):
        cache = refresh_badge_cache()
    per_user = cache.get("per_user") or {}
    user_entry = per_user.get(user_slug, {})
    user_badges = user_entry.get("badges", [])
    type_counts = Counter(badge.get("type", "unknown") for badge in user_badges)
    badge_count = len(user_badges)
    summary_block = cache.get("summary", {}) if isinstance(cache, dict) else {}
    total_holders = int(summary_block.get("users_with_badges") or 0)
    population_total = total_holders or len(per_user)

    badge_rankings = _build_badge_rankings(per_user)
    ranking_info = badge_rankings.get(user_slug)
    percentile = None
    if ranking_info and population_total > 0:
        percentile = (ranking_info["rank"] / population_total) * 100.0

    stats_payload = {
        "badge_count": badge_count,
        "count": badge_count,
        "type_counts": {k: int(v) for k, v in type_counts.items()},
        "total_holders": total_holders,
        "total": population_total,
    }
    if ranking_info:
        stats_payload["rank"] = ranking_info["rank"]
        stats_payload["percentile"] = percentile

    return jsonify({"badges": user_badges, "stats": stats_payload})


@app.route("/api/users/<user_slug>/year/<int:year>")
def api_user_year_summary(user_slug: str, year: int):
    from_date = f"{year:04d}-01-01"
    to_date = f"{year:04d}-12-31"
    summary_path = find_user_summary(user_slug, from_date, to_date)
    if not summary_path:
        abort(404, description="Yearly summary not found")
    data = load_user_summary_file(summary_path, augment=True)
    peer_rankings, peer_details = compute_user_year_peer_rankings(user_slug, year)
    if peer_rankings:
        data["peer_rankings"] = peer_rankings
    if peer_details:
        data["peer_rankings_detail"] = peer_details
    data.setdefault("from", from_date)
    data.setdefault("to", to_date)
    data.setdefault("is_yearly", True)
    data.setdefault("period", {"from": from_date, "to": to_date, "is_yearly": True})
    return jsonify(data)


@app.route("/api/users/<user_slug>/month/<from_date>/<to_date>")
def api_user_month_summary(user_slug: str, from_date: str, to_date: str):
    summary_path = find_user_summary(user_slug, from_date, to_date)
    if not summary_path:
        # Attempt to resolve by deriving year/month file
        try:
            year = int(from_date[:4])
            month = int(from_date[5:7])
            month_file = os.path.join(STATS_ROOT, "users", user_slug, f"{year:04d}", f"{month:02d}.json")
            summary_path = month_file if os.path.isfile(month_file) else None
        except Exception:
            summary_path = None
    if not summary_path:
        abort(404, description="Monthly summary not found")
    data = load_user_summary_file(summary_path, augment=True)
    peer_rankings = compute_user_month_peer_rankings(user_slug, from_date, to_date)
    if peer_rankings:
        data["peer_rankings"] = peer_rankings
    data.setdefault("from", from_date)
    data.setdefault("to", to_date)
    data.setdefault("is_yearly", False)
    data.setdefault("period", {"from": from_date, "to": to_date, "is_yearly": False})
    return jsonify(data)


@app.route("/api/users/<user_slug>/ownership-timeline")
def api_user_ownership_timeline(user_slug: str):
    return jsonify({"timelines": {}})


@app.route("/api/users/<user_slug>/subsystem-activity/<int:year>")
def api_user_subsystem_activity(user_slug: str, year: int):
    try:
        payload = build_user_subsystem_activity(user_slug, year)
        return jsonify(payload)
    except Exception as exc:
        app.logger.exception("Failed to build subsystem activity for %s", user_slug)
        return (
            jsonify(
                {
                    "user": user_slug,
                    "year": year,
                    "timeline": [],
                    "summary": {},
                    "error": str(exc),
                }
            ),
            500,
        )


@app.route("/api/subsystems")
def api_subsystems_list():
    return jsonify({"subsystems": _discover_subsystems()})


@app.route("/api/subsystems/dead-status")
def api_subsystems_dead_status():
    status = compute_dead_subsystems(STATS_ROOT)
    return jsonify({"subsystem_status": status})


@app.route("/api/subsystems/overview")
def api_subsystems_overview():
    size_data = compute_subsystem_size_rankings(STATS_ROOT)
    dead_status = compute_dead_subsystems(STATS_ROOT)
    dead_count = sum(1 for entry in dead_status.values() if entry.get("is_dead"))
    payload = {
        "size_data": size_data,
        "dead_subsystems": {
            "count": dead_count,
            "details": dead_status,
        },
        "total_subsystems": size_data.get("total_subsystems", 0),
        "trend": [],
        "recent_trend": [],
    }
    return jsonify(payload)


def _build_update_settings_payload(settings: Dict[str, Any]) -> Dict[str, Any]:
    state = get_background_state_snapshot()
    payload = {
        "background_enabled": bool(settings.get("background_enabled", False)),
        "interval_hours": settings.get("interval_hours", 24),
        "last_update": settings.get("last_update"),
        "last_manual_completed_at": settings.get("last_manual_completed_at"),
        "last_background_completed_at": settings.get("last_background_completed_at"),
        "next_run": state.get("next_run"),
        "background_running": state.get("running", False),
    }
    return payload


@app.route("/api/settings/update-config", methods=["GET", "POST"])
def api_settings_update_config():
    settings = load_update_settings()
    if request.method == "GET":
        return jsonify(_build_update_settings_payload(settings))

    body = request.get_json(silent=True) or {}
    if "background_enabled" in body:
        settings["background_enabled"] = bool(body.get("background_enabled"))
    if "interval_hours" in body:
        try:
            settings["interval_hours"] = max(1, int(body.get("interval_hours", 24)))
        except (TypeError, ValueError):
            settings["interval_hours"] = 24
    save_update_settings(settings)
    schedule_background_check()
    if settings.get("background_enabled"):
        start_background_scheduler()
    payload = _build_update_settings_payload(settings)
    payload["background_started"] = payload["background_enabled"]
    return jsonify({"settings": payload, "background_started": payload["background_enabled"]})


@app.route("/api/update/reset", methods=["POST"])
def api_update_reset():
    background_cancel_event.set()
    reset_update_progress()
    log_update_message({'type': 'info', 'message': '🔄 Update state reset by user', 'progress': 0})
    return jsonify({"status": "reset"})


@app.route("/api/update/run-analysis", methods=["POST"])
def api_update_run_analysis():
    global update_worker_thread
    if update_process_active:
        return jsonify({"error": "Update is already running"}), 409

    body = request.get_json(silent=True) or {}
    force_update = bool(body.get("force") or body.get("force_update"))
    reset_update_progress()
    background_cancel_event.clear()

    def _runner():
        try:
            run_full_update_async(force_update=force_update)
        finally:
            background_cancel_event.clear()

    update_worker_thread = threading.Thread(target=_runner, daemon=True, name="manual-update-runner")
    update_worker_thread.start()
    return jsonify({"status": "started"})


@app.route("/api/update/progress")
def api_update_progress():
    def event_stream():
        for entry in list(update_progress_history):
            yield f"data: {json.dumps(entry)}\n\n"
        while True:
            try:
                message = update_progress_queue.get(timeout=15)
                yield f"data: {json.dumps(message)}\n\n"
                if message.get("type") in {"complete", "error"}:
                    break
            except queue.Empty:
                heartbeat = {"type": "heartbeat", "ts": datetime.utcnow().isoformat() + "Z"}
                yield f"data: {json.dumps(heartbeat)}\n\n"
                if not update_process_active:
                    break
    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "X-Accel-Buffering": "no",
    }
    return Response(stream_with_context(event_stream()), headers=headers)


@app.route("/api/update/git-pull", methods=["POST"])
def api_update_git_pull():
    body = request.get_json(silent=True) or {}
    force_update = bool(body.get("force") or body.get("force_update"))
    success = run_git_pull_all(force_update=force_update)
    return jsonify({"success": bool(success)})


@app.route("/api/update/logs/download")
def api_update_logs_download():
    if not os.path.isfile(UPDATE_LOG_FILE):
        abort(404, description="Update log not found")
    directory = os.path.dirname(UPDATE_LOG_FILE)
    filename = os.path.basename(UPDATE_LOG_FILE)
    return send_from_directory(directory, filename, mimetype="text/plain", as_attachment=True)


@app.route("/api/stats/check")
def api_stats_check():
    exists = os.path.isdir(STATS_ROOT)
    latest_run = (load_update_settings().get("last_update") or {}).get("timestamp")
    return jsonify({
        "stats_available": exists,
        "last_update": latest_run,
        "stats_path": STATS_ROOT,
    })


@app.route("/api/subsystems/size-rankings")
def api_subsystems_size_rankings():
    return jsonify(compute_subsystem_size_rankings(STATS_ROOT))


@app.route("/api/subsystems/<subsystem_name>/significant-ownership")
def api_subsystem_significant_ownership(subsystem_name: str):
    data = compute_subsystem_significant_ownership(STATS_ROOT, subsystem_name)
    return jsonify(data)


@app.route("/api/subsystems/<subsystem_name>/top-maintainers")
def api_subsystem_top_maintainers(subsystem_name: str):
    data = compute_subsystem_top_maintainers(STATS_ROOT, subsystem_name)
    return jsonify(data)


@app.route("/api/subsystems/<subsystem_name>/languages")
def api_subsystem_languages(subsystem_name: str):
    entry = _find_subsystem_entry(subsystem_name)
    payload = _normalize_language_payload(_load_subsystem_language_snapshot(entry))
    return jsonify(payload)


@app.route("/api/subsystems/language-lines")
def api_subsystems_language_lines():
    manifest_getter = getattr(subsystem_metrics, "_get_subsystem_entries", None)
    totals: Dict[str, int] = defaultdict(int)
    if callable(manifest_getter):
        manifest = manifest_getter(STATS_ROOT)
        for entry in manifest.get("entries", []):
            snapshot = _normalize_language_payload(_load_subsystem_language_snapshot(entry))
            for lang, stats in snapshot.get("languages", {}).items():
                totals[lang] += int(stats.get("code_lines") or stats.get("code") or stats.get("lines") or 0)
    return jsonify({"languages": totals, "language_count": len(totals)})


@app.route("/api/subsystems/<subsystem_name>/loc-evolution/<int:year>")
def api_subsystem_loc_evolution(subsystem_name: str, year: int):
    entry = _find_subsystem_entry(subsystem_name)
    series = _build_subsystem_loc_series(entry, year)
    return jsonify({"series": series})


@app.route("/api/subsystems/<subsystem_name>/year/<int:year>")
def api_subsystem_year(subsystem_name: str, year: int):
    data = _load_subsystem_summary(subsystem_name, f"{year:04d}-01-01", f"{year:04d}-12-31", True)
    if not data:
        abort(404, description="Subsystem yearly summary not found")
    return jsonify(data)


@app.route("/api/subsystems/<subsystem_name>/month/<from_date>/<to_date>")
def api_subsystem_month(subsystem_name: str, from_date: str, to_date: str):
    data = _load_subsystem_summary(subsystem_name, from_date, to_date, False)
    if not data:
        abort(404, description="Subsystem period summary not found")
    return jsonify(data)


@app.route("/api/pagerduty/overview")
def api_pagerduty_overview():
    return jsonify(load_json(PAGERDUTY_OVERVIEW_FILE, default={}))


@app.route("/api/pagerduty/incidents")
def api_pagerduty_incidents():
    limit = request.args.get("limit", default=200, type=int) or 200
    limit = max(1, min(limit, 1000))
    responder_id = request.args.get("responder_id", type=str)
    if responder_id:
        responder_id = responder_id.strip()

    if not os.path.exists(PAGERDUTY_INCIDENTS_FILE):
        return jsonify({"incidents": [], "total": 0, "error": "PagerDuty data unavailable."}), 404

    try:
        incidents = load_json(PAGERDUTY_INCIDENTS_FILE, default=[])
    except Exception as exc:  # pragma: no cover - filesystem errors
        logger.error("Failed to load PagerDuty incidents: %s", exc)
        return jsonify({"incidents": [], "total": 0, "error": "Unable to read PagerDuty incidents."}), 500

    if not isinstance(incidents, list):
        incidents = []

    if responder_id:
        def _matches_user(ref: Optional[Dict[str, Any]]) -> bool:
            return bool(ref and str(ref.get("id")) == responder_id)

        def _legacy_matched_events(incident: Dict[str, Any]) -> List[Dict[str, Any]]:
            events: List[Dict[str, Any]] = []
            status = (incident.get("status") or "").lower()
            if status == "resolved" and _matches_user(incident.get("last_status_change_by")):
                events.append(
                    {
                        "role": "resolved",
                        "at": incident.get("resolved_at")
                        or incident.get("last_status_change_at")
                        or incident.get("updated_at"),
                    }
                )
            for acknowledgement in incident.get("acknowledgements") or []:
                if _matches_user(acknowledgement.get("acknowledger")):
                    events.append({"role": "acknowledged", "at": acknowledgement.get("at") or acknowledgement.get("created_at")})
                    break
            for assignment in incident.get("assignments") or []:
                if _matches_user(assignment.get("assignee")):
                    events.append({"role": "assigned", "at": assignment.get("at") or assignment.get("created_at")})
                    break
            events = [event for event in events if event.get("role")]
            events.sort(key=lambda item: item.get("at") or "")
            return events

        def _extract_matched_events(incident: Dict[str, Any]) -> List[Dict[str, Any]]:
            events: List[Dict[str, Any]] = []
            for event in incident.get("responder_events") or []:
                if str(event.get("user_id")) != responder_id:
                    continue
                role = (event.get("role") or "").lower()
                if role not in {"assigned", "acknowledged", "resolved"}:
                    continue
                entry = {"role": role}
                if event.get("at"):
                    entry["at"] = event["at"]
                events.append(entry)
            if events:
                events.sort(key=lambda item: item.get("at") or "")
                return events
            return _legacy_matched_events(incident)

        filtered: List[Dict[str, Any]] = []
        for incident in incidents:
            matched_events = _extract_matched_events(incident)
            if matched_events:
                incident_copy = dict(incident)
                incident_copy.pop("responder_events", None)
                incident_copy["matched_events"] = matched_events
                incident_copy["matched_roles"] = sorted({event["role"] for event in matched_events if event.get("role")})
                filtered.append(incident_copy)
        incidents = filtered

    sorted_incidents = sorted(
        incidents,
        key=lambda item: item.get("created_at") or item.get("updated_at") or "",
        reverse=True,
    )
    return jsonify({"incidents": sorted_incidents[:limit], "total": len(sorted_incidents)})


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global storage for clone progress
clone_operations = {}

# Global queue for update progress messages
update_progress_queue = queue.Queue()
update_progress_history = deque(maxlen=500)
update_process_active = False
update_worker_thread: Optional[threading.Thread] = None

# Update log file
UPDATE_LOG_FILE = os.path.join(BASE_DIR, "update_logs.txt")
INTEGRATIONS_FILE = os.path.join(BASE_DIR, "configuration", "integrations.json")
KIOSK_CONFIG_FILE = os.path.join(BASE_DIR, "configuration", "kiosk_config.json")

def start_new_update_log() -> None:
    try:
        timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with open(UPDATE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"\n===== Update started at {timestamp} =====\n")
    except Exception:
        pass

def reset_update_progress() -> None:
    global update_process_active
    update_process_active = False
    with update_progress_queue.mutex:
        update_progress_queue.queue.clear()
    update_progress_history.clear()


def log_update_message(message_dict):
    """Log update messages to both queue and persistent file."""
    # Track history for late subscribers and enqueue for SSE
    update_progress_history.append(message_dict)
    update_progress_queue.put(message_dict)
    
    # Also write to log file with timestamp
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg_type = message_dict.get('type', 'info').upper()
        message = message_dict.get('message', '')
        progress = message_dict.get('progress', 0)
        
        log_entry = f"[{timestamp}] [{msg_type}] [{progress:.1f}%] {message}\n"
        
        with open(UPDATE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"Error writing to update log: {e}")

LOG_SNIPPET_CHAR_LIMIT = 1200


UPDATE_SETTINGS_FILE = os.path.join(BASE_DIR, "configuration", "update_settings.json")
DEFAULT_UPDATE_SETTINGS = {
    "background_enabled": False,
    "interval_hours": 24,
    "last_update": None,
    "last_background_completed_at": None,
    "last_manual_completed_at": None,
}

DEFAULT_KIOSK_CONFIG = {
    "rotation_seconds": 30,
    "refresh_minutes": 15,
    "pages": []
}

VALID_KIOSK_LAYOUTS = {"grid", "vertical", "horizontal"}

update_settings_lock = threading.Lock()
background_scheduler_event = threading.Event()
background_scheduler_stop_event = threading.Event()
background_scheduler_thread: Optional[threading.Thread] = None
background_state = {
    "running": False,
    "next_run": None,
}

background_state_lock = threading.Lock()
background_cancel_event = threading.Event()


def _ensure_known_hosts_file(path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8"):
            pass
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    return path


def _is_ssh_repo_url(url: str) -> bool:
    if not url:
        return False
    return url.startswith("git@") or url.startswith("ssh://")


def _build_git_clone_env(repo_url: str) -> Dict[str, str]:
    env = os.environ.copy()
    env["GIT_PROGRESS_DELAY"] = "1"
    if _is_ssh_repo_url(repo_url):
        known_hosts_path = _ensure_known_hosts_file(SSH_KNOWN_HOSTS_FILE)
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"UserKnownHostsFile={known_hosts_path}",
        ]
        env["GIT_SSH_COMMAND"] = " ".join(shlex.quote(part) for part in ssh_cmd)
    return env


def get_background_state_snapshot() -> Dict[str, Any]:
    with background_state_lock:
        return {
            "running": background_state.get("running", False),
            "next_run": background_state.get("next_run")
        }

def _format_command(cmd: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)

def _summarize_stream(text: Optional[str], limit: int = LOG_SNIPPET_CHAR_LIMIT) -> str:
    if not text:
        return ""
    text = text.strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return f"…{text[-limit:]}"

def _log_subprocess_streams(label: str, stdout: Optional[str], stderr: Optional[str], progress: float) -> None:
    stdout_summary = _summarize_stream(stdout)
    if stdout_summary:
        log_update_message({
            'type': 'info',
            'message': f"{label} STDOUT (tail):\n{stdout_summary}",
            'progress': progress
        })
    stderr_summary = _summarize_stream(stderr)
    if stderr_summary:
        log_update_message({
            'type': 'info',
            'message': f"{label} STDERR (tail):\n{stderr_summary}",
            'progress': progress
        })

def _log_command_start(label: str, cmd: List[str], progress: float) -> None:
    log_update_message({
        'type': 'info',
        'message': f"{label}: starting command {_format_command(cmd)}",
        'progress': progress
    })


def _run_command_with_live_logs(
    label: str,
    cmd: List[str],
    cwd: Optional[str],
    progress: float,
    timeout: Optional[int] = None,
    env: Optional[Dict[str, str]] = None,
    line_handler: Optional[Callable[[str, str], bool]] = None,
) -> Tuple[int, str, str]:
    """Run a subprocess and stream its output into the progress queue."""
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env
    )

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []

    def _reader(stream, stream_name: str, collector: List[str]):
        try:
            for line in iter(stream.readline, ''):
                collector.append(line)
                stripped = line.rstrip()
                if not stripped:
                    continue
                handled = False
                if line_handler:
                    try:
                        handled = line_handler(stripped, stream_name)
                    except Exception:
                        handled = False
                if handled:
                    continue
                timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"
                log_update_message({
                    'type': 'detail',
                    'message': f"{timestamp} {stripped}",
                    'progress': progress
                })
        finally:
            stream.close()

    threads: List[threading.Thread] = []
    for stream, name, collector in (
        (process.stdout, 'stdout', stdout_lines),
        (process.stderr, 'stderr', stderr_lines),
    ):
        if stream is not None:
            reader_thread = threading.Thread(target=_reader, args=(stream, name, collector), daemon=True)
            reader_thread.start()
            threads.append(reader_thread)

    try:
        return_code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        raise
    finally:
        for reader_thread in threads:
            reader_thread.join(timeout=1)

    return return_code, ''.join(stdout_lines), ''.join(stderr_lines)



def ensure_update_settings_file() -> None:
    os.makedirs(os.path.dirname(UPDATE_SETTINGS_FILE), exist_ok=True)
    if not os.path.exists(UPDATE_SETTINGS_FILE):
        with open(UPDATE_SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_UPDATE_SETTINGS, f, indent=2)


def load_update_settings() -> Dict[str, Any]:
    ensure_update_settings_file()
    with update_settings_lock:
        try:
            with open(UPDATE_SETTINGS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {}
        merged = copy.deepcopy(DEFAULT_UPDATE_SETTINGS)
        merged.update(data or {})
        interval = merged.get('interval_hours', 24)
        try:
            interval = int(interval)
        except (TypeError, ValueError):
            interval = 24
        merged['interval_hours'] = max(1, interval)
        return merged


def save_update_settings(settings: Dict[str, Any]) -> None:
    ensure_update_settings_file()
    with update_settings_lock:
        with open(UPDATE_SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=2)


def ensure_kiosk_config_file() -> None:
    os.makedirs(os.path.dirname(KIOSK_CONFIG_FILE), exist_ok=True)
    if not os.path.exists(KIOSK_CONFIG_FILE):
        with open(KIOSK_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_KIOSK_CONFIG, f, indent=2)


def _sanitize_kiosk_item(item: Dict[str, Any], page_id: str, index: int) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    visualization_id = item.get('visualization_id')
    if not visualization_id:
        return None
    entry = {
        'id': item.get('id') or f"{page_id}-item-{index + 1}",
        'visualization_id': visualization_id,
        'scope': item.get('scope'),
        'entity_id': item.get('entity_id'),
        'entity_label': item.get('entity_label'),
        'period_mode': item.get('period_mode') or 'latest-year',
        'period': item.get('period') if isinstance(item.get('period'), dict) else None,
        'custom_title': item.get('custom_title'),
        'options': item.get('options') if isinstance(item.get('options'), dict) else {},
        'notes': item.get('notes') or ''
    }
    return entry


def _sanitize_kiosk_page(page: Dict[str, Any], index: int) -> Dict[str, Any]:
    if not isinstance(page, dict):
        page = {}
    page_id = page.get('id') or f"page-{index + 1}"
    title = (page.get('title') or '').strip() or f"Page {index + 1}"
    description = (page.get('description') or '').strip()
    layout = (page.get('layout') or 'grid').strip().lower() or 'grid'
    if layout not in VALID_KIOSK_LAYOUTS:
        layout = 'grid'
    raw_items = page.get('items') if isinstance(page.get('items'), list) else []
    items: List[Dict[str, Any]] = []
    for item_index, raw in enumerate(raw_items):
        sanitized_item = _sanitize_kiosk_item(raw, page_id, item_index)
        if sanitized_item:
            items.append(sanitized_item)
    return {
        'id': page_id,
        'title': title,
        'description': description,
        'layout': layout,
        'items': items
    }


def _prepare_kiosk_pages_structure(source: Any) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if isinstance(source, list):
        candidates = source
    elif isinstance(source, dict):
        if isinstance(source.get('pages'), list):
            candidates = source.get('pages') or []
        elif isinstance(source.get('items'), list) and source.get('items'):
            candidates = [{
                'id': source.get('id') or 'page-1',
                'title': source.get('title') or 'Slide 1',
                'description': source.get('description') or '',
                'layout': source.get('layout') or 'grid',
                'items': source.get('items')
            }]
    pages: List[Dict[str, Any]] = []
    for idx, page in enumerate(candidates):
        pages.append(_sanitize_kiosk_page(page, idx))
    return pages


def _flatten_kiosk_items(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for page in pages:
        flattened.extend(page.get('items') or [])
    return flattened


def load_kiosk_config() -> Dict[str, Any]:
    ensure_kiosk_config_file()
    try:
        with open(KIOSK_CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        data = {}
    merged = copy.deepcopy(DEFAULT_KIOSK_CONFIG)
    incoming = data or {}
    merged.update({k: v for k, v in incoming.items() if k not in {'pages', 'items'}})
    merged['rotation_seconds'] = max(5, int(merged.get('rotation_seconds', 30) or 30))
    merged['refresh_minutes'] = max(1, int(merged.get('refresh_minutes', 15) or 15))
    pages = _prepare_kiosk_pages_structure(incoming)
    merged['pages'] = pages
    merged['items'] = _flatten_kiosk_items(pages)
    return merged


def save_kiosk_config(config: Dict[str, Any]) -> Dict[str, Any]:
    ensure_kiosk_config_file()
    sanitized = load_kiosk_config()
    incoming = config or {}
    sanitized.update({
        'rotation_seconds': max(5, int(incoming.get('rotation_seconds', sanitized['rotation_seconds']))),
        'refresh_minutes': max(1, int(incoming.get('refresh_minutes', sanitized['refresh_minutes'])))
    })
    pages = _prepare_kiosk_pages_structure(incoming)
    sanitized['pages'] = pages
    sanitized['items'] = _flatten_kiosk_items(pages)
    with open(KIOSK_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(sanitized, f, indent=2)
    return sanitized


@app.route("/api/settings/kiosk", methods=["GET", "POST"])
def api_settings_kiosk():
    if request.method == "GET":
        return jsonify(load_kiosk_config())
    config = request.get_json(silent=True) or {}
    saved = save_kiosk_config(config)
    return jsonify({"config": saved})


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith('Z'):
            value = value[:-1]
            dt = datetime.fromisoformat(value)
            return dt.replace(tzinfo=timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def record_last_update(status: str, update_type: str) -> None:
    try:
        settings = load_update_settings()
        now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        settings['last_update'] = {
            'timestamp': now_iso,
            'status': status,
            'type': update_type,
        }
        if update_type == 'background':
            settings['last_background_completed_at'] = now_iso
        elif update_type == 'manual':
            settings['last_manual_completed_at'] = now_iso
        save_update_settings(settings)
    finally:
        schedule_background_check()


def schedule_background_check() -> None:
    background_scheduler_event.set()


def _format_utc_timestamp(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat() + "Z"


def _compute_next_run_time(settings: Dict[str, Any]) -> datetime:
    interval_hours = settings.get("interval_hours", 24)
    try:
        interval_hours = max(1, int(interval_hours))
    except (TypeError, ValueError):
        interval_hours = 24
    last_completed_iso = settings.get("last_background_completed_at")
    if not last_completed_iso:
        last_completed_iso = (settings.get("last_update") or {}).get("timestamp")
    last_completed = parse_timestamp(last_completed_iso)
    if last_completed is None:
        # Run as soon as possible when no history is available
        return datetime.utcnow()
    return last_completed + timedelta(hours=interval_hours)


def _update_background_state(running: bool, next_run: Optional[datetime]) -> None:
    with background_state_lock:
        background_state["running"] = running
        background_state["next_run"] = _format_utc_timestamp(next_run) if next_run else None


def background_scheduler_loop() -> None:
    background_cancel_event.clear()
    while not background_scheduler_stop_event.is_set():
        settings = load_update_settings()
        if not settings.get("background_enabled", False):
            _update_background_state(False, None)
            background_scheduler_event.wait(timeout=300)
            background_scheduler_event.clear()
            continue

        next_run = _compute_next_run_time(settings)
        _update_background_state(False, next_run)
        wait_seconds = max(0.0, (next_run - datetime.utcnow()).total_seconds())
        triggered = background_scheduler_event.wait(timeout=wait_seconds)
        if triggered:
            background_scheduler_event.clear()
            continue
        if background_scheduler_stop_event.is_set():
            break
        if update_process_active:
            # Manual run in progress; re-check soon
            time.sleep(10)
            continue
        background_cancel_event.clear()
        _update_background_state(True, next_run)
        try:
            perform_background_update('scheduled')
        finally:
            schedule_background_check()


def start_background_scheduler() -> None:
    global background_scheduler_thread
    if background_scheduler_thread and background_scheduler_thread.is_alive():
        return
    background_scheduler_stop_event.clear()
    background_scheduler_event.set()
    background_scheduler_thread = threading.Thread(
        target=background_scheduler_loop,
        name="background-update-scheduler",
        daemon=True,
    )
    background_scheduler_thread.start()


def build_low_priority_command(base_cmd: List[str]) -> List[str]:
    cmd = list(base_cmd)
    ionice_path = shutil.which('ionice')
    nice_path = shutil.which('nice')
    cpulimit_path = shutil.which('cpulimit')
    if ionice_path:
        cmd = [ionice_path, '-c', '3'] + cmd
    if nice_path:
        cmd = [nice_path, '-n', '19'] + cmd
    if cpulimit_path:
        cmd = [cpulimit_path, '-l', '30', '--'] + cmd
    return cmd


def swap_stats_directories(temp_output_root: str) -> None:
    temp_stats = os.path.join(temp_output_root, 'stats')
    if not os.path.exists(temp_stats):
        raise RuntimeError('Temporary stats directory not found after background update')
    final_stats = os.path.join(BASE_DIR, 'stats')
    backup_stats = os.path.join(BASE_DIR, 'stats_backup')
    if os.path.exists(backup_stats):
        shutil.rmtree(backup_stats, ignore_errors=True)
    moved_backup = False
    try:
        if os.path.exists(final_stats):
            os.rename(final_stats, backup_stats)
            moved_backup = True
        os.rename(temp_stats, final_stats)
    except Exception:
        if moved_backup and not os.path.exists(final_stats) and os.path.exists(backup_stats):
            os.rename(backup_stats, final_stats)
        raise
    finally:
        if os.path.exists(backup_stats):
            shutil.rmtree(backup_stats, ignore_errors=True)


def build_master_command(
    months: int,
    output_dir: str,
    python_exe: Optional[str] = None,
    parallel: Optional[int] = None,
) -> List[str]:
    python_path = python_exe or sys.executable or "python3"
    repos_dir = os.path.join(BASE_DIR, "repos")
    config_dir = os.path.join(BASE_DIR, "configuration")
    os.makedirs(output_dir, exist_ok=True)
    cmd = [
        python_path,
        os.path.join(BASE_DIR, "master.py"),
        "--months",
        str(months),
        "--repos-dir",
        repos_dir,
        "--config-dir",
        config_dir,
        "--output-dir",
        output_dir,
    ]
    if parallel and parallel > 0:
        cmd.extend(["--parallel", str(parallel)])
    cmd.append("--progress-events")
    return cmd


def preserve_pagerduty_cache(target_stats_root: str) -> None:
    source_dir = os.path.join(BASE_DIR, "stats", "pagerduty")
    if not os.path.exists(source_dir):
        return
    destination = os.path.join(target_stats_root, "pagerduty")
    overview_path = os.path.join(destination, "overview.json")
    if os.path.isfile(overview_path):
        return
    if os.path.exists(destination):
        shutil.rmtree(destination, ignore_errors=True)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    shutil.copytree(source_dir, destination)


def perform_background_update(reason: str = 'scheduled') -> bool:
    success = False
    temp_dir: Optional[str] = None
    temp_output_root: Optional[str] = None
    try:
        if background_cancel_event.is_set():
            logger.info('Background update cancelled before start')
            return False
        logger.info('Starting background update (%s)', reason)
        if not run_git_pull_all(False):
            logger.warning('Background update aborted: repository scan failed')
            return False
        if background_cancel_event.is_set():
            logger.info('Background update cancelled after repository validation')
            return False
        temp_dir = tempfile.mkdtemp(prefix='background-update-', dir=BASE_DIR)
        temp_output_root = os.path.join(temp_dir, 'output')
        os.makedirs(temp_output_root, exist_ok=True)
        temp_stats_root = os.path.join(temp_output_root, 'stats')
        python_exe = sys.executable or 'python3'
        master_script = os.path.join(BASE_DIR, 'master.py')
        if not os.path.exists(master_script):
            logger.error('master.py not found, aborting background update')
            return False
        python_env = os.environ.copy()
        python_env.setdefault('PYTHONUNBUFFERED', '1')
        if background_cancel_event.is_set():
            logger.info('Background update cancelled before analysis phase')
            return False
        rolling_months = get_default_rolling_months()
        base_cmd = build_master_command(rolling_months, temp_stats_root, python_exe, None)
        cmd = build_low_priority_command(base_cmd)
        logger.info('Background update running master.py (rolling %s months)', rolling_months)
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            text=True,
            capture_output=True,
            timeout=144000,
            env=python_env,
        )
        if result.returncode != 0:
            logger.error('Background update failed: %s', (result.stderr or result.stdout or '').strip())
            return False
        preserve_pagerduty_cache(temp_stats_root)
        swap_stats_directories(temp_output_root)
        logger.info('Background update completed successfully')
        success = True
        return True
    finally:
        if temp_dir and os.path.isdir(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info('Background update cleaned up temporary workspace: %s', temp_dir)
            except Exception as e:
                logger.warning('Failed to cleanup background update directory %s: %s', temp_dir, e)
            if not success:
                logger.info('Background update cleaned up temporary workspace after failure')
        record_last_update('success' if success else 'failed', 'background')



def run_full_update_async(force_update=False):
    """Run complete update process (git pull + master.py) with progress reporting."""
    global update_process_active

    update_process_active = True
    overall_success = False
    temp_dir: Optional[str] = None
    temp_output_root: Optional[str] = None

    # Start a new log section
    start_new_update_log()

    pagerduty_enabled = is_pagerduty_configured()
    analysis_progress_floor = 12.0
    analysis_progress_cap = 95.0 if pagerduty_enabled else 100.0
    if analysis_progress_cap < analysis_progress_floor:
        analysis_progress_cap = analysis_progress_floor
    analysis_progress_span = max(analysis_progress_cap - analysis_progress_floor, 1.0)
    analysis_progress_value = analysis_progress_floor

    def _resolved_progress(target: float) -> float:
        nonlocal analysis_progress_value
        analysis_progress_value = max(analysis_progress_value, target)
        return analysis_progress_value

    def master_progress_handler(line: str, _stream: str) -> bool:
        nonlocal analysis_progress_value
        if not line.startswith(MASTER_PROGRESS_PREFIX):
            return False
        payload_raw = line[len(MASTER_PROGRESS_PREFIX):].strip()
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            return False
        steps_completed = payload.get('steps_completed')
        total_steps = payload.get('total_steps')
        if (
            not isinstance(steps_completed, (int, float))
            or not isinstance(total_steps, (int, float))
            or total_steps <= 0
        ):
            return False
        ratio = max(0.0, min(1.0, float(steps_completed) / float(total_steps)))
        progress_value = analysis_progress_floor + ratio * analysis_progress_span
        progress_value = min(analysis_progress_cap, progress_value)
        analysis_progress_value = max(analysis_progress_value, progress_value)
        label = payload.get('label') or 'Analysis progress updated'
        log_update_message({
            'type': 'info',
            'message': f'📈 {label}',
            'progress': analysis_progress_value
        })
        return True

    try:
        start_timestamp = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'🚀 Starting update process... [{start_timestamp.strftime("%H:%M:%S")}]',
            'progress': 0
        })

        if not run_git_pull_all(force_update):
            log_update_message({
                'type': 'error',
                'message': '❌ Repository validation failed. Aborting update.',
                'progress': 5
            })
            return

        python_exe = sys.executable or 'python3'
        master_script = os.path.join(BASE_DIR, 'master.py')
        if not os.path.exists(master_script):
            log_update_message({
                'type': 'error',
                'message': '❌ master.py script not found. Cannot continue.',
                'progress': 5
            })
            return

        temp_dir = tempfile.mkdtemp(prefix='manual-update-', dir=BASE_DIR)
        temp_output_root = os.path.join(temp_dir, 'output')
        os.makedirs(temp_output_root, exist_ok=True)
        temp_stats_root = os.path.join(temp_output_root, 'stats')

        months_to_process = get_default_rolling_months()
        auto_parallel = _detect_parallel_default()
        parallel_override: Optional[int] = None
        python_env = os.environ.copy()
        python_env.setdefault('PYTHONUNBUFFERED', '1')

        master_cmd = build_master_command(months_to_process, temp_stats_root, python_exe, parallel_override)
        log_update_message({
            'type': 'info',
            'message': f'🧮 Running master.py for last {months_to_process} months (parallel=auto ≈{auto_parallel})...',
            'progress': 10
        })
        _log_command_start('master.py rolling window', master_cmd, 12)

        analysis_start = datetime.now()
        try:
            returncode, stdout_text, stderr_text = _run_command_with_live_logs(
                'master.py rolling window',
                master_cmd,
                cwd=BASE_DIR,
                progress=12,
                timeout=144000,
                env=python_env,
                line_handler=master_progress_handler,
            )
        except subprocess.TimeoutExpired:
            log_update_message({
                'type': 'error',
                'message': '❌ master.py timed out. Update aborted.',
                'progress': 60
            })
            return

        if returncode != 0:
            log_update_message({
                'type': 'error',
                'message': f'❌ master.py failed with exit code {returncode}',
                'progress': 60
            })
            _log_subprocess_streams('master.py rolling window', stdout_text, stderr_text, 60)
            return

        analysis_end = datetime.now()
        analysis_duration = (analysis_end - analysis_start).total_seconds()
        log_update_message({
            'type': 'info',
            'message': f'✅ master.py completed in {analysis_duration:.0f}s',
            'progress': _resolved_progress(90)
        })

        preserve_pagerduty_cache(temp_stats_root)
        swap_stats_directories(temp_output_root)
        overall_success = True
        log_update_message({
            'type': 'info',
            'message': '📂 New statistics swapped into place.',
            'progress': _resolved_progress(93)
        })

        try:
            log_update_message({
                'type': 'info',
                'message': '🏅 Refreshing badge cache...',
                'progress': _resolved_progress(95)
            })
            badge_cache = refresh_badge_cache()
            if badge_cache and badge_cache.get('summary'):
                badge_count = badge_cache['summary'].get('users_with_badges', 0)
                log_update_message({
                    'type': 'info',
                    'message': f'✅ Badge cache updated ({badge_count} developers with badges)',
                    'progress': _resolved_progress(96)
                })
            else:
                log_update_message({
                    'type': 'warning',
                    'message': '⚠️ Badge cache refresh produced no data. UI will rebuild on demand.',
                    'progress': _resolved_progress(96)
                })
        except Exception as cache_exc:
            log_update_message({
                'type': 'warning',
                'message': f'⚠️ Failed to refresh badge cache: {cache_exc}',
                'progress': _resolved_progress(96)
            })

        final_end_time = datetime.now()
        total_duration = (final_end_time - start_timestamp).total_seconds()
        log_update_message({
            'type': 'info',
            'message': f'🎉 Update completed successfully! [{final_end_time.strftime("%H:%M:%S")}] (total duration: {total_duration:.0f}s)',
            'progress': _resolved_progress(100)
        })
        log_update_message({
            'type': 'complete',
            'message': 'Update process finished successfully.',
            'progress': _resolved_progress(100)
        })

    except Exception as e:
        final_error_time = datetime.now()
        log_update_message({
            'type': 'error',
            'message': f'❌ Update process failed: {str(e)} [{final_error_time.strftime("%H:%M:%S")}]',
            'progress': _resolved_progress(100)
        })
    finally:
        if temp_dir and os.path.isdir(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info('Manual update cleaned up temporary workspace: %s', temp_dir)
            except Exception as e:
                logger.warning('Failed to cleanup manual update directory %s: %s', temp_dir, e)
        update_process_active = False
        record_last_update('success' if overall_success else 'failed', 'manual')


def run_git_pull_all(force_update=False):
    """Run git pull on all repositories and report progress."""
    try:
        import subprocess
        
        start_time = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'[{start_time.strftime("%H:%M:%S")}] Getting repository list...',
            'progress': 1
        })
        
        repos_root = os.path.join(BASE_DIR, "repos")
        if not os.path.exists(repos_root):
            error_time = datetime.now()
            log_update_message({
                'type': 'error',
                'message': f'[{error_time.strftime("%H:%M:%S")}] ❌ No repos directory found',
                'progress': 1
            })
            return False
        
        # Count repositories first
        repo_list = []
        for org_dir in os.listdir(repos_root):
            org_path = os.path.join(repos_root, org_dir)
            if not os.path.isdir(org_path):
                continue
                
            for repo_dir in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_dir)
                git_dir = os.path.join(repo_path, ".git")
                
                if os.path.isdir(repo_path) and os.path.exists(git_dir):
                    repo_name = f"{org_dir}/{repo_dir}"
                    repo_list.append((repo_name, repo_path))
        
        if not repo_list:
            error_time = datetime.now()
            log_update_message({
                'type': 'error',
                'message': f'[{error_time.strftime("%H:%M:%S")}] ❌ No git repositories found',
                'progress': 1
            })
            return False
        
        # Process repositories
        success_count = 0
        for i, (repo_name, repo_path) in enumerate(repo_list):
            # Calculate progress (1% to 5% for git operations)
            progress = 1 + int((i / len(repo_list)) * 4)
            repo_start_time = datetime.now()

            log_update_message({
                'type': 'info',
                'message': f'[{repo_start_time.strftime("%H:%M:%S")}] 🔄 {repo_name}: Fetching remote updates...',
                'progress': progress
            })

            try:
                fetch_result = subprocess.run(
                    ["git", "fetch", "--all", "--prune"],
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
            except subprocess.TimeoutExpired:
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git fetch timed out',
                    'progress': progress
                })
                continue
            except Exception as exc:
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git fetch failed ({exc})',
                    'progress': progress
                })
                continue

            if fetch_result.returncode != 0:
                summary = (fetch_result.stderr or fetch_result.stdout or "git fetch failed").strip()
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git fetch failed ({summary})',
                    'progress': progress
                })
                continue

            try:
                pull_result = subprocess.run(
                    ["git", "pull", "--ff-only"],
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
            except subprocess.TimeoutExpired:
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git pull timed out',
                    'progress': progress
                })
                continue
            except Exception as exc:
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git pull failed ({exc})',
                    'progress': progress
                })
                continue

            if pull_result.returncode != 0:
                summary = (pull_result.stderr or pull_result.stdout or "git pull failed").strip()
                log_update_message({
                    'type': 'warning',
                    'message': f'[{datetime.now().strftime("%H:%M:%S")}] ⚠️ {repo_name}: git pull failed ({summary})',
                    'progress': progress
                })
                continue

            summary = (pull_result.stdout or pull_result.stderr or "Already up to date.").strip()
            if "\n" in summary:
                summary = summary.splitlines()[-1].strip()
            if not summary:
                summary = "Already up to date."

            log_update_message({
                'type': 'success',
                'message': f'[{datetime.now().strftime("%H:%M:%S")}] ✅ {repo_name}: {summary}',
                'progress': progress
            })
            success_count += 1

        return success_count > 0  # Return True if at least one repo was updated successfully
        
    except Exception as e:
        error_time = datetime.now()
        log_update_message({
            'type': 'error',
            'message': f'[{error_time.strftime("%H:%M:%S")}] ❌ Git pull failed: {str(e)}',
            'progress': 1
        })
        return False



def cleanup_orphaned_temp_directories():
    """Clean up leftover temporary directories from previous runs."""
    try:
        pattern = os.path.join(BASE_DIR, "background-update-*")
        temp_dirs = glob.glob(pattern)
        manual_pattern = os.path.join(BASE_DIR, "manual-update-*")
        temp_dirs.extend(glob.glob(manual_pattern))

        cleaned_count = 0
        for temp_dir in temp_dirs:
            if os.path.isdir(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logger.info('Startup cleanup: removed orphaned temp directory %s', temp_dir)
                    cleaned_count += 1
                except Exception as e:
                    logger.warning('Startup cleanup: failed to remove temp directory %s: %s', temp_dir, e)

        if cleaned_count > 0:
            logger.info('Startup cleanup: removed %d orphaned temporary directories', cleaned_count)

    except Exception as e:
        logger.error('Startup cleanup failed: %s', e)


def cleanup_on_shutdown(signum=None, frame=None):
    """Clean up resources when the server shuts down."""
    logger.info('Dashboard server shutting down, cleaning up...')

    # Stop background scheduler
    if background_scheduler_thread and background_scheduler_thread.is_alive():
        logger.info('Stopping background scheduler...')
        background_scheduler_stop_event.set()
        background_scheduler_event.set()
        background_scheduler_thread.join(timeout=5)

    # Cancel any running background update
    background_cancel_event.set()

    # Clean up any remaining temp directories
    try:
        cleanup_orphaned_temp_directories()
    except Exception as e:
        logger.error('Error during shutdown cleanup: %s', e)

    logger.info('Dashboard server shutdown cleanup complete')
    if signum:
        sys.exit(0)


def launch_background_scheduler():
    start_background_scheduler()

if not os.environ.get("DISABLE_DASHBOARD_SCHEDULER"):
    # Clean up any orphaned temp directories from previous runs
    cleanup_orphaned_temp_directories()
    launch_background_scheduler()


@app.route("/api/teams")
def api_teams():
    """Get list of teams with their periods (similar to users/subsystems)."""
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        return jsonify({"teams": []})
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        teams_config = {}
    
    teams = []
    responsibilities = load_team_subsystem_responsibilities()
    user_month_lookup = list_user_months()
    alias_lookup = load_alias_lookup()
    
    for team_id, team_info in teams_config.items():
        responsible_subsystems = responsibilities.get(team_id, [])
        raw_members = team_info.get("members", []) or []
        canonical_members = canonicalize_team_members(raw_members, alias_lookup)
        
        # Get available periods from actual team data files
        team_name = team_info.get("name", team_id)
        team_dir = os.path.join(STATS_ROOT, "teams", team_name)
        team_periods = []
        
        if os.path.exists(team_dir):
            for filename in os.listdir(team_dir):
                if not filename.endswith(".json"):
                    continue
                
                # Parse filename: YYYY-MM.json, YYYY-MM-DD_YYYY-MM-DD.json or YYYY.json
                basename = filename[:-5]  # Remove .json
                
                if "_" in basename:
                    # Monthly file: YYYY-MM-DD_YYYY-MM-DD
                    parts = basename.split("_")
                    if len(parts) == 2:
                        from_date, to_date = parts
                        # Extract year-month for label
                        year_month = from_date[:7]  # YYYY-MM
                        team_periods.append({
                            "from": from_date,
                            "to": to_date,
                            "label": year_month,
                            "is_yearly": False
                        })
                elif "-" in basename and len(basename) == 7:
                    # Monthly file: YYYY-MM
                    year_month = basename
                    team_periods.append({
                        "from": year_month,
                        "to": year_month,
                        "label": year_month,
                        "is_yearly": False
                    })
                elif len(basename) == 4 and basename.isdigit():
                    # Yearly file: YYYY
                    year = basename
                    team_periods.append({
                        "from": year,
                        "to": year,
                        "label": year,
                        "is_yearly": True
                    })
            
            # Sort periods by from date
            team_periods.sort(key=lambda x: x["from"])

        if not team_periods:
            team_periods = build_team_periods_from_members(canonical_members, user_month_lookup)
        
        teams.append({
            "id": team_id,
            "name": team_info.get("name", team_id),
            "description": team_info.get("description", ""),
            "members": canonical_members,
            "configured_members": raw_members,
            "responsible_subsystems": responsible_subsystems,
            "periods": team_periods
        })
    
    return jsonify({"teams": teams})


def build_team_overview_snapshot() -> Dict[str, Any]:
    teams_file_path = os.path.join(BASE_DIR, "configuration", "teams.json")
    if not os.path.exists(teams_file_path):
        return {"teams": [], "period": "Last 3 Months", "generated_at": datetime.utcnow().isoformat() + "Z"}
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        teams_config = {}
    if not teams_config:
        return {"teams": [], "period": "Last 3 Months", "generated_at": datetime.utcnow().isoformat() + "Z"}

    # Always use Last 3 Months period
    current_date = datetime.now()
    three_months_ago = current_date - timedelta(days=90)
    from_date = three_months_ago.strftime("%Y-%m-01")
    to_date = current_date.strftime("%Y-%m-%d")
    period_label = "Last 3 Months"

    alias_lookup = load_alias_lookup()
    size_payload = compute_subsystem_size_rankings(STATS_ROOT)
    subsystem_line_lookup: Dict[str, int] = {}
    subsystem_line_lookup_lower: Dict[str, int] = {}
    rankings = size_payload.get("rankings") if isinstance(size_payload, dict) else {}
    if isinstance(rankings, dict):
        for name, entry in rankings.items():
            if not isinstance(entry, dict):
                continue
            total_lines = int(entry.get("total_lines") or 0)
            subsystem_line_lookup[name] = total_lines
            subsystem_line_lookup_lower[name.lower()] = total_lines

    def resolve_subsystem_lines(subsystem_name: str) -> int:
        if not subsystem_name:
            return 0
        if subsystem_name in subsystem_line_lookup:
            return subsystem_line_lookup[subsystem_name]
        lowered = subsystem_name.lower()
        if lowered in subsystem_line_lookup_lower:
            return subsystem_line_lookup_lower[lowered]
        if "/" in subsystem_name:
            short = subsystem_name.split("/")[-1]
            if short in subsystem_line_lookup:
                return subsystem_line_lookup[short]
            short_lower = short.lower()
            if short_lower in subsystem_line_lookup_lower:
                return subsystem_line_lookup_lower[short_lower]

        # NEW: Try reverse matching - look for full names that end with our subsystem name
        for full_name, lines in subsystem_line_lookup.items():
            if "/" in full_name:
                short_name = full_name.split("/")[-1]
                if short_name == subsystem_name:
                    return lines

        # Try case-insensitive reverse matching
        for full_name, lines in subsystem_line_lookup_lower.items():
            if "/" in full_name:
                short_name = full_name.split("/")[-1].lower()
                if short_name == lowered:
                    return lines

        return 0

    teams_analytics: List[Dict[str, Any]] = []
    for team_id, team_info in teams_config.items():
        team_name = team_info.get("name", team_id)
        members = team_info.get("members", [])
        responsible_subsystems = get_team_responsible_subsystems(team_id)
        canonical_members = canonicalize_team_members(members, alias_lookup)
        team_stats = {
            "id": team_id,
            "name": team_name,
            "description": team_info.get("description", ""),
            "member_count": len(canonical_members),
            "members": canonical_members,
            "responsible_subsystems": responsible_subsystems,
            "responsible_subsystems_count": len(responsible_subsystems),
            "total_commits": 0,
            "total_additions": 0,
            "total_deletions": 0,
            "total_lines_changed": 0,
            "active_subsystems": set(),
            "languages": {},
            "active_months": set(),
        }
        for member in canonical_members:
            member_stats = aggregate_user_data_for_period(member, from_date, to_date)
            if member_stats:
                team_stats["total_commits"] += member_stats.get("total_commits", 0)
                team_stats["total_additions"] += member_stats.get("total_lines_added", member_stats.get("total_additions", 0))
                team_stats["total_deletions"] += member_stats.get("total_lines_deleted", member_stats.get("total_deletions", 0))
                for repo in member_stats.get("per_repo", {}).keys():
                    team_stats["active_subsystems"].add(repo)
                for lang, lang_data in member_stats.get("languages", {}).items():
                    team_stats["languages"].setdefault(lang, 0)
                    team_stats["languages"][lang] += lang_data.get("net_lines", 0)
                for date_str, payload in member_stats.get("per_date", {}).items():
                    if payload.get("commits", 0) > 0:
                        month_key = date_str[:7]
                        team_stats["active_months"].add(month_key)
        team_stats["total_lines_changed"] = team_stats["total_additions"] + team_stats["total_deletions"]
        team_stats["active_subsystems_count"] = len(team_stats["active_subsystems"])
        team_stats["active_months_count"] = len(team_stats["active_months"])
        team_stats["active_subsystems"] = list(team_stats["active_subsystems"])
        team_stats["active_months"] = list(team_stats["active_months"])
        if team_stats["languages"]:
            team_stats["primary_language"] = max(team_stats["languages"], key=team_stats["languages"].get)
        else:
            team_stats["primary_language"] = "N/A"
        teams_analytics.append(team_stats)

    for team_stats in teams_analytics:
        total_responsible_lines = 0
        covered_subsystems = 0
        for subsystem_name in team_stats.get("responsible_subsystems", []):
            lines = resolve_subsystem_lines(subsystem_name)
            if lines > 0:
                total_responsible_lines += lines
                covered_subsystems += 1
        team_stats["responsible_lines_of_code"] = total_responsible_lines
        team_stats["responsible_subsystems_with_stats"] = covered_subsystems

    teams_analytics.sort(key=lambda x: x["total_commits"], reverse=True)
    return {
        "teams": teams_analytics,
        "period": period_label,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


@app.route("/api/teams/<team_id>/month/<from_date>/<to_date>")
@app.route("/api/teams/<team_id>/month/<from_date>")
def api_team_month(team_id: str, from_date: str, to_date: str = None):
    """Get aggregated monthly summary for a team."""
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        abort(404, description="Teams configuration not found")
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        abort(404, description="Invalid teams configuration")
    
    if team_id not in teams_config:
        abort(404, description="Team not found")
    
    team = teams_config[team_id]
    team_name = team.get("name", team_id)
    
    # If to_date is None, from_date is in YYYY-MM format, try to load the file directly
    if to_date is None or from_date == to_date:
        # Try loading from YYYY-MM.json format
        month_str = from_date if to_date is None else from_date
        team_file = os.path.join(STATS_ROOT, "teams", team_name, f"{month_str}.json")
        
        if os.path.exists(team_file):
            try:
                with open(team_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    stored_details = data.get("responsible_subsystem_details", {}) or {}
                    configured_responsibilities = get_team_responsible_subsystems(team_id)
                    source_responsibilities = (
                        configured_responsibilities
                        or data.get("responsible_subsystems", [])
                        or list(stored_details.keys())
                    )
                    if not isinstance(source_responsibilities, list):
                        source_responsibilities = list(source_responsibilities)
                    responsible_subsystem_details: Dict[str, Any] = {}
                    total_responsible_lines = 0
                    capacity_languages: Dict[str, int] = {}
                    for subsystem_name in source_responsibilities:
                        languages, subsystem_lines = _resolve_responsible_subsystem_languages(subsystem_name)
                        if not languages and subsystem_name in stored_details:
                            entry = stored_details.get(subsystem_name) or {}
                            languages = entry.get("languages", {})
                            fallback_lines = entry.get("lines")
                            try:
                                subsystem_lines = int(fallback_lines)
                            except (TypeError, ValueError):
                                subsystem_lines = subsystem_lines or 0
                        responsible_subsystem_details[subsystem_name] = {
                            "name": subsystem_name,
                            "lines": subsystem_lines,
                            "languages": languages,
                        }
                        total_responsible_lines += subsystem_lines
                        for lang, lines in (languages or {}).items():
                            try:
                                capacity_languages[lang] = capacity_languages.get(lang, 0) + int(lines or 0)
                            except (TypeError, ValueError):
                                continue

                    payload = {
                        "type": "team",
                        "team_id": team_id,
                        "team_name": team_name,
                        "description": team.get("description", ""),
                        "members": data.get("members", []),
                        "responsible_subsystems": source_responsibilities,
                        "responsible_subsystem_details": responsible_subsystem_details,
                        "total_responsible_lines": total_responsible_lines,
                        "total_commits": data.get("commits", 0),
                        "total_additions": data.get("lines_added", 0),
                        "total_deletions": data.get("lines_deleted", 0),
                        "languages": data.get("languages", {}),
                        "subsystems": data.get("subsystems", {}),
                        "per_date": data.get("per_date", {}),
                        "member_contributions": data.get("member_contributions", {})
                    }
                    payload["members"] = canonicalize_team_members(payload.get("members", []))
                    team_size = len(payload.get("members", []))
                    payload["capacity_analysis"] = calculate_team_capacity(capacity_languages, team_size)
                    payload["developer_capacity_profiles"] = compute_developer_capacity_profiles(
                        payload.get("members", []),
                        source_responsibilities,
                        responsible_subsystem_details,
                    )
                    payload["subsystems_touched"] = len(payload.get("subsystems", {}))
                    payload["total_lines_changed"] = payload.get("total_additions", 0) + payload.get("total_deletions", 0)
                    rank_from = data.get("from")
                    rank_to = data.get("to")
                    month_label = data.get("month") or month_str
                    if month_label and (not rank_from or not rank_to):
                        try:
                            year_part = int(month_label[:4])
                            month_part = int(month_label[5:7])
                            last_day = calendar.monthrange(year_part, month_part)[1]
                            if not rank_from:
                                rank_from = f"{year_part:04d}-{month_part:02d}-01"
                            if not rank_to:
                                rank_to = f"{year_part:04d}-{month_part:02d}-{last_day:02d}"
                        except (ValueError, IndexError):
                            pass
                    if rank_from and not payload.get("from"):
                        payload["from"] = rank_from
                    if rank_to and not payload.get("to"):
                        payload["to"] = rank_to
                    payload["member_contributions"] = _ensure_member_contributions(
                        payload.get("members", []),
                        payload.get("from"),
                        payload.get("to"),
                        payload.get("member_contributions", {}),
                    )
                    if not payload.get("developer_capacity_profiles"):
                        fallback_profiles = data.get("developer_capacity_profiles") or build_activity_capacity_profiles(
                            payload.get("members", []),
                            payload.get("from"),
                            payload.get("to"),
                            assume_canonical=True,
                        )
                        payload["developer_capacity_profiles"] = fallback_profiles
                    target_metrics = _extract_team_metrics(payload)
                    if rank_from and rank_to:
                        peer_rankings = compute_team_peer_rankings(team_id, rank_from, rank_to, target_metrics)
                        if peer_rankings:
                            payload["peer_rankings"] = peer_rankings
                    return jsonify(payload)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading team file {team_file}: {e}")
    
    # Fall back to old aggregation method if file doesn't exist
    members = team.get("members", [])
    responsible_subsystems = get_team_responsible_subsystems(team_id)
    
    if to_date is None:
        to_date = from_date
    
    # Resolve all member slugs to their canonical forms present in stats data
    canonical_members = canonicalize_team_members(members)
    
    if not canonical_members:
        # Even for empty teams, calculate responsible subsystem details
        responsible_subsystem_details = {}
        total_responsible_lines = 0
        capacity_languages: Dict[str, int] = {}
        
        for subsystem_name in responsible_subsystems:
            languages, subsystem_lines = _resolve_responsible_subsystem_languages(subsystem_name)
            responsible_subsystem_details[subsystem_name] = {
                "name": subsystem_name,
                "lines": subsystem_lines,
                "languages": languages,
            }
            total_responsible_lines += subsystem_lines
            for lang, lines in (languages or {}).items():
                try:
                    capacity_languages[lang] = capacity_languages.get(lang, 0) + int(lines or 0)
                except (TypeError, ValueError):
                    continue
        
        team_size = len(team.get("members", []))
        capacity_analysis = calculate_team_capacity(capacity_languages, team_size)
        
        return jsonify({
            "type": "team",
            "team_id": team_id,
            "team_name": team.get("name", team_id),
            "description": team.get("description", ""),
            "members": canonical_members,
            "responsible_subsystems": responsible_subsystems,
            "responsible_subsystem_details": responsible_subsystem_details,
            "total_responsible_lines": total_responsible_lines,
            "total_commits": 0,
            "total_additions": 0,
            "total_deletions": 0,
            "files_changed": {},
            "languages": {},
            "subsystems": {},
            "commits_timeline": [],
            "capacity_analysis": capacity_analysis
        })
    
    # Aggregate data from all team members
    aggregated_data = {
        "type": "team",
        "team_id": team_id,
        "team_name": team.get("name", team_id),
        "description": team.get("description", ""),
        "members": canonical_members,
        "responsible_subsystems": responsible_subsystems,
        "total_commits": 0,
        "total_additions": 0,
        "total_deletions": 0,
        "languages": {},
        "subsystems": {},
        "per_date": {},
        "member_contributions": {}
    }
    member_language_cache: Dict[str, Dict[str, Any]] = {}
    
    for member in canonical_members:
        # Use the same aggregation method as the teams overview for consistency
        member_data = aggregate_user_data_for_period(member, from_date, to_date) or {}
        member_language_cache[member] = member_data.get("languages", {}) or {}
        if member_data:
            contribution_entry = _build_member_contribution_entry(member_data)
            # Aggregate basic stats
            aggregated_data["total_commits"] += contribution_entry["commits"]
            aggregated_data["total_additions"] += contribution_entry["additions"]
            aggregated_data["total_deletions"] += contribution_entry["deletions"]
            
            # Store individual member contribution with comparison metrics
            aggregated_data["member_contributions"][member] = contribution_entry
            
            # Aggregate files changed
            # Note: files_changed doesn't exist in user summaries, skip this aggregation
            
            # Aggregate languages
            for lang, lang_data in member_data.get("languages", {}).items():
                if lang not in aggregated_data["languages"]:
                    aggregated_data["languages"][lang] = {"additions": 0, "deletions": 0, "net_lines": 0}
                aggregated_data["languages"][lang]["additions"] += lang_data.get("additions", 0)
                aggregated_data["languages"][lang]["deletions"] += lang_data.get("deletions", 0)
                aggregated_data["languages"][lang]["net_lines"] += lang_data.get("net_lines", 0)
            
            # Aggregate subsystems (using per_repo data)
            for repo, repo_data in member_data.get("per_repo", {}).items():
                if repo not in aggregated_data["subsystems"]:
                    aggregated_data["subsystems"][repo] = {"commits": 0, "additions": 0, "deletions": 0}
                aggregated_data["subsystems"][repo]["commits"] += repo_data.get("commits", 0)
                aggregated_data["subsystems"][repo]["additions"] += repo_data.get("additions", 0)
                aggregated_data["subsystems"][repo]["deletions"] += repo_data.get("deletions", 0)
            
            # Aggregate per_date information for timeline
            for date, date_data in member_data.get("per_date", {}).items():
                if date not in aggregated_data["per_date"]:
                    aggregated_data["per_date"][date] = {"commits": 0, "additions": 0, "deletions": 0}
                aggregated_data["per_date"][date]["commits"] += date_data.get("commits", 0)
                aggregated_data["per_date"][date]["additions"] += date_data.get("additions", 0)
                aggregated_data["per_date"][date]["deletions"] += date_data.get("deletions", 0)
    
    # Add responsible subsystem details with line counts
    responsible_subsystem_details = {}
    total_responsible_lines = 0
    capacity_languages: Dict[str, int] = {}
    
    for subsystem_name in responsible_subsystems:
        languages, subsystem_lines = _resolve_responsible_subsystem_languages(subsystem_name)
        responsible_subsystem_details[subsystem_name] = {
            "name": subsystem_name,
            "lines": subsystem_lines,
            "languages": languages,
        }
        total_responsible_lines += subsystem_lines
        for lang, lines in (languages or {}).items():
            try:
                capacity_languages[lang] = capacity_languages.get(lang, 0) + int(lines or 0)
            except (TypeError, ValueError):
                continue
    
    aggregated_data["responsible_subsystem_details"] = responsible_subsystem_details
    aggregated_data["total_responsible_lines"] = total_responsible_lines
    team_size = len(canonical_members)
    aggregated_data["capacity_analysis"] = calculate_team_capacity(capacity_languages, team_size)
    aggregated_data["developer_capacity_profiles"] = compute_developer_capacity_profiles(
        canonical_members,
        responsible_subsystems,
        responsible_subsystem_details,
    )
    if not aggregated_data["developer_capacity_profiles"]:
        aggregated_data["developer_capacity_profiles"] = build_activity_capacity_profiles(
            canonical_members,
            from_date,
            to_date,
            language_cache=member_language_cache,
            assume_canonical=True,
        )
    aggregated_data["subsystems_touched"] = len(aggregated_data.get("subsystems", {}))
    aggregated_data["total_lines_changed"] = aggregated_data.get("total_additions", 0) + aggregated_data.get("total_deletions", 0)
    aggregated_data["from"] = from_date
    aggregated_data["to"] = to_date
    target_metrics = _extract_team_metrics(aggregated_data)
    peer_rankings = compute_team_peer_rankings(team_id, from_date, to_date, target_metrics)
    if peer_rankings:
        aggregated_data["peer_rankings"] = peer_rankings
    
    return jsonify(aggregated_data)


def calculate_team_capacity(languages: Dict[str, int], team_size: int) -> Dict[str, Any]:
    """Calculate team capacity analysis based on lines of code by language."""
    config_file = os.path.join(BASE_DIR, "configuration", "capacity_config.json")
    
    # Define non-code languages to exclude from capacity analysis
    excluded_languages = {
        'HTML', 'CSS', 'SCSS', 'Sass', 'Less',
        'JSON', 'YAML', 'XML', 'TOML', 'INI',
        'Markdown', 'reStructuredText', 'AsciiDoc', 'LaTeX', 'TeX',
        'CSV', 'TSV', 'Properties', 'Dockerfile', 'Makefile',
        'Text', 'Binary', 'Data', 'Image', 'Video', 'Audio',
        'Protocol Buffer', 'Thrift', 'Avro', 'GraphQL',
        'Mustache', 'Handlebars', 'Jinja', 'Smarty',
        'SVG', 'PostScript', 'Rich Text Format',
        'Unknown'
    }
    
    # Load capacity configuration
    try:
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                config = json.load(f)
        else:
            config = {
                "default_lines_per_developer": 20000,
                "language_lines_per_developer": {},
                "warning_threshold_percent": 90,
                "critical_threshold_percent": 100
            }
    except:
        config = {
            "default_lines_per_developer": 20000,
            "language_lines_per_developer": {},
            "warning_threshold_percent": 90,
            "critical_threshold_percent": 100
        }
    
    default_lines = config.get("default_lines_per_developer", 20000)
    language_config = config.get("languages", {})
    warning_threshold = config.get("warning_threshold_percent", 90)
    critical_threshold = config.get("critical_threshold_percent", 100)
    
    # Calculate required developers per language (excluding non-code languages)
    total_required_developers = 0.0
    language_breakdown = {}
    
    for language, lines in languages.items():
        # Skip non-code languages
        if language in excluded_languages:
            continue
        lines_per_dev = language_config.get(language, default_lines)
        required_devs = lines / lines_per_dev
        total_required_developers += required_devs
        language_breakdown[language] = {
            "lines": lines,
            "lines_per_developer": lines_per_dev,
            "theoretical_devs": round(required_devs, 2)
        }
    
    # Calculate capacity status
    if team_size == 0:
        capacity_percent = 0
        status = "unknown"
        status_color = "gray"
    else:
        capacity_percent = (total_required_developers / team_size) * 100
        
        if capacity_percent <= warning_threshold:
            status = "healthy"
            status_color = "green"
        elif capacity_percent <= critical_threshold:
            status = "warning"
            status_color = "yellow"
        else:
            status = "critical"
            status_color = "red"
    
    return {
        "team_size": team_size,
        "required_developers": round(total_required_developers, 2),
        "capacity_percent": round(capacity_percent, 1),
        "status": status,
        "status_color": status_color,
        "language_breakdown": language_breakdown,
        "thresholds": {
            "warning": warning_threshold,
            "critical": critical_threshold
        }
    }


def _strip_accents(text: str) -> str:
    if not text:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))


def slugify_identifier(text: str) -> str:
    text = _strip_accents((text or "").strip()).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def _compact_identifier(text: Optional[str]) -> str:
    if not text:
        return ""
    normalized = _strip_accents(text).lower()
    normalized = re.sub(r"[^a-z0-9]", "", normalized)
    normalized = re.sub(r"(.)\1+", r"\1", normalized)
    return normalized


def _collect_slug_variants(value: Optional[str]) -> List[str]:
    variants: Set[str] = set()
    if not value:
        return []
    normalized = value.strip()
    if not normalized:
        return []

    def _add_basic_variants(text_value: str) -> None:
        if not text_value:
            return
        variants.add(text_value)
        variants.add(text_value.lower())
        compact = text_value.replace(" ", "")
        if compact:
            variants.add(compact)
            variants.add(compact.lower())
        slugified = slugify_identifier(text_value)
        if slugified:
            variants.add(slugified)
            variants.add(slugified.lower())
            compact_slug = slugified.replace("-", "")
            if compact_slug:
                variants.add(compact_slug)
                variants.add(compact_slug.lower())
            dedup_slug = re.sub(r"(.)\1+", r"\1", slugified)
            if dedup_slug and dedup_slug != slugified:
                variants.add(dedup_slug)
                variants.add(dedup_slug.lower())
                dedup_compact = dedup_slug.replace("-", "")
                if dedup_compact:
                    variants.add(dedup_compact)
                    variants.add(dedup_compact.lower())
        tokens = [token for token in re.split(r"[\s_/-]+", text_value) if token]
        if len(tokens) >= 2:
            first_last = f"{tokens[0]} {tokens[-1]}".strip()
            if first_last:
                variants.add(first_last)
                variants.add(first_last.lower())
                compact_first_last = first_last.replace(" ", "")
                if compact_first_last:
                    variants.add(compact_first_last)
                    variants.add(compact_first_last.lower())
                slugified_first_last = slugify_identifier(first_last)
                if slugified_first_last:
                    variants.add(slugified_first_last)
                    variants.add(slugified_first_last.lower())
                    compact_first_last_slug = slugified_first_last.replace("-", "")
                    if compact_first_last_slug:
                        variants.add(compact_first_last_slug)
                        variants.add(compact_first_last_slug.lower())

    ascii_normalized = _strip_accents(normalized)
    _add_basic_variants(normalized)
    if ascii_normalized and ascii_normalized != normalized:
        _add_basic_variants(ascii_normalized)

    return [variant for variant in variants if variant]


def _register_user_slug_variants(index: Dict[str, str], actual_slug: str, alias_lookup: Dict[str, str]) -> None:
    if not actual_slug:
        return
    for variant in _collect_slug_variants(actual_slug):
        index[variant] = actual_slug
    slugified = slugify_identifier(actual_slug)
    canonical = alias_lookup.get(slugified) or alias_lookup.get(actual_slug) or alias_lookup.get(actual_slug.lower())
    if canonical:
        for variant in _collect_slug_variants(canonical):
            index.setdefault(variant, actual_slug)


def _build_user_slug_index() -> Dict[str, str]:
    alias_lookup = load_alias_lookup()
    users_root = os.path.join(STATS_ROOT, "users")
    index: Dict[str, str] = {}
    if not os.path.isdir(users_root):
        return index
    for entry in os.listdir(users_root):
        entry_path = os.path.join(users_root, entry)
        if not os.path.isdir(entry_path):
            continue
        _register_user_slug_variants(index, entry, alias_lookup)
    for alias, canonical in alias_lookup.items():
        target = (
            index.get(canonical)
            or index.get(canonical.lower())
            or index.get(slugify_identifier(canonical))
        )
        if not target:
            continue
        for variant in _collect_slug_variants(alias):
            index[variant] = target
    return index


def _get_user_slug_index(refresh: bool = False) -> Dict[str, str]:
    global _USER_SLUG_INDEX
    if refresh or _USER_SLUG_INDEX is None:
        _USER_SLUG_INDEX = _build_user_slug_index()
    return _USER_SLUG_INDEX or {}


def resolve_user_slug(user_slug: Optional[str]) -> Optional[str]:
    if not user_slug:
        return None
    users_root = os.path.join(STATS_ROOT, "users")
    direct_path = os.path.join(users_root, user_slug)
    if os.path.isdir(direct_path):
        return user_slug
    alias_lookup = load_alias_lookup()
    candidates = _collect_slug_variants(user_slug)
    canonical = canonicalize_slug(user_slug, alias_lookup)
    if canonical:
        candidates.extend(_collect_slug_variants(canonical))
    index = _get_user_slug_index()
    for candidate in candidates:
        if not candidate:
            continue
        target = index.get(candidate) or index.get(candidate.lower())
        if target:
            return target
    index = _get_user_slug_index(refresh=True)
    for candidate in candidates:
        if not candidate:
            continue
        target = index.get(candidate) or index.get(candidate.lower())
        if target:
            return target
    compact_target = _compact_identifier(user_slug)
    if compact_target and len(compact_target) >= 4:
        user_months = list_user_months()
        best_match = None
        for slug in user_months.keys():
            compact_slug = _compact_identifier(slug)
            if not compact_slug:
                continue
            if compact_slug == compact_target:
                return slug
            if compact_slug.startswith(compact_target) or compact_target.startswith(compact_slug):
                if not best_match or len(compact_slug) < len(best_match[1]):
                    best_match = (slug, compact_slug)
        if best_match:
            return best_match[0]
    return None


def load_alias_lookup() -> Dict[str, str]:
    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    lookup: Dict[str, str] = {}
    if not os.path.isfile(alias_file):
        return lookup
    try:
        data = load_json(alias_file)
    except Exception:
        return lookup

    if isinstance(data, dict):
        for canonical, aliases in data.items():
            if not isinstance(canonical, str):
                continue
            lookup[canonical] = canonical
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str):
                        lookup[alias] = canonical
            elif isinstance(aliases, str):
                lookup[aliases] = canonical
    return lookup


def canonicalize_slug(slug: Optional[str], alias_lookup: Optional[Dict[str, str]] = None) -> Optional[str]:
    if not slug:
        return None
    if alias_lookup is None:
        alias_lookup = load_alias_lookup()
    return alias_lookup.get(slug, slug)


def canonicalize_team_members(
    members: List[str],
    alias_lookup: Optional[Dict[str, str]] = None,
) -> List[str]:
    alias_lookup = alias_lookup or load_alias_lookup()
    canonical: List[str] = []
    seen: Set[str] = set()
    for member in members or []:
        resolved = resolve_user_slug(member)
        ascii_member = _strip_accents(member or "") if member else ""
        if not resolved and ascii_member and ascii_member != member:
            resolved = resolve_user_slug(ascii_member)
        if not resolved:
            resolved = canonicalize_slug(member, alias_lookup)
        if not resolved and ascii_member and ascii_member != member:
            resolved = canonicalize_slug(ascii_member, alias_lookup)
        if not resolved:
            slug_candidate = slugify_identifier(member)
            resolved = resolve_user_slug(slug_candidate) or slug_candidate or member
        if resolved and resolved not in seen:
            seen.add(resolved)
            canonical.append(resolved)
    return canonical


def load_ignored_user_slugs() -> Set[str]:
    ignore_file = os.path.join(BASE_DIR, "configuration", "ignore_user.txt")
    ignored: Set[str] = set()
    if not os.path.isfile(ignore_file):
        return ignored
    try:
        with open(ignore_file, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                ignored.add(line)
                ignored.add(slugify_identifier(line))
                if "@" in line:
                    local = line.split("@", 1)[0]
                    ignored.add(local)
                    ignored.add(slugify_identifier(local))
    except Exception:
        return ignored
    return ignored


def load_subsystem_languages_map() -> Dict[str, Dict[str, int]]:
    languages_cache: Dict[str, Dict[str, int]] = {}
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")
    if not os.path.isdir(subsystems_root):
        return languages_cache
    for subsystem_name in os.listdir(subsystems_root):
        lang_file = os.path.join(subsystems_root, subsystem_name, "languages.json")
        if not os.path.isfile(lang_file):
            continue
        try:
            lang_data = load_json(lang_file)
            langs = {}
            for lang, info in (lang_data.get("languages", {}) or {}).items():
                if isinstance(info, dict):
                    langs[lang] = int(info.get("code_lines", 0) or 0)
            if not langs:
                continue
            languages_cache[subsystem_name] = langs
            languages_cache[subsystem_name.lower()] = langs
        except Exception:
            continue
    return languages_cache


def _resolve_language_map(candidates: List[str], languages_cache: Dict[str, Dict[str, int]]):
    for candidate in candidates:
        if not candidate:
            continue
        langs = languages_cache.get(candidate)
        if langs:
            return langs
        langs = languages_cache.get(candidate.lower())
        if langs:
            return langs
    return None


def _resolve_latest_blame_snapshot(blame_dir: str) -> Optional[str]:
    if not os.path.isdir(blame_dir):
        return None

    latest_file = os.path.join(blame_dir, "latest.json")
    if os.path.isfile(latest_file):
        return latest_file

    legacy_file = os.path.join(blame_dir, "blame.json")
    if os.path.isfile(legacy_file):
        return legacy_file

    candidates: List[str] = []
    try:
        with os.scandir(blame_dir) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith(".json"):
                    candidates.append(entry.path)
                elif entry.is_dir():
                    try:
                        with os.scandir(entry.path) as sub_entries:
                            for sub_entry in sub_entries:
                                if sub_entry.is_file() and sub_entry.name.endswith(".json"):
                                    candidates.append(sub_entry.path)
                    except OSError:
                        continue
    except OSError:
        return None

    if not candidates:
        return None

    candidates.sort()
    return candidates[-1]


def _iter_latest_blame_snapshots(repos_path: str):
    if not os.path.isdir(repos_path):
        return

    try:
        owner_entries = os.scandir(repos_path)
    except OSError:
        return

    with owner_entries:
        for owner_entry in owner_entries:
            if not owner_entry.is_dir():
                continue
            try:
                repo_entries = os.scandir(owner_entry.path)
            except OSError:
                continue
            with repo_entries:
                for repo_entry in repo_entries:
                    if not repo_entry.is_dir():
                        continue
                    blame_dir = os.path.join(repo_entry.path, "blame")
                    if not os.path.isdir(blame_dir):
                        continue
                    blame_file = _resolve_latest_blame_snapshot(blame_dir)
                    if blame_file:
                        yield repo_entry.path, blame_file


def build_developer_language_portfolio(target_slugs: Optional[Set[str]] = None) -> Dict[str, Dict[str, Any]]:
    alias_lookup = load_alias_lookup()
    ignored_slugs = load_ignored_user_slugs()
    ignored_slugified = {slugify_identifier(s) for s in ignored_slugs}
    languages_cache = load_subsystem_languages_map()

    if target_slugs:
        canonical_targets = {canonicalize_slug(slug, alias_lookup) for slug in target_slugs}
        canonical_targets.discard(None)
    else:
        canonical_targets = None

    developer_data: Dict[str, Dict[str, Any]] = {}

    repos_path = os.path.join(STATS_ROOT, "repos")
    if not os.path.isdir(repos_path):
        return developer_data

    def should_track(slug: Optional[str]) -> bool:
        if not slug:
            return False
        if slug in ignored_slugs or slugify_identifier(slug) in ignored_slugified:
            return False
        if canonical_targets is not None and slug not in canonical_targets:
            return False
        return True

    def add_context(
        candidates: List[str],
        total_lines: int,
        developers: Dict[str, Dict[str, Any]],
        context_hint: Optional[Dict[str, Any]] = None,
    ):
        if not developers:
            return
        context_total = total_lines or sum(dev.get("lines", 0) or 0 for dev in developers.values())
        if context_total <= 0:
            return
        primary_label = candidates[0] if candidates else None
        languages = _resolve_language_map(candidates, languages_cache)
        if not languages and context_hint:
            if context_hint.get("type") == "service":
                languages = _get_service_language_breakdown(
                    context_hint.get("repo_full_name"),
                    context_hint.get("service_name"),
                )
            elif context_hint.get("type") == "repo":
                languages = _get_repo_language_breakdown(context_hint.get("repo_full_name"))
            if languages and primary_label:
                languages_cache[primary_label] = languages
                languages_cache[primary_label.lower()] = languages
        if languages:
            language_total = sum(max(0, lines) for lines in languages.values())
            if language_total <= 0:
                languages = None
        if not languages:
            fallback_label = primary_label or "Unclassified"
            languages = {f"Code:{fallback_label}": max(context_total, 0)}
        context_key = None
        if context_hint and context_hint.get("context_key"):
            context_key = context_hint.get("context_key")
        elif primary_label:
            context_key = primary_label
        for slug, info in developers.items():
            canonical_slug = canonicalize_slug(slug, alias_lookup)
            if not should_track(canonical_slug):
                continue
            dev_lines = info.get("lines", 0) or 0
            if dev_lines <= 0:
                continue
            share = dev_lines / context_total
            if share <= 0:
                continue
            profile = developer_data.setdefault(
                canonical_slug,
                {
                    "slug": canonical_slug,
                    "display_name": info.get("display_name") or canonical_slug,
                    "languages": defaultdict(float),
                    "contexts": set(),
                },
            )
            if "contexts" not in profile:
                profile["contexts"] = set()
            if not profile.get("display_name") and info.get("display_name"):
                profile["display_name"] = info.get("display_name")
            if context_key:
                profile["contexts"].add(context_key)
            for lang, code_lines in languages.items():
                if code_lines <= 0:
                    continue
                profile["languages"][lang] += code_lines * share

    for repo_dir, blame_file in _iter_latest_blame_snapshots(repos_path):
        try:
            blame_data = load_json(blame_file)
        except Exception:
            continue

        repo_rel = os.path.relpath(repo_dir, repos_path)
        repo_rel = repo_rel.replace(os.sep, "/")
        repo_name = repo_rel.split("/")[-1]
        repo_full_name = blame_data.get("repo", repo_rel)

        developers = _normalize_blame_developers(blame_data.get("developers", {}))
        total_lines = blame_data.get("total_lines", 0)
        services = blame_data.get("services", {}) or {}

        if developers and not services:
            add_context(
                [repo_name, repo_full_name],
                total_lines,
                developers,
                {"type": "repo", "repo_full_name": repo_full_name, "context_key": repo_full_name},
            )

        for service_name, service_data in services.items():
            service_devs = _normalize_blame_developers(service_data.get("developers", {}))
            if not service_devs:
                continue
            service_total = service_data.get("total_lines", 0)
            add_context(
                [service_name],
                service_total,
                service_devs,
                {
                    "type": "service",
                    "repo_full_name": repo_full_name,
                    "service_name": service_name,
                    "context_key": f"{repo_full_name}/{service_name}",
                },
            )

    return developer_data


def build_developer_capacity_profiles(
    target_slugs: Optional[Set[str]] = None,
    min_equivalent: float = 0.9,
) -> Dict[str, Dict[str, Any]]:
    portfolios = build_developer_language_portfolio(target_slugs)
    if not portfolios:
        return {}

    profiles: Dict[str, Dict[str, Any]] = {}
    for slug, info in portfolios.items():
        raw_languages = info.get("languages", {})
        normalized_languages = {
            lang: int(round(lines))
            for lang, lines in raw_languages.items()
            if lines > 0
        }
        if not normalized_languages:
            continue
        capacity = calculate_team_capacity(normalized_languages, team_size=1)
        language_breakdown = capacity.get("language_breakdown", {})
        if not language_breakdown:
            continue
        developer_equivalent = capacity.get("required_developers", 0)
        if developer_equivalent < min_equivalent:
            continue
        total_lines = sum(lang_data.get("lines", 0) for lang_data in language_breakdown.values())
        profiles[slug] = {
            "slug": slug,
            "display_name": info.get("display_name", slug),
            "language_breakdown": language_breakdown,
            "developer_equivalent": round(developer_equivalent, 2),
            "total_lines": int(round(total_lines)),
        }
    return profiles


def get_developer_capacity_profile(user_slug: str, min_equivalent: float = 0.9) -> Optional[Dict[str, Any]]:
    alias_lookup = load_alias_lookup()
    canonical_slug = canonicalize_slug(user_slug, alias_lookup)
    if not canonical_slug:
        return None
    profiles = build_developer_capacity_profiles({canonical_slug}, min_equivalent=min_equivalent)
    return profiles.get(canonical_slug)


def _normalize_blame_developers(dev_obj):
    """Normalize blame developer structures into slug -> {lines, display_name}."""
    normalized = {}

    def _ingest_entry(raw_slug: Optional[str], lines_value: Any, label: Optional[str]):
        if not raw_slug:
            return
        try:
            lines = int(lines_value or 0)
        except (TypeError, ValueError):
            return
        if lines <= 0:
            return
        slug_key = slugify_identifier(raw_slug)
        if not slug_key:
            return
        entry = normalized.setdefault(slug_key, {"lines": 0, "display_name": label or raw_slug})
        entry["lines"] += lines
        if not entry.get("display_name") and label:
            entry["display_name"] = label

    if isinstance(dev_obj, dict):
        for slug, info in dev_obj.items():
            if isinstance(info, dict):
                lines = info.get("lines", 0)
                display_name = info.get("display_name") or info.get("user") or slug
            else:
                lines = info
                display_name = slug
            _ingest_entry(slug, lines, display_name)
    elif isinstance(dev_obj, list):
        for entry in dev_obj:
            if not isinstance(entry, dict):
                continue
            slug = entry.get("slug") or entry.get("user") or entry.get("name")
            display_name = entry.get("display_name") or entry.get("user") or entry.get("name") or slug
            lines = entry.get("lines") or entry.get("loc") or entry.get("line_count") or 0
            _ingest_entry(slug, lines, display_name)

    return normalized


def _collect_blame_data_for_subsystems(target_subsystems):
    """Return blame developer data for the requested subsystem names."""
    if not target_subsystems:
        return {}

    repos_path = os.path.join(STATS_ROOT, "repos")
    if not os.path.exists(repos_path):
        return {}

    collected = {}
    target_set = set(target_subsystems)

    for repo_path, blame_file in _iter_latest_blame_snapshots(repos_path):
        try:
            blame_data = load_json(blame_file)
        except Exception as exc:
            print(f"Error loading blame file {blame_file}: {exc}")
            continue

        repo_base = os.path.basename(repo_path)
        repo_full_name = blame_data.get("repo", "")
        repo_key_candidates = {repo_base}
        if repo_full_name:
            repo_key_candidates.add(repo_full_name)
            repo_key_candidates.add(repo_full_name.split("/")[-1])

        # Repo-level ownership
        developers = _normalize_blame_developers(blame_data.get("developers", {}))
        services = blame_data.get("services", {}) or {}

        # Repo-level ownership only when no per-service breakdown exists
        if developers and not services:
            repo_total = blame_data.get("total_lines", 0)
            if repo_total <= 0:
                repo_total = sum(dev["lines"] for dev in developers.values())
            for candidate in repo_key_candidates:
                if candidate in target_set:
                    collected[candidate] = {
                        "total_lines": repo_total,
                        "developers": developers,
                    }

        # Service-level ownership
        for service_name, service_data in services.items():
            if service_name not in target_set:
                continue
            service_devs = _normalize_blame_developers(service_data.get("developers", {}))
            if not service_devs:
                continue
            service_total = service_data.get("total_lines", 0)
            if service_total <= 0:
                service_total = sum(dev["lines"] for dev in service_devs.values())
            collected[service_name] = {
                "total_lines": service_total,
                "developers": service_devs,
            }

        if target_set.issubset(collected.keys()):
            break

    return collected


def compute_developer_capacity_profiles(team_members, responsibilities, subsystem_details):
    """Build per-developer language ownership and theoretical capacity."""
    if not team_members or not responsibilities:
        return []

    alias_lookup = load_alias_lookup()
    member_set = set()
    for member in team_members:
        canonical_member = canonicalize_slug(slugify_identifier(member), alias_lookup) or slugify_identifier(member)
        if canonical_member:
            member_set.add(canonical_member)
    blame_map = _collect_blame_data_for_subsystems(responsibilities)
    if not blame_map:
        return []

    dev_language_totals = {}

    for subsystem_name in responsibilities:
        subsystem_meta = subsystem_details.get(subsystem_name) or {}
        subsystem_languages = subsystem_meta.get("languages") or {}
        if not subsystem_languages:
            continue

        subsystem_blame = blame_map.get(subsystem_name)
        if not subsystem_blame:
            continue

        total_lines = subsystem_blame.get("total_lines", 0)
        if total_lines <= 0:
            total_lines = sum(dev["lines"] for dev in subsystem_blame.get("developers", {}).values())
        if total_lines <= 0:
            continue

        for dev_slug, dev_info in (subsystem_blame.get("developers") or {}).items():
            canonical_dev_slug = canonicalize_slug(slugify_identifier(dev_slug), alias_lookup) or slugify_identifier(dev_slug)
            if canonical_dev_slug not in member_set:
                continue
            dev_lines = dev_info.get("lines", 0)
            if dev_lines <= 0:
                continue

            share = dev_lines / total_lines
            if share <= 0:
                continue

            profile = dev_language_totals.setdefault(
                canonical_dev_slug,
                {
                    "display_name": dev_info.get("display_name") or dev_slug,
                    "languages": defaultdict(float),
                },
            )

            for lang, lang_lines in subsystem_languages.items():
                if lang_lines <= 0:
                    continue
                profile["languages"][lang] += lang_lines * share

    developer_profiles = []

    for dev_slug, profile in dev_language_totals.items():
        normalized_languages = {
            lang: int(round(lines))
            for lang, lines in profile["languages"].items()
            if lines > 0
        }
        if not normalized_languages:
            continue

        capacity = calculate_team_capacity(normalized_languages, team_size=1)
        language_breakdown = capacity.get("language_breakdown", {})
        if not language_breakdown:
            continue

        total_lines = sum(lang_data.get("lines", 0) for lang_data in language_breakdown.values())
        developer_equivalent = capacity.get("required_developers", 0)

        if developer_equivalent < 0.9:
            continue

        developer_profiles.append({
            "slug": dev_slug,
            "display_name": profile.get("display_name", dev_slug),
            "language_breakdown": language_breakdown,
            "developer_equivalent": round(developer_equivalent, 2),
            "total_lines": int(round(total_lines)),
        })

    developer_profiles.sort(key=lambda item: item["developer_equivalent"], reverse=True)
    return developer_profiles


def build_activity_capacity_profiles(
    members: List[str],
    from_date: Optional[str],
    to_date: Optional[str],
    min_equivalent: float = 0.9,
    language_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    assume_canonical: bool = False,
) -> List[Dict[str, Any]]:
    if not members or not from_date or not to_date:
        return []
    alias_lookup = load_alias_lookup()
    if assume_canonical:
        canonical_members: List[str] = []
        seen: Set[str] = set()
        for member in members:
            resolved = canonicalize_slug(member, alias_lookup) or slugify_identifier(member) or member
            if resolved and resolved not in seen:
                seen.add(resolved)
                canonical_members.append(resolved)
    else:
        canonical_members = canonicalize_team_members(members, alias_lookup)
    if not canonical_members:
        return []

    profiles: List[Dict[str, Any]] = []
    for member in canonical_members:
        member_languages = None
        member_stats: Dict[str, Any] = {}
        if language_cache is not None:
            member_languages = language_cache.get(member)
        if not member_languages:
            try:
                member_stats = aggregate_user_data_for_period(member, from_date, to_date) or {}
            except Exception:
                member_stats = {}
            member_languages = (member_stats or {}).get("languages", {})
        else:
            member_stats = {}

        language_lines: Dict[str, int] = {}
        for lang, lang_data in (member_languages or {}).items():
            additions = _safe_int((lang_data or {}).get("additions"), 0)
            deletions = _safe_int((lang_data or {}).get("deletions"), 0)
            net = _safe_int((lang_data or {}).get("net_lines"), 0)
            total_lines = additions + deletions
            if total_lines <= 0:
                total_lines = abs(net)
            if total_lines <= 0:
                continue
            language_lines[lang] = language_lines.get(lang, 0) + total_lines
        if not language_lines:
            continue

        capacity = calculate_team_capacity(language_lines, team_size=1)
        breakdown = capacity.get("language_breakdown") or {}
        if not breakdown:
            continue
        developer_equivalent = capacity.get("required_developers", 0)
        if developer_equivalent < min_equivalent:
            continue
        total_lines = sum(entry.get("lines", 0) for entry in breakdown.values())
        display_name = (
            member_stats.get("display_name")
            or member_stats.get("user")
            or member_stats.get("author")
        ) if member_stats else None
        if not display_name:
            display_name = _get_user_display_name(member)
        profiles.append({
            "slug": member,
            "display_name": display_name or member,
            "language_breakdown": breakdown,
            "developer_equivalent": round(developer_equivalent, 2),
            "total_lines": int(total_lines),
        })

    profiles.sort(key=lambda item: item["developer_equivalent"], reverse=True)
    return profiles


def build_global_developer_totals() -> List[Dict[str, Any]]:
    portfolios = build_developer_language_portfolio()
    developers = []
    for slug, info in portfolios.items():
        languages = info.get("languages", {})
        if not languages:
            continue
        total_lines = sum(lines for lines in languages.values() if lines > 0)
        if total_lines <= 0:
            continue
        contexts = info.get("contexts") or set()
        resolved_slug = resolve_user_slug(slug) or slug
        developer_entry = {
            "slug": resolved_slug,
            "display_name": info.get("display_name", slug),
            "total_lines": int(round(total_lines)),
            "subsystem_count": len(contexts),
            "subsystems": sorted(contexts),
        }
        if resolved_slug != slug:
            developer_entry["canonical_slug"] = slug
        developers.append(developer_entry)
    developers.sort(key=lambda dev: dev["total_lines"], reverse=True)
    return developers


@app.route("/api/developers/capacity-profiles")
def api_developers_capacity_profiles():
    limit = int(request.args.get("limit", 25))
    profiles = build_developer_capacity_profiles()
    sorted_profiles = sorted(profiles.values(), key=lambda item: item.get("developer_equivalent", 0), reverse=True)
    return jsonify({"profiles": sorted_profiles[:limit]})


@app.route("/api/developers/total-ownership")
def api_developers_total_ownership():
    totals = build_global_developer_totals()
    return jsonify({"developers": totals})


def build_team_per_date(members: list, year: int):
    aggregated = {}
    try:
        for member in members or []:
            for month in range(1, 13):
                summary_path = resolve_user_month_summary_path(member, year, month)
                if not summary_path:
                    continue
                monthly = load_user_summary_file(summary_path)
                for date_str, day in (monthly.get("per_date", {}) or {}).items():
                    entry = aggregated.setdefault(date_str, {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0})
                    entry["commits"] += day.get("commits", 0)
                    entry["additions"] += day.get("additions", 0)
                    entry["deletions"] += day.get("deletions", 0)
                    entry["net_lines"] += day.get("net_lines", 0)
    except Exception:
        pass
    return aggregated

@app.route("/api/teams/<team_id>/year/<int:year>")
def api_team_year(team_id: str, year: int):
    """Get aggregated yearly summary for a team."""
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        abort(404, description="Teams configuration not found")
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        abort(404, description="Invalid teams configuration")
    
    if team_id not in teams_config:
        abort(404, description="Team not found")
    
    team = teams_config[team_id]
    team_name = team.get("name", team_id)
    
    # Try loading from YYYY.json format
    team_file = os.path.join(STATS_ROOT, "teams", team_name, f"{year}.json")
    
    if os.path.exists(team_file):
        try:
            with open(team_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Calculate capacity analysis based on responsible subsystems ownership
                # Recompute responsible_subsystem_details from current responsibilities
                responsibilities_file = os.path.join(BASE_DIR, "configuration", "team_subsystem_responsibilities.json")
                team_responsibilities = []
                try:
                    with open(responsibilities_file, "r", encoding="utf-8") as rf:
                        resp_map = json.load(rf)
                        team_responsibilities = resp_map.get(team_id, [])
                except Exception:
                    team_responsibilities = []
                recomputed_details = {}
                total_responsible_lines = 0
                
                for subsystem_name in team_responsibilities:
                    subsystem_languages, subsystem_lines = _resolve_responsible_subsystem_languages(subsystem_name)
                    if not subsystem_languages and subsystem_lines <= 0:
                        app.logger.debug(f"[teams-year] No language data for subsystem '{subsystem_name}'")
                    recomputed_details[subsystem_name] = {
                        "name": subsystem_name,
                        "lines": subsystem_lines,
                        "languages": subsystem_languages
                    }
                    total_responsible_lines += subsystem_lines
                # Aggregate language lines from recomputed details
                languages = {}
                for subsystem_name, details in recomputed_details.items():
                    for lang, lines in details.get("languages", {}).items():
                        languages[lang] = languages.get(lang, 0) + lines
                team_size = len(data.get("members", []))
                capacity_analysis = calculate_team_capacity(languages, team_size)
                developer_capacity_profiles = compute_developer_capacity_profiles(
                    team.get("members", []),
                    team_responsibilities,
                    recomputed_details
                )
                
                # Normalize subsystem keys for frontend (expects 'additions'/'deletions')
                subsystems = data.get("subsystems", {})
                for sub_name, stats in subsystems.items():
                    if "additions" not in stats and "lines_added" in stats:
                        stats["additions"] = stats.get("lines_added", 0)
                    if "deletions" not in stats and "lines_deleted" in stats:
                        stats["deletions"] = stats.get("lines_deleted", 0)
                # Convert to expected format
                payload = {
                    "type": "team",
                    "team_id": team_id,
                    "team_name": team_name,
                    "description": team.get("description", ""),
                    "members": data.get("members", []),
                    "responsible_subsystems": team_responsibilities,
                    "responsible_subsystem_details": recomputed_details,
                    "total_responsible_lines": total_responsible_lines,
                    "total_commits": data.get("commits", 0),
                    "total_additions": data.get("lines_added", 0),
                    "total_deletions": data.get("lines_deleted", 0),
                    "languages": languages,
                    "subsystems": subsystems,
                    "per_date": build_team_per_date(team.get("members", []), year),
                    "member_contributions": data.get("member_contributions", {}),
                    "capacity_analysis": capacity_analysis,
                    "developer_capacity_profiles": developer_capacity_profiles
                }
                payload["members"] = canonicalize_team_members(payload.get("members", []))
                payload["subsystems_touched"] = len(payload.get("subsystems", {}))
                payload["total_lines_changed"] = payload.get("total_additions", 0) + payload.get("total_deletions", 0)
                payload.setdefault("from", f"{year:04d}-01-01")
                payload.setdefault("to", f"{year:04d}-12-31")
                payload["member_contributions"] = _ensure_member_contributions(
                    payload.get("members", []),
                    payload.get("from"),
                    payload.get("to"),
                    payload.get("member_contributions", {}),
                )
                target_metrics = _extract_team_metrics(payload)
                peer_rankings = compute_team_peer_rankings(team_id, payload["from"], payload["to"], target_metrics)
                if peer_rankings:
                    payload["peer_rankings"] = peer_rankings
                return jsonify(payload)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading team file {team_file}: {e}")
    
    # Fall back to old aggregation method
    from_date = f"{year:04d}-01-01"
    to_date = f"{year:04d}-12-31"
    return api_team_month(team_id, from_date, to_date)


@app.route("/api/teams/overview")
def api_teams_overview():
    """Get overview analytics for all teams (Last 3 Months)."""
    force_refresh = request.args.get('refresh') in {'1', 'true', 'True', 'YES', 'yes'}

    if not force_refresh:
        cached = load_json(TEAM_OVERVIEW_CACHE_FILE, default={})
        if cached.get("teams"):
            cached.setdefault("period", "Last 3 Months")
            return jsonify(cached)

    snapshot = build_team_overview_snapshot()
    if snapshot.get("teams"):
        save_json(TEAM_OVERVIEW_CACHE_FILE, snapshot)
    return jsonify(snapshot)


def aggregate_user_data_for_period(user_slug, from_date, to_date):
    """Aggregate user data across multiple monthly summaries for a given period."""
    aggregated_data = {
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "per_repo": {},
        "languages": {},
        "per_date": {}
    }
    
    actual_slug = resolve_user_slug(user_slug) or user_slug
    
    # Get all available periods for this user
    user_months = list_user_months()
    user_periods = user_months.get(actual_slug, [])
    
    # Check if we have a yearly summary that exactly matches our period
    exact_yearly_match = None
    for period in user_periods:
        if (period["is_yearly"] and 
            period["from"] == from_date and 
            period["to"] == to_date):
            exact_yearly_match = period
            break
    
    # If we have an exact yearly match, use that instead of aggregating monthly data
    if exact_yearly_match:
        summary_path = resolve_user_period_summary_path(actual_slug, exact_yearly_match)
        if summary_path and os.path.exists(summary_path):
            try:
                return load_user_summary_file(summary_path, augment=True)
            except Exception as e:
                print(f"Error loading yearly summary for {user_slug}: {e}")
                # Fall back to aggregation
    
    # Otherwise, aggregate from overlapping periods, but prioritize monthly summaries over yearly
    # when we're looking for a partial period
    monthly_periods = [p for p in user_periods if not p["is_yearly"]]
    
    for period in monthly_periods:
        # Check if this period overlaps with our target date range
        period_from = period["from"]
        period_to = period["to"]
        
        # Simple overlap check: period overlaps if it starts before our end date and ends after our start date
        if period_from <= to_date and period_to >= from_date:
            summary_path = resolve_user_period_summary_path(actual_slug, period)
            if summary_path and os.path.exists(summary_path):
                try:
                    period_data = load_user_summary_file(summary_path, augment=True)
                    
                    # Aggregate basic stats
                    aggregated_data["total_commits"] += period_data.get("total_commits", 0)
                    aggregated_data["total_lines_added"] += period_data.get("total_lines_added", 0)
                    aggregated_data["total_lines_deleted"] += period_data.get("total_lines_deleted", 0)
                    
                    # Aggregate per_repo data
                    for repo, repo_data in period_data.get("per_repo", {}).items():
                        if repo not in aggregated_data["per_repo"]:
                            aggregated_data["per_repo"][repo] = {"commits": 0, "additions": 0, "deletions": 0}
                        aggregated_data["per_repo"][repo]["commits"] += repo_data.get("commits", 0)
                        aggregated_data["per_repo"][repo]["additions"] += repo_data.get("additions", 0)
                        aggregated_data["per_repo"][repo]["deletions"] += repo_data.get("deletions", 0)
                    
                    # Aggregate languages
                    for lang, lang_data in period_data.get("languages", {}).items():
                        if lang not in aggregated_data["languages"]:
                            aggregated_data["languages"][lang] = {"net_lines": 0, "additions": 0, "deletions": 0}
                        aggregated_data["languages"][lang]["net_lines"] += lang_data.get("net_lines", 0)
                        aggregated_data["languages"][lang]["additions"] += lang_data.get("additions", 0)
                        aggregated_data["languages"][lang]["deletions"] += lang_data.get("deletions", 0)
                    
                    # Aggregate per-date data, but only include dates within our target range
                    for date_str, date_data in period_data.get("per_date", {}).items():
                        if from_date <= date_str <= to_date:
                            aggregated_data["per_date"][date_str] = date_data
                    
                except Exception as e:
                    print(f"Error loading period data for {user_slug} in period {period['folder']}: {e}")
                    continue
    
    return aggregated_data


def build_users_overview_payload() -> Dict[str, Any]:
    user_periods = list_user_months()
    ignored = load_ignored_user_slugs()
    visible_users = [slug for slug in user_periods.keys() if slug not in ignored]
    overview: Dict[str, Any] = {}

    latest_month_period = None
    for slug in visible_users:
        for period in user_periods.get(slug, []):
            if period.get("is_yearly"):
                continue
            if not latest_month_period or period["from"] > latest_month_period["from"]:
                latest_month_period = period

    if latest_month_period:
        from_date = latest_month_period["from"]
        to_date = latest_month_period["to"]
        leaderboard = []
        total_commits = 0
        total_lines = 0
        for slug in visible_users:
            stats = aggregate_user_data_for_period(slug, from_date, to_date)
            commits = int(stats.get("total_commits", 0))
            lines_added = int(stats.get("total_lines_added", 0))
            if commits or lines_added:
                leaderboard.append({
                    "slug": slug,
                    "display_name": _get_user_display_name(slug, user_periods),
                    "monthly_commits": commits,
                    "monthly_lines_added": lines_added,
                })
                total_commits += commits
                total_lines += lines_added
        overview["activity"] = {
            "period": latest_month_period.get("label") or from_date[:7],
            "from": from_date,
            "to": to_date,
            "total_commits": total_commits,
            "total_lines_added": total_lines,
            "total_active_users": sum(1 for entry in leaderboard if entry["monthly_commits"] > 0),
            "most_active_monthly": sorted(leaderboard, key=lambda item: item["monthly_commits"], reverse=True),
            "most_productive_monthly": sorted(leaderboard, key=lambda item: item["monthly_lines_added"], reverse=True),
        }

    latest_year_period = None
    for slug in visible_users:
        for period in user_periods.get(slug, []):
            if not period.get("is_yearly"):
                continue
            if not latest_year_period or period["from"] > latest_year_period["from"]:
                latest_year_period = period

    if latest_year_period:
        from_date = latest_year_period["from"]
        to_date = latest_year_period["to"]
        year_label = latest_year_period.get("label") or from_date[:4]
        leaderboard = []
        total_commits = 0
        total_lines = 0
        for slug in visible_users:
            stats = aggregate_user_data_for_period(slug, from_date, to_date)
            commits = int(stats.get("total_commits", 0))
            lines_added = int(stats.get("total_lines_added", 0))
            if commits or lines_added:
                leaderboard.append({
                    "slug": slug,
                    "display_name": _get_user_display_name(slug, user_periods),
                    "yearly_commits": commits,
                    "yearly_lines_added": lines_added,
                })
                total_commits += commits
                total_lines += lines_added
        overview["yearly"] = {
            "year": year_label,
            "from": from_date,
            "to": to_date,
            "total_commits": total_commits,
            "total_lines_added": total_lines,
            "total_active_users": sum(1 for entry in leaderboard if entry["yearly_commits"] > 0),
            "most_active_yearly": sorted(leaderboard, key=lambda item: item["yearly_commits"], reverse=True),
            "most_productive_yearly": sorted(leaderboard, key=lambda item: item["yearly_lines_added"], reverse=True),
        }

    return overview


@app.route("/api/settings/available-users")
def api_settings_available_users():
    """Get list of available users for team member selection and ignore list management.
    Includes both active users (with recent commits) and inactive users (with ownership/blame).
    Returns only canonical users (filters out aliased identities)."""
    from collections import defaultdict
    
    # Load aliases to filter out non-canonical users
    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    alias_map = {}
    if os.path.exists(alias_file):
        try:
            alias_map = load_json(alias_file)
        except:
            pass
    
    # Build reverse map: aliased_slug -> canonical_slug
    aliased_to_canonical = {}
    for canonical, aliases in alias_map.items():
        if isinstance(aliases, list):
            for alias in aliases:
                aliased_to_canonical[alias] = canonical
        elif isinstance(aliases, str):
            aliased_to_canonical[aliases] = canonical
    
    users_dict = {}
    
    # Get active users from summaries
    user_months = list_user_months()
    for slug, months in user_months.items():
        # Get canonical slug
        canonical_slug = aliased_to_canonical.get(slug, slug)
        
        # Try to get a display name from any summary.json
        display_name = canonical_slug
        try:
            any_month = months[0]
            path = find_user_summary(canonical_slug, any_month["from"], any_month["to"])
            if not path:
                # Try with original slug if canonical didn't work
                path = find_user_summary(slug, any_month["from"], any_month["to"])
            if path:
                data = load_user_summary_file(path)
                if data and data.get("author_name"):
                    display_name = data["author_name"]
        except Exception:
            pass
        
        # Only add if not already present (prefer first seen display name)
        if canonical_slug not in users_dict:
            users_dict[canonical_slug] = {
                "slug": canonical_slug,
                "display_name": display_name,
                "active": True
            }
    
    # Also get inactive users from blame files (historical contributors)
    repos_path = os.path.join(STATS_ROOT, "repos")
    for root, dirs, files in os.walk(repos_path):
        if "blame.json" in files:
            blame_file = os.path.join(root, "blame.json")
            try:
                blame_data = load_json(blame_file)
                
                # Check repo-level developers
                developers = blame_data.get("developers", {})
                for dev_slug, dev_data in developers.items():
                    canonical_slug = aliased_to_canonical.get(dev_slug, dev_slug)
                    
                    if canonical_slug not in users_dict:
                        display_name = dev_data.get("display_name", canonical_slug) if isinstance(dev_data, dict) else canonical_slug
                        users_dict[canonical_slug] = {
                            "slug": canonical_slug,
                            "display_name": display_name,
                            "active": False
                        }
                
                # Check service-level developers
                services = blame_data.get("services", {})
                for service_data in services.values():
                    service_developers = service_data.get("developers", {})
                    for dev_slug, dev_data in service_developers.items():
                        canonical_slug = aliased_to_canonical.get(dev_slug, dev_slug)
                        
                        if canonical_slug not in users_dict:
                            display_name = dev_data.get("display_name", canonical_slug) if isinstance(dev_data, dict) else canonical_slug
                            users_dict[canonical_slug] = {
                                "slug": canonical_slug,
                                "display_name": display_name,
                                "active": False
                            }
            except Exception as e:
                continue
    
    # Convert to list
    users = list(users_dict.values())
    
    # Sort by display name for better UX
    users.sort(key=lambda u: u["display_name"].lower())
    
    return jsonify({"users": users})


@app.route("/api/settings/aliases", methods=["GET", "POST"])
def api_settings_aliases():
    if request.method == "GET":
        content = "{}"
        if os.path.isfile(ALIASES_FILE):
            with open(ALIASES_FILE, "r", encoding="utf-8") as fh:
                content = fh.read()
        return jsonify({"content": content})
    data = request.get_json(silent=True) or {}
    raw_content = data.get("content", "{}")
    try:
        parsed = json.loads(raw_content or "{}")
    except json.JSONDecodeError as exc:
        return jsonify({"error": f"Invalid JSON: {exc}"}), 400
    save_json(ALIASES_FILE, parsed)
    _reset_user_slug_index()
    return jsonify({"status": "saved"})


@app.route("/api/settings/ignore-users", methods=["GET", "POST"])
def api_settings_ignore_users():
    if request.method == "GET":
        content = ""
        if os.path.isfile(IGNORE_USERS_FILE):
            with open(IGNORE_USERS_FILE, "r", encoding="utf-8") as fh:
                content = fh.read()
        return jsonify({"content": content})
    data = request.get_json(silent=True) or {}
    content = data.get("content", "")
    os.makedirs(os.path.dirname(IGNORE_USERS_FILE), exist_ok=True)
    with open(IGNORE_USERS_FILE, "w", encoding="utf-8") as fh:
        fh.write((content.rstrip() + "\n") if content else "")
    return jsonify({"status": "saved"})


@app.route("/api/settings/subsystems", methods=["GET", "POST"])
def api_settings_subsystems():
    if request.method == "GET":
        content = "{}"
        if os.path.isfile(SERVICES_CONFIG_FILE):
            with open(SERVICES_CONFIG_FILE, "r", encoding="utf-8") as fh:
                content = fh.read()
        return jsonify({"content": content})
    data = request.get_json(silent=True) or {}
    raw_content = data.get("content", "{}")
    try:
        parsed = json.loads(raw_content or "{}")
    except json.JSONDecodeError as exc:
        return jsonify({"error": f"Invalid JSON: {exc}"}), 400
    save_json(SERVICES_CONFIG_FILE, parsed)
    return jsonify({"status": "saved"})


@app.route("/api/settings/teams", methods=["GET", "POST"])
def api_settings_teams():
    if request.method == "GET":
        content = "{}"
        if os.path.isfile(TEAMS_CONFIG_FILE):
            with open(TEAMS_CONFIG_FILE, "r", encoding="utf-8") as fh:
                content = fh.read()
        return jsonify({"content": content})
    data = request.get_json(silent=True) or {}
    raw_content = data.get("content", "{}")
    try:
        parsed = json.loads(raw_content or "{}")
    except json.JSONDecodeError as exc:
        return jsonify({"error": f"Invalid JSON: {exc}"}), 400
    save_json(TEAMS_CONFIG_FILE, parsed)
    return jsonify({"status": "saved"})


@app.route("/api/settings/capacity-config", methods=["GET", "POST"])
def api_settings_capacity_config():
    if request.method == "GET":
        return jsonify(load_json(CAPACITY_CONFIG_FILE, default={}))
    data = request.get_json(silent=True) or {}
    save_json(CAPACITY_CONFIG_FILE, data)
    return jsonify({"status": "saved"})


@app.route("/api/settings/team-subsystem-responsibilities", methods=["GET", "POST"])
def api_team_responsibilities_settings():
    if request.method == "GET":
        responsibilities = load_team_subsystem_responsibilities()
        teams = load_json(TEAMS_CONFIG_FILE, default={})
        subsystems = [sub.get("display_name") or sub.get("name") for sub in _discover_subsystems()]
        return jsonify({
            "responsibilities": responsibilities,
            "teams": teams,
            "available_subsystems": subsystems,
        })
    data = request.get_json(silent=True) or {}
    responsibilities = data.get("responsibilities", {})
    if not isinstance(responsibilities, dict):
        return jsonify({"error": "Responsibilities must be an object"}), 400
    save_json(TEAM_RESPONSIBILITIES_FILE, responsibilities)
    return jsonify({"status": "saved", "responsibilities": responsibilities})


# Static files (for completeness; Flask static_folder already serves /static/<file>)
def get_user_monthly_stats(user_slug: str, year: int) -> List[Dict[str, Any]]:
    """
    Get monthly line addition/deletion statistics for a user for a specific year.
    Returns list of monthly data with month names and line counts.
    """
    user_months = list_user_months()
    actual_slug = resolve_user_slug(user_slug) or user_slug
    if actual_slug not in user_months:
        return []
    
    monthly_stats = []
    
    # Get all month periods for this user
    for period in user_months[actual_slug]:
        if period["is_yearly"]:
            continue  # Skip yearly summaries
        
        # Check if this period is in the requested year
        if not period["from"].startswith(str(year)):
            continue
            
        # Load the summary for this month
        try:
            summary_path = resolve_user_period_summary_path(actual_slug, period)
            if summary_path and os.path.exists(summary_path):
                data = load_user_summary_file(summary_path)
                monthly_stats.append({
                    "month": period["label"],  # YYYY-MM format
                    "month_name": datetime.strptime(period["from"], "%Y-%m-%d").strftime("%B"),
                    "lines_added": data.get("total_lines_added", 0),
                    "lines_deleted": data.get("total_lines_deleted", 0),
                    "commits": data.get("total_commits", 0)
                })
        except (json.JSONDecodeError, IOError, ValueError):
            continue
    
    # Sort by month
    monthly_stats.sort(key=lambda x: x["month"])
    return monthly_stats


def get_user_last_month_stats(user_slug: str) -> Dict[str, Any]:
    """
    Get last month statistics for a user.
    Returns data for the most recent completed month.
    """
    # Get the last completed month
    now = datetime.now()
    if now.month == 1:
        last_month = 12
        last_year = now.year - 1
    else:
        last_month = now.month - 1
        last_year = now.year
    
    last_month_str = f"{last_year:04d}-{last_month:02d}"
    
    user_months = list_user_months()
    actual_slug = resolve_user_slug(user_slug) or user_slug
    if actual_slug not in user_months:
        return {"month": last_month_str, "month_name": "", "lines_added": 0, "lines_deleted": 0, "commits": 0}
    
    # Find the specific month data
    for period in user_months[actual_slug]:
        if period["is_yearly"]:
            continue
        if period["label"] == last_month_str:
            try:
                summary_path = resolve_user_period_summary_path(actual_slug, period)
                if summary_path and os.path.exists(summary_path):
                    data = load_user_summary_file(summary_path)
                    return {
                        "month": period["label"],
                        "month_name": datetime.strptime(period["from"], "%Y-%m-%d").strftime("%B %Y"),
                        "lines_added": data.get("total_lines_added", 0),
                        "lines_deleted": data.get("total_lines_deleted", 0),
                        "commits": data.get("total_commits", 0)
                    }
            except (json.JSONDecodeError, IOError, ValueError):
                break
    
    # Return empty stats if no data found
    month_name = datetime(last_year, last_month, 1).strftime("%B %Y")
    return {"month": last_month_str, "month_name": month_name, "lines_added": 0, "lines_deleted": 0, "commits": 0}


def get_team_last_month_stats(team_id: str) -> Dict[str, Any]:
    """
    Get last month aggregated statistics for a team.
    Returns data for the most recent completed month.
    """
    # Get the last completed month
    now = datetime.now()
    if now.month == 1:
        last_month = 12
        last_year = now.year - 1
    else:
        last_month = now.month - 1
        last_year = now.year
    
    last_month_str = f"{last_year:04d}-{last_month:02d}"
    month_name = datetime(last_year, last_month, 1).strftime("%B %Y")
    
    # Load teams configuration
    teams_path = os.path.join(BASE_DIR, "configuration/teams.json")
    if not os.path.exists(teams_path):
        return {"month": last_month_str, "month_name": month_name, "lines_added": 0, "lines_deleted": 0, "commits": 0}
    
    try:
        with open(teams_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"month": last_month_str, "month_name": month_name, "lines_added": 0, "lines_deleted": 0, "commits": 0}
    
    # Find the team using the correct structure
    if team_id not in teams_config:
        return {"month": last_month_str, "month_name": month_name, "lines_added": 0, "lines_deleted": 0, "commits": 0}
    
    team = teams_config[team_id]
    
    # Aggregate statistics for team members in the last month
    total_lines_added = 0
    total_lines_deleted = 0
    total_commits = 0
    
    user_months = list_user_months()
    
    for member_slug in team.get("members", []):
        resolved_member = resolve_user_slug(member_slug) or member_slug
        member_periods = user_months.get(resolved_member)
        if not member_periods:
            continue
        
        # Find the specific month data for this member
        for period in member_periods:
            if period["is_yearly"]:
                continue
            if period["label"] == last_month_str:
                try:
                    summary_path = resolve_user_period_summary_path(resolved_member, period)
                    if summary_path and os.path.exists(summary_path):
                        data = load_user_summary_file(summary_path)
                        total_lines_added += data.get("total_lines_added", 0)
                        total_lines_deleted += data.get("total_lines_deleted", 0)
                        total_commits += data.get("total_commits", 0)
                except (json.JSONDecodeError, IOError, ValueError):
                    continue
                break
    
    return {
        "month": last_month_str,
        "month_name": month_name,
        "lines_added": total_lines_added,
        "lines_deleted": total_lines_deleted,
        "commits": total_commits
    }


def get_team_monthly_stats(team_id: str, year: int) -> List[Dict[str, Any]]:
    """
    Get aggregated monthly line addition/deletion statistics for a team for a specific year.
    """
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        return []
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []
    
    if team_id not in teams_config:
        return []
    
    team = teams_config[team_id]
    members = team.get("members", [])
    
    if not members:
        return []
    
    # Get monthly stats for each team member
    team_monthly_stats = {}
    
    for member in members:
        member_stats = get_user_monthly_stats(member, year)
        
        for month_data in member_stats:
            month = month_data["month"]
            
            if month not in team_monthly_stats:
                team_monthly_stats[month] = {
                    "month": month,
                    "month_name": month_data["month_name"],
                    "lines_added": 0,
                    "lines_deleted": 0,
                    "commits": 0
                }
            
            team_monthly_stats[month]["lines_added"] += month_data["lines_added"]
            team_monthly_stats[month]["lines_deleted"] += month_data["lines_deleted"]
            team_monthly_stats[month]["commits"] += month_data["commits"]
    
    # Convert to list and sort by month
    monthly_stats = list(team_monthly_stats.values())
    monthly_stats.sort(key=lambda x: x["month"])
    return monthly_stats


@app.route("/api/users/<user_slug>/monthly-stats/<int:year>")
def api_user_monthly_stats(user_slug: str, year: int):
    """Get monthly line addition/deletion statistics for a user."""
    try:
        stats = get_user_monthly_stats(user_slug, year)
        return jsonify({"monthly_stats": stats})
    except Exception as e:
        app.logger.error(f"Error getting user monthly stats: {e}")
        abort(500, description="Failed to get monthly statistics")


@app.route("/api/teams/<team_id>/monthly-stats/<int:year>")
def api_team_monthly_stats(team_id: str, year: int):
    """Get aggregated monthly line addition/deletion statistics for a team."""
    try:
        stats = get_team_monthly_stats(team_id, year)
        return jsonify({"monthly_stats": stats})
    except Exception as e:
        app.logger.error(f"Error getting team monthly stats: {e}")
        abort(500, description="Failed to get monthly statistics")


@app.route("/api/users/<user_slug>/last-month-stats")
def api_user_last_month_stats(user_slug: str):
    """Get last month statistics for a user."""
    try:
        stats = get_user_last_month_stats(user_slug)
        return jsonify({"last_month_stats": stats})
    except Exception as e:
        app.logger.error(f"Error getting user last month stats: {e}")
        abort(500, description="Failed to get last month statistics")


@app.route("/api/teams/<team_id>/last-month-stats")
def api_team_last_month_stats(team_id: str):
    """Get last month statistics for a team."""
    try:
        stats = get_team_last_month_stats(team_id)
        return jsonify({"last_month_stats": stats})
    except Exception as e:
        app.logger.error(f"Error getting team last month stats: {e}")
        abort(500, description="Failed to get last month statistics")


def get_user_daily_stats(user_slug: str, year: int, month: int) -> List[Dict[str, Any]]:
    """
    Get daily line addition/deletion statistics for a user for a specific month.
    Returns list of daily data with dates and line counts.
    Aggregates data from all aliased user names.
    Also matches folders by normalized name to include accented/variant slugs.
    """
    users_root = os.path.join(STATS_ROOT, "users")
    
    # Load aliases to get all user names that should be aggregated
    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    alias_map = {}
    if os.path.exists(alias_file):
        try:
            alias_map = load_json(alias_file)
        except:
            pass
    
    user_names = [user_slug]
    
    # Find all aliases that map to this user
    for canonical, aliases in alias_map.items():
        if canonical == user_slug:
            user_names.extend(aliases)
        elif isinstance(aliases, list) and user_slug in aliases:
            user_names.append(canonical)
            user_names.extend([a for a in aliases if a != user_slug])
            break
        elif isinstance(aliases, str) and user_slug == aliases:
            user_names.append(canonical)
            break
    
    # Also try to match by normalized folder names (handles accents/case differences)
    try:
        import unicodedata
        def normalize(s: str) -> str:
            s = unicodedata.normalize('NFKD', s)
            s = ''.join(c for c in s if not unicodedata.combining(c))
            return s.lower().strip()
        normalized_target = normalize(user_slug)
        if os.path.isdir(users_root):
            for folder in os.listdir(users_root):
                try:
                    if normalize(folder) == normalized_target and folder not in user_names:
                        user_names.append(folder)
                except Exception:
                    continue
    except Exception:
        pass
    
    # Aggregate daily stats from all user names
    aggregated_by_date = {}
    target_month = f"{year:04d}-{month:02d}"
    user_months_all = list_user_months()
    
    for username in user_names:
        user_dir = os.path.join(users_root, username)
        if not os.path.exists(user_dir):
            continue
        
        user_periods = user_months_all.get(username, [])
        
        for period in user_periods:
            if period.get("is_yearly"):
                continue
            if period.get("from", "")[:7] != target_month:
                continue
            summary_path = resolve_user_period_summary_path(username, period)
            
            if summary_path and os.path.exists(summary_path):
                summary_data = load_user_summary_file(summary_path)
                
                per_date_data = summary_data.get("per_date", {})
                
                for date_str, day_data in per_date_data.items():
                    if date_str[:7] != target_month:
                        continue
                    if date_str not in aggregated_by_date:
                        aggregated_by_date[date_str] = {
                            "lines_added": 0,
                            "lines_deleted": 0,
                            "commits": 0
                        }
                    aggregated_by_date[date_str]["lines_added"] += day_data.get("additions", 0)
                    aggregated_by_date[date_str]["lines_deleted"] += day_data.get("deletions", 0)
                    aggregated_by_date[date_str]["commits"] += day_data.get("commits", 0)
                
                break
    
    # Convert to list format
    daily_stats = []
    for date_str, day_data in aggregated_by_date.items():
        daily_stats.append({
            "date": date_str,
            "day": int(date_str.split("-")[2]),
            "lines_added": day_data["lines_added"],
            "lines_deleted": day_data["lines_deleted"],
            "commits": day_data["commits"]
        })
    
    # Sort by date
    daily_stats.sort(key=lambda x: x["date"])
    return daily_stats


def get_team_daily_stats(team_id: str, year: int, month: int) -> List[Dict[str, Any]]:
    """
    Get aggregated daily line addition/deletion statistics for a team for a specific month.
    """
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        return []
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error reading teams file: {e}")
        return []
    
    if team_id not in teams:
        return []
    
    original_members = teams[team_id].get("members", [])
    team_members = canonicalize_team_members(original_members)
    if not team_members:
        return []
    
    # Get daily stats for each team member
    team_daily_stats = {}
    
    for member in team_members:
        member_stats = get_user_daily_stats(member, year, month)
        
        for day_data in member_stats:
            date = day_data["date"]
            
            if date not in team_daily_stats:
                team_daily_stats[date] = {
                    "date": date,
                    "day": day_data["day"],
                    "lines_added": 0,
                    "lines_deleted": 0,
                    "commits": 0
                }
            
            team_daily_stats[date]["lines_added"] += day_data["lines_added"]
            team_daily_stats[date]["lines_deleted"] += day_data["lines_deleted"] 
            team_daily_stats[date]["commits"] += day_data["commits"]
    
    # Convert to list and sort by date
    daily_stats = list(team_daily_stats.values())
    daily_stats.sort(key=lambda x: x["date"])
    return daily_stats


@app.route("/api/users/<user_slug>/daily-stats/<int:year>/<int:month>")
def api_user_daily_stats(user_slug: str, year: int, month: int):
    """Get daily line addition/deletion statistics for a user for a specific month."""
    try:
        stats = get_user_daily_stats(user_slug, year, month)
        return jsonify({"daily_stats": stats})
    except Exception as e:
        import traceback
        app.logger.error(f"Error getting user daily stats for {user_slug} {year}-{month}: {e}")
        app.logger.error(traceback.format_exc())
        abort(500, description=f"Failed to get daily statistics: {str(e)}")

@app.route("/api/users/<user_slug>/daily-stats/<int:year>")
def api_user_daily_stats_year(user_slug: str, year: int):
    """Get aggregated daily statistics for a user across the entire year."""
    try:
        aggregated = {}
        # Aggregate each month present for the user in the given year
        monthly_stats = get_user_monthly_stats(user_slug, year)
        for m in monthly_stats:
            # month is in YYYY-MM
            try:
                month_num = int(m["month"].split("-")[1])
            except Exception:
                continue
            for day in get_user_daily_stats(user_slug, year, month_num):
                date = day["date"]
                if date not in aggregated:
                    aggregated[date] = {"lines_added": 0, "lines_deleted": 0, "commits": 0}
                aggregated[date]["lines_added"] += day["lines_added"]
                aggregated[date]["lines_deleted"] += day["lines_deleted"]
                aggregated[date]["commits"] += day["commits"]
        # Convert to list
        result = [{"date": d, "day": int(d.split("-")[2]), **vals} for d, vals in sorted(aggregated.items())]
        return jsonify({"daily_stats": result})
    except Exception as e:
        import traceback
        app.logger.error(f"Error getting yearly daily stats for {user_slug} {year}: {e}")
        app.logger.error(traceback.format_exc())
        abort(500, description=f"Failed to get yearly daily statistics: {str(e)}")


@app.route("/api/teams/<team_id>/daily-stats/<int:year>/<int:month>")
def api_team_daily_stats(team_id: str, year: int, month: int):
    """Get aggregated daily line addition/deletion statistics for a team for a specific month."""
    try:
        stats = get_team_daily_stats(team_id, year, month)
        return jsonify({"daily_stats": stats})
    except Exception as e:
        app.logger.error(f"Error getting team daily stats: {e}")
        abort(500, description="Failed to get daily statistics")


@app.route("/static/<path:filename>")
def static_files(filename: str):
    return send_from_directory(app.static_folder, filename)


if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, cleanup_on_shutdown)
    signal.signal(signal.SIGTERM, cleanup_on_shutdown)
    atexit.register(cleanup_on_shutdown)

    # Clean up orphaned temp directories from previous runs
    cleanup_orphaned_temp_directories()

    # You can set host="0.0.0.0" if you want to reach it from other machines
    # Exclude repos directory from file watcher to prevent restarts during cloning
    parser = argparse.ArgumentParser(description="Dashboard server")
    parser.add_argument("--host", "--listen-address", dest="host", default="127.0.0.1", help="Host/IP to bind the dashboard server")
    parser.add_argument("--port", type=int, default=5001, help="Port to bind the dashboard server")
    parser.add_argument("--read-only", action="store_true", help="Run dashboard in read-only mode (disable updates/settings)")
    parser.add_argument("--disable-logo", action="store_true", help="Hide repo-squirrel branding in the sidebar header")
    args = parser.parse_args()

    app.config["READ_ONLY_MODE"] = args.read_only
    app.config["SHOW_LOGO"] = not args.disable_logo

    app.run(host=args.host, port=args.port, debug=True, use_reloader=False,
            exclude_patterns=["repos/*", "repos/**/*", "stats/*", "stats/**/*"])

