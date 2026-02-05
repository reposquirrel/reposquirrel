#!/usr/bin/env python3
"""Generate blame, contribution, and language statistics per subsystem."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

UTC = timezone.utc
ROOT_SUBSYSTEM_KEY = "__root__"
MASTER_PROGRESS_PREFIX = "[[MASTER_PROGRESS]]"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True, help="Target year, e.g. 2025")
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), help="Target month (1-12)")
    parser.add_argument(
        "--repos-dir",
        type=Path,
        default=Path("repos"),
        help="Directory containing git repositories (default: repos)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("configuration"),
        help="Directory containing configuration JSON files (default: configuration)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("stats"),
        help="Root directory for generated statistics (default: stats)",
    )
    parser.add_argument(
        "--repo",
        action="append",
        dest="repos",
        help="Limit processing to specific repo paths (relative to --repos-dir, e.g. appgate-sdp-int/stratus-appliance).",
    )
    parser.add_argument(
        "--progress-events",
        action="store_true",
        help="Emit progress markers for each completed repo",
    )
    return parser.parse_args()


@dataclass
class Subsystem:
    name: Optional[str]
    include_paths: List[str]
    repo_rel_path: str
    repo_name: str
    is_remainder: bool = False
    exclusion_paths: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.include_paths = [normalize_service_path(p) for p in self.include_paths if p]
        self.exclusion_paths = [normalize_service_path(p) for p in self.exclusion_paths if p]

    @property
    def identifier(self) -> str:
        return self.name or ROOT_SUBSYSTEM_KEY

    @property
    def display_name(self) -> str:
        return self.name or self.repo_name

    def matches(self, file_path: str) -> bool:
        if self.is_remainder or not self.include_paths:
            return False
        return any(path_matches_prefix(file_path, prefix) for prefix in self.include_paths)

    def stats_base_path(self, stats_root: Path) -> Path:
        parts = self.repo_rel_path.split("/")
        base = Path(stats_root, "repos", *parts)
        if self.name:
            base = base / self.name
        return base


@dataclass
class RepoContext:
    rel_path: str
    abs_path: Path
    repo_name: str
    explicit_subsystems: List[Subsystem]
    remainder: Subsystem

    def subsystems(self) -> List[Subsystem]:
        return self.explicit_subsystems + [self.remainder]

    def classify(self, file_path: str) -> Subsystem:
        for subsystem in self.explicit_subsystems:
            if subsystem.matches(file_path):
                return subsystem
        return self.remainder


def normalize_service_path(path: str) -> str:
    cleaned = path.strip().lstrip("./")
    return cleaned.rstrip("/")


def path_matches_prefix(path: str, prefix: str) -> bool:
    if not prefix:
        return False
    prefix = prefix.rstrip("/")
    if path == prefix:
        return True
    return path.startswith(f"{prefix}/")


def _extract_hour_key(date_value: Optional[object]) -> Optional[str]:
    if not isinstance(date_value, str) or not date_value:
        return None
    candidate = date_value.strip()
    if not candidate:
        return None
    normalized = candidate.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return f"{dt.hour:02d}"
    except ValueError:
        pass
    time_part: Optional[str] = None
    if "T" in candidate:
        time_part = candidate.split("T", 1)[1]
    elif " " in candidate:
        time_part = candidate.split(" ", 1)[1]
    if time_part and len(time_part) >= 2 and time_part[:2].isdigit():
        hour_val = int(time_part[:2]) % 24
        return f"{hour_val:02d}"
    return None


def normalize_per_hour(per_hour: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    if not per_hour:
        return {}

    def sort_key(hour: str) -> Tuple[int, str]:
        try:
            numeric = int(hour)
            return (0, f"{numeric:02d}")
        except ValueError:
            return (1, hour)

    sorted_keys = sorted(per_hour.keys(), key=sort_key)
    return {key: per_hour[key] for key in sorted_keys}


def load_json_file(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_alias_index(path: Path) -> Dict[str, str]:
    data = load_json_file(path)
    index: Dict[str, str] = {}
    for canonical, aliases in data.items():
        if not isinstance(aliases, list):
            continue
        index[canonical.lower()] = canonical
        for alias in aliases:
            index[alias.lower()] = canonical
    return index


def load_ignore_users(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ignored: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            entry = line.strip()
            if entry:
                ignored.add(entry.lower())
    return ignored


def discover_git_repos(root: Path) -> Dict[str, Path]:
    repos: Dict[str, Path] = {}
    if not root.exists():
        return repos
    for scope in sorted(p for p in root.iterdir() if p.is_dir()):
        for repo_dir in sorted(p for p in scope.iterdir() if p.is_dir()):
            if (repo_dir / ".git").exists():
                rel = f"{scope.name}/{repo_dir.name}"
                repos[rel] = repo_dir
    return repos


def normalize_repo_filter(value: str) -> str:
    cleaned = value.strip().strip("/")
    if cleaned.startswith("repos/"):
        cleaned = cleaned[6:]
    return cleaned


def resolve_requested_repo(value: str, available: Sequence[str]) -> Optional[str]:
    candidate = normalize_repo_filter(value)
    if not candidate:
        return None
    current = candidate
    while True:
        if current in available:
            return current
        if "/" not in current:
            break
        current = current.rsplit("/", 1)[0]
    return None


def build_repo_contexts(
    discovered: Dict[str, Path], services_config: Dict[str, Dict[str, List[str]]]
) -> List[RepoContext]:
    contexts: List[RepoContext] = []
    for rel_path in sorted(discovered.keys()):
        repo_path = discovered[rel_path]
        repo_name = repo_path.name
        config = services_config.get(rel_path, {})
        explicit: List[Subsystem] = []
        for subsystem_name in sorted(config.keys()):
            include_paths = config[subsystem_name]
            explicit.append(
                Subsystem(
                    name=subsystem_name,
                    include_paths=include_paths,
                    repo_rel_path=rel_path,
                    repo_name=repo_name,
                )
            )
        exclusion_paths = [path for sub in explicit for path in sub.include_paths]
        remainder = Subsystem(
            name=None,
            include_paths=[],
            repo_rel_path=rel_path,
            repo_name=repo_name,
            is_remainder=True,
            exclusion_paths=exclusion_paths,
        )
        contexts.append(
            RepoContext(
                rel_path=rel_path,
                abs_path=repo_path,
                repo_name=repo_name,
                explicit_subsystems=explicit,
                remainder=remainder,
            )
        )
    return contexts


def run_git_command(repo_path: Path, args: Sequence[str]) -> subprocess.CompletedProcess:
    cmd = ["git", "-C", str(repo_path)] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")


def resolve_snapshot_commit(repo: RepoContext, snapshot_iso: str) -> Optional[str]:
    result = run_git_command(repo.abs_path, ["rev-list", "-1", f"--before={snapshot_iso}", "HEAD"])
    if result.returncode != 0:
        print(f"[WARN] Unable to resolve commit for {repo.rel_path}: {result.stderr.strip()}")
        return None
    commit = result.stdout.strip()
    return commit or None


def list_files_at_commit(repo: RepoContext, commit: str) -> List[str]:
    result = run_git_command(repo.abs_path, ["ls-tree", "-r", "--name-only", commit])
    if result.returncode != 0:
        raise RuntimeError(f"git ls-tree failed for {repo.rel_path}: {result.stderr.strip()}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def blame_file(
    repo: RepoContext,
    commit: str,
    file_path: str,
    canonicalize,
    ignored_users: set[str],
) -> Counter:
    cmd = [
        "git",
        "-C",
        str(repo.abs_path),
        "blame",
        commit,
        "--line-porcelain",
        "--",
        file_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(f"[WARN] git blame failed for {repo.rel_path}:{file_path}: {exc.stderr.strip() if exc.stderr else exc}")
        return Counter()
    counts: Counter = Counter()
    current_author: Optional[str] = None
    for line in result.stdout.splitlines():
        if line.startswith("author "):
            current_author = line[7:].strip()
        elif line.startswith("\t"):
            if not current_author:
                continue
            user = canonicalize(current_author)
            if not user:
                continue
            if user.lower() in ignored_users:
                continue
            counts[user] += 1
    return counts


def collect_blame_for_subsystem(
    repo: RepoContext,
    commit: str,
    files: List[str],
    canonicalize,
    ignored_users: set[str],
) -> Tuple[int, List[Dict[str, float]], Counter]:
    total_lines = 0
    aggregate: Counter = Counter()
    for file_path in files:
        file_counts = blame_file(repo, commit, file_path, canonicalize, ignored_users)
        aggregate.update(file_counts)
    total_lines = sum(aggregate.values())
    if not total_lines:
        return 0, [], aggregate
    owners = [
        {
            "user": user,
            "lines": lines,
            "percentage": round((lines / total_lines) * 100, 2),
        }
        for user, lines in aggregate.most_common()
    ]
    return total_lines, owners, aggregate


def parse_git_log_month(
    repo: RepoContext, since_iso: str, until_iso: str
) -> List[Dict[str, object]]:
    format_str = "%H%x00%an%x00%ae%x00%ad"
    cmd = [
        "log",
        "--no-merges",
        "--no-renames",
        f"--since={since_iso}",
        f"--until={until_iso}",
        "--numstat",
        "--date=iso-strict",
        f"--format={format_str}",
    ]
    result = run_git_command(repo.abs_path, cmd)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        benign_markers = (
            "does not have any commits yet",
            "unknown revision or path",  # e.g. empty repo
        )
        if any(marker in stderr for marker in benign_markers):
            return []
        raise RuntimeError(f"git log failed for {repo.rel_path}: {stderr}")
    entries: List[Dict[str, object]] = []
    current: Optional[Dict[str, object]] = None
    for raw_line in result.stdout.splitlines():
        line = raw_line.rstrip("\n")
        if not line:
            continue
        if "\x00" in line:
            if current and current.get("changes"):
                entries.append(current)
            parts = line.split("\x00")
            if len(parts) == 4:
                sha, author, email, date_str = parts
            else:
                sha, author, email = parts[:3]
                date_str = ""
            current = {
                "sha": sha,
                "author": author,
                "email": email,
                "date": date_str,
                "changes": [],
            }
        else:
            if current is None:
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                continue
            added_raw, removed_raw, path = parts
            added = int(added_raw) if added_raw.isdigit() else 0
            removed = int(removed_raw) if removed_raw.isdigit() else 0
            current["changes"].append({"path": path, "added": added, "removed": removed})
    if current and current.get("changes"):
        entries.append(current)
    return entries


def canonicalize_user_builder(alias_index: Dict[str, str]):
    def canonicalize(name: str) -> Optional[str]:
        cleaned = name.strip()
        if not cleaned:
            return None
        lookup = cleaned.lower()
        return alias_index.get(lookup, cleaned)

    return canonicalize


def normalize_email_value(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate or "@" not in candidate or " " in candidate:
        return None
    if candidate.lower().startswith("http"):
        return None
    return candidate


def record_user_email(target: Dict[str, object], email_value: object) -> None:
    normalized = normalize_email_value(email_value)
    if not normalized:
        return
    emails = target.setdefault("_emails", set())
    if not isinstance(emails, set):
        emails = {item for item in emails if isinstance(item, str)}
    emails.add(normalized)
    target["_emails"] = emails
    existing_primary = normalize_email_value(target.get("_primary_email"))
    if not existing_primary:
        target["_primary_email"] = normalized


def safe_user_dirname(user: str) -> str:
    return user.replace("/", "_").strip()


def write_json_file(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def aggregate_user_stats(
    repo: RepoContext,
    month_entries: List[Dict[str, object]],
    canonicalize,
    ignored_users: set[str],
    user_accumulator: Dict[str, Dict],
    subsystem_accumulator: Dict[Tuple[str, str], Dict[str, object]],
    subsystem_lookup: Dict[str, Subsystem],
    period_from: str,
    period_to: str,
) -> None:
    for entry in month_entries:
        author = canonicalize(entry["author"])
        if not author or author.lower() in ignored_users:
            continue
        changes = entry.get("changes", [])
        if not changes:
            continue
        user_stats = user_accumulator.setdefault(
            author,
            {
                "summary": {
                    "commits": 0,
                    "lines_added": 0,
                    "lines_removed": 0,
                    "lines_net": 0,
                    "files_changed": 0,
                },
                "subsystems": {},
                "per_date": {},
                "per_hour": {},
            },
        )
        record_user_email(user_stats, entry.get("email"))
        commit_lines_added = 0
        commit_lines_removed = 0
        files_changed = 0
        touched_keys: set[Tuple[str, str]] = set()
        per_subsystem_totals: Dict[Tuple[str, str], Dict[str, int]] = {}
        for change in changes:  # type: ignore[var-annotated]
            path = change.get("path", "")
            added = int(change.get("added", 0))
            removed = int(change.get("removed", 0))
            subsystem = repo.classify(normalize_service_path(path))
            key = (repo.rel_path, subsystem.identifier)
            per_entry = per_subsystem_totals.setdefault(
                key,
                {
                    "repo": repo.rel_path,
                    "subsystem": subsystem.display_name,
                    "commits": 0,
                    "lines_added": 0,
                    "lines_removed": 0,
                    "lines_net": 0,
                    "files_changed": 0,
                },
            )
            per_entry["lines_added"] += added
            per_entry["lines_removed"] += removed
            per_entry["lines_net"] = per_entry["lines_added"] - per_entry["lines_removed"]
            per_entry["files_changed"] += 1
            commit_lines_added += added
            commit_lines_removed += removed
            files_changed += 1
            touched_keys.add(key)
        for key in touched_keys:
            per_subsystem_totals[key]["commits"] += 1
        summary = user_stats["summary"]
        summary["commits"] += 1
        summary["lines_added"] += commit_lines_added
        summary["lines_removed"] += commit_lines_removed
        summary["lines_net"] = summary["lines_added"] - summary["lines_removed"]
        summary["files_changed"] += files_changed
        per_date = user_stats.setdefault("per_date", {})
        date_value = entry.get("date")
        date_str = (date_value or "")[:10]
        if date_str:
            day_entry = per_date.setdefault(date_str, {"commits": 0, "additions": 0, "deletions": 0})
            day_entry["commits"] += 1
            day_entry["additions"] += commit_lines_added
            day_entry["deletions"] += commit_lines_removed
        per_hour = user_stats.setdefault("per_hour", {})
        hour_key = _extract_hour_key(date_value)
        if hour_key:
            hour_entry = per_hour.setdefault(hour_key, {"commits": 0, "additions": 0, "deletions": 0})
            hour_entry["commits"] += 1
            hour_entry["additions"] += commit_lines_added
            hour_entry["deletions"] += commit_lines_removed
        subsystems = user_stats["subsystems"]
        for key, data in per_subsystem_totals.items():
            existing = subsystems.setdefault(key, data.copy())
            if existing is data:
                continue
            existing["commits"] += data["commits"]
            existing["lines_added"] += data["lines_added"]
            existing["lines_removed"] += data["lines_removed"]
            existing["lines_net"] = existing["lines_added"] - existing["lines_removed"]
            existing["files_changed"] += data["files_changed"]
        for key, data in per_subsystem_totals.items():
            repo_rel, subsystem_id = key
            subsystem = subsystem_lookup.get(subsystem_id)
            if not subsystem:
                continue
            stats_entry = get_or_create_subsystem_entry(
                subsystem_accumulator,
                key,
                subsystem,
                repo_rel,
                period_from,
                period_to,
            )
            summary_block = stats_entry["summary"]
            summary_block["total_commits"] += data["commits"]
            summary_block["total_lines_added"] += data["lines_added"]
            summary_block["total_lines_deleted"] += data["lines_removed"]
            summary_block["total_changed_lines"] += data["lines_added"] + data["lines_removed"]
            summary_block["lines_net"] = (
                summary_block["total_lines_added"] - summary_block["total_lines_deleted"]
            )
            summary_block["files_changed"] += data["files_changed"]
            stats_entry["contributors"].add(author)
            repo_entry = ensure_repo_entry(stats_entry, repo_rel)
            repo_entry["commits"] += data["commits"]
            repo_entry["lines_added"] += data["lines_added"]
            repo_entry["lines_deleted"] += data["lines_removed"]
            repo_entry["lines_net"] = repo_entry["lines_added"] - repo_entry["lines_deleted"]
            repo_entry["files_changed"] += data["files_changed"]
            repo_dev = ensure_developer_entry(repo_entry["developers"], author)
            repo_dev["commits"] += data["commits"]
            repo_dev["lines_added"] += data["lines_added"]
            repo_dev["lines_deleted"] += data["lines_removed"]
            repo_dev["lines_net"] = repo_dev["lines_added"] - repo_dev["lines_deleted"]
            repo_dev["changed_lines"] += data["lines_added"] + data["lines_removed"]
            repo_dev["files_changed"] += data["files_changed"]
            overall_dev = ensure_developer_entry(stats_entry["developers"], author)
            overall_dev["commits"] += data["commits"]
            overall_dev["lines_added"] += data["lines_added"]
            overall_dev["lines_deleted"] += data["lines_removed"]
            overall_dev["lines_net"] = overall_dev["lines_added"] - overall_dev["lines_deleted"]
            overall_dev["changed_lines"] += data["lines_added"] + data["lines_removed"]
            overall_dev["files_changed"] += data["files_changed"]
            per_date_block = stats_entry.setdefault("per_date", {})
            if date_str:
                day_entry = per_date_block.setdefault(
                    date_str,
                    {"commits": 0, "additions": 0, "deletions": 0, "changed_lines": 0},
                )
                day_entry["commits"] += data["commits"]
                day_entry["additions"] += data["lines_added"]
                day_entry["deletions"] += data["lines_removed"]
                day_entry["changed_lines"] += data["lines_added"] + data["lines_removed"]


def get_or_create_subsystem_entry(
    accumulator: Dict[Tuple[str, str], Dict[str, object]],
    key: Tuple[str, str],
    subsystem: Subsystem,
    repo_rel: str,
    period_from: str,
    period_to: str,
) -> Dict[str, object]:
    entry = accumulator.get(key)
    if entry:
        return entry
    entry = {
        "subsystem": subsystem,
        "repo_rel": repo_rel,
        "summary": {
            "total_commits": 0,
            "total_lines_added": 0,
            "total_lines_deleted": 0,
            "total_changed_lines": 0,
            "lines_net": 0,
            "files_changed": 0,
        },
        "developers": {},
        "repositories": {},
        "per_date": {},
        "contributors": set(),
        "period_from": period_from,
        "period_to": period_to,
    }
    accumulator[key] = entry
    return entry


def ensure_repo_entry(entry: Dict[str, object], repo_rel: str) -> Dict[str, object]:
    repositories: Dict[str, Dict[str, object]] = entry["repositories"]  # type: ignore[index]
    repo_entry = repositories.setdefault(
        repo_rel,
        {
            "repo": repo_rel,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "lines_net": 0,
            "files_changed": 0,
            "developers": {},
        },
    )
    return repo_entry


def ensure_developer_entry(container: Dict[str, Dict[str, object]], slug: str) -> Dict[str, object]:
    return container.setdefault(
        slug,
        {
            "display_name": slug,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "lines_net": 0,
            "changed_lines": 0,
            "files_changed": 0,
        },
    )


def build_owner_entries(counter: Counter, total_lines: int) -> List[Dict[str, float]]:
    if not total_lines:
        total = sum(counter.values())
    else:
        total = total_lines
    if not total:
        return []
    entries: List[Dict[str, float]] = []
    for user, lines in counter.most_common():
        percentage = round((lines / total) * 100, 2) if total else 0.0
        entries.append({"user": user, "lines": lines, "percentage": percentage})
    return entries


def write_repo_blame_files(stats_root: Path, repo: RepoContext, year: int, month_key: str, payload: Dict) -> None:
    repo_parts = repo.rel_path.split("/")
    blame_dir = Path(stats_root, "repos", *repo_parts, "blame")
    month_path = blame_dir / str(year) / f"{month_key}.json"
    write_json_file(month_path, payload)
    write_json_file(blame_dir / "latest.json", payload)


def normalize_per_date(per_date: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    if not per_date:
        return {}
    return {key: per_date[key] for key in sorted(per_date.keys())}


def prepare_user_month_payload(
    user: str,
    year: int,
    month: int,
    stats: Dict,
    generated_at: str,
) -> Dict:
    subsystems_list = sorted(
        stats["subsystems"].values(),
        key=lambda item: (item["repo"], item["subsystem"]),
    )
    summary = stats["summary"].copy()
    summary["subsystems_touched"] = len(subsystems_list)
    per_date = normalize_per_date(stats.get("per_date", {}))
    per_hour = normalize_per_hour(stats.get("per_hour", {}))

    email_candidates: set[str] = set()
    raw_emails = stats.get("_emails")
    if isinstance(raw_emails, set):
        iterable = raw_emails
    elif isinstance(raw_emails, list):
        iterable = raw_emails
    else:
        iterable = []
    for value in iterable:
        normalized = normalize_email_value(value)
        if normalized:
            email_candidates.add(normalized)
    primary_email = normalize_email_value(stats.get("_primary_email"))
    if primary_email:
        email_candidates.add(primary_email)
    emails_list = sorted(email_candidates)
    if not primary_email and emails_list:
        primary_email = emails_list[0]

    payload = {
        "user": user,
        "author_name": user,
        "year": year,
        "month": month,
        "generated_at": generated_at,
        "summary": summary,
        "subsystems": subsystems_list,
        "per_date": per_date,
        "per_hour": per_hour,
    }
    if primary_email:
        payload["author_email"] = primary_email
        summary["author_email"] = primary_email
    if emails_list:
        payload["author_emails"] = emails_list
        payload["emails"] = emails_list
        summary["author_emails"] = emails_list
    return payload


def update_user_yearly_file(
    stats_root: Path,
    user: str,
    year: int,
    month_key: str,
    month_payload: Dict,
    generated_at: str,
) -> None:
    yearly_path = Path(stats_root, "users", safe_user_dirname(user), str(year), "yearly.json")
    base = {
        "user": user,
        "year": year,
        "months": {},
        "updated_at": generated_at,
    }
    if yearly_path.exists():
        try:
            with yearly_path.open(encoding="utf-8") as handle:
                base = json.load(handle)
        except json.JSONDecodeError:
            print(
                f"[WARN] Corrupted yearly stats for user {user} {year} at {yearly_path}, regenerating"
            )
    base.setdefault("months", {})[month_key] = month_payload
    base["updated_at"] = generated_at
    base["author_name"] = month_payload.get("author_name") or base.get("author_name") or user
    month_primary_email = normalize_email_value(month_payload.get("author_email"))
    if month_primary_email:
        base["author_email"] = month_primary_email
    combined_emails: set[str] = set()
    existing_emails = base.get("author_emails")
    if isinstance(existing_emails, list):
        for item in existing_emails:
            normalized = normalize_email_value(item)
            if normalized:
                combined_emails.add(normalized)
    month_email_values = month_payload.get("author_emails") or month_payload.get("emails")
    if isinstance(month_email_values, list):
        for item in month_email_values:
            normalized = normalize_email_value(item)
            if normalized:
                combined_emails.add(normalized)
    if month_primary_email:
        combined_emails.add(month_primary_email)
    if combined_emails:
        base["author_emails"] = sorted(combined_emails)
        base["emails"] = base["author_emails"]
    base["summary"] = build_user_yearly_summary(base["months"])
    write_json_file(yearly_path, base)


def build_user_yearly_summary(months: Dict[str, Dict]) -> Dict:
    totals = {
        "commits": 0,
        "lines_added": 0,
        "lines_removed": 0,
        "lines_net": 0,
        "files_changed": 0,
    }
    subsystems: set[Tuple[str, str]] = set()
    per_hour_totals: Dict[str, Dict[str, int]] = {}
    for payload in months.values():
        summary = payload.get("summary", {})
        totals["commits"] += summary.get("commits", 0)
        totals["lines_added"] += summary.get("lines_added", 0)
        totals["lines_removed"] += summary.get("lines_removed", 0)
        totals["files_changed"] += summary.get("files_changed", 0)
        subsystems.update(
            (sub.get("repo"), sub.get("subsystem")) for sub in payload.get("subsystems", [])
        )
        per_hour_map = payload.get("per_hour", {})
        if isinstance(per_hour_map, dict):
            for hour_key, stats in per_hour_map.items():
                if not isinstance(stats, dict):
                    continue
                label = str(hour_key).strip()
                if not label:
                    continue
                if label.isdigit():
                    label = f"{int(label) % 24:02d}"
                hour_entry = per_hour_totals.setdefault(
                    label,
                    {"commits": 0, "additions": 0, "deletions": 0},
                )
                hour_entry["commits"] += int(stats.get("commits", 0) or 0)
                hour_entry["additions"] += int(stats.get("additions", 0) or 0)
                hour_entry["deletions"] += int(stats.get("deletions", 0) or 0)
    totals["lines_net"] = totals["lines_added"] - totals["lines_removed"]
    base_subsystems = [
        {"repo": repo, "subsystem": subsystem}
        for repo, subsystem in sorted(subsystems)
    ]
    totals["subsystems_touched"] = len(base_subsystems)
    return {
        "totals": totals,
        "subsystems": base_subsystems,
        "months_recorded": sorted(months.keys()),
        "per_hour": normalize_per_hour(per_hour_totals),
    }


def build_subsystem_yearly_summary(months: Dict[str, Dict]) -> Dict:
    totals = {
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "total_changed_lines": 0,
        "lines_net": 0,
        "files_changed": 0,
    }
    contributors: set[str] = set()
    for payload in months.values():
        summary = payload.get("summary", {})
        totals["total_commits"] += summary.get("total_commits", 0)
        totals["total_lines_added"] += summary.get("total_lines_added", 0)
        totals["total_lines_deleted"] += summary.get("total_lines_deleted", 0)
        totals["total_changed_lines"] += summary.get("total_changed_lines", 0)
        totals["files_changed"] += summary.get("files_changed", 0)
        developers = payload.get("developers", {}) or {}
        contributors.update(developers.keys())
    totals["lines_net"] = totals["total_lines_added"] - totals["total_lines_deleted"]
    months_recorded = sorted(months.keys())
    latest_month = months_recorded[-1] if months_recorded else None
    summary_payload = {
        "months_recorded": months_recorded,
        "totals": totals,
        "contributors_count": len(contributors),
    }
    if latest_month:
        summary_payload["latest_month"] = latest_month
    return summary_payload


def prepare_subsystem_month_payload(
    stats_entry: Dict[str, object],
    year: int,
    month: int,
    generated_at: str,
) -> Dict:
    subsystem: Subsystem = stats_entry["subsystem"]  # type: ignore[index]
    summary = stats_entry["summary"].copy()  # type: ignore[index]
    summary["contributors"] = len(stats_entry["contributors"])  # type: ignore[index]
    developers = stats_entry["developers"]  # type: ignore[index]
    repositories = stats_entry["repositories"]  # type: ignore[index]
    per_date = normalize_per_date(stats_entry.get("per_date", {}))  # type: ignore[arg-type]
    return {
        "repo": stats_entry["repo_rel"],
        "subsystem": subsystem.display_name,
        "subsystem_identifier": subsystem.identifier,
        "year": year,
        "month": month,
        "generated_at": generated_at,
        "period": {"from": stats_entry["period_from"], "to": stats_entry["period_to"]},
        "summary": summary,
        "developers": developers,
        "repositories": repositories,
        "per_date": per_date,
        "paths": {
            "include": subsystem.include_paths,
            "exclude": subsystem.exclusion_paths,
            "is_remainder": subsystem.is_remainder,
        },
    }


def update_subsystem_yearly_file(
    stats_root: Path,
    subsystem: Subsystem,
    year: int,
    month_key: str,
    month_payload: Dict,
    generated_at: str,
) -> None:
    yearly_path = subsystem.stats_base_path(stats_root) / "summary" / str(year) / "yearly.json"
    base = {
        "repo": subsystem.repo_rel_path,
        "subsystem": subsystem.display_name,
        "year": year,
        "months": {},
        "updated_at": generated_at,
    }
    if yearly_path.exists():
        try:
            with yearly_path.open(encoding="utf-8") as handle:
                base = json.load(handle)
        except json.JSONDecodeError:
            print(
                f"[WARN] Corrupted subsystem yearly stats for {subsystem.display_name} {year} at {yearly_path}, regenerating"
            )
    base.setdefault("months", {})[month_key] = month_payload
    base["updated_at"] = generated_at
    base["summary"] = build_subsystem_yearly_summary(base["months"])
    write_json_file(yearly_path, base)


def materialize_snapshot(repo: RepoContext, commit: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix=f"snapshot_{repo.repo_name}_"))
    cmd = ["git", "-C", str(repo.abs_path), "archive", commit]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as archive:
            archive.extractall(temp_dir)
    finally:
        proc.wait()
    if proc.returncode != 0:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"git archive failed for {repo.rel_path}")
    return temp_dir


def run_tokei(snapshot_dir: Path, paths: List[str], excludes: Optional[List[str]] = None) -> Optional[Dict]:
    if not paths:
        return None
    cmd = ["tokei", "-o", "json"]
    if excludes:
        for pattern in excludes:
            cmd.extend(["-e", pattern])
    cmd.extend(paths)
    try:
        result = subprocess.run(
            cmd,
            cwd=snapshot_dir,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(f"[WARN] tokei failed in {snapshot_dir}: {exc.stderr.strip() if exc.stderr else exc}")
        return None
    return json.loads(result.stdout)


def format_language_payload(raw: Dict) -> Dict:
    languages = []
    total_metrics = raw.get("Total", {})
    for name, metrics in raw.items():
        if name == "Total":
            continue
        code = int(metrics.get("code", 0))
        comments = int(metrics.get("comments", 0))
        blanks = int(metrics.get("blanks", 0))
        languages.append(
            {
                "name": name,
                "code": code,
                "comments": comments,
                "blanks": blanks,
                "lines": code + comments + blanks,
            }
        )
    languages.sort(key=lambda item: item["code"], reverse=True)
    code_total = int(total_metrics.get("code", 0))
    comments_total = int(total_metrics.get("comments", 0))
    blanks_total = int(total_metrics.get("blanks", 0))
    total = {
        "code": code_total,
        "comments": comments_total,
        "blanks": blanks_total,
        "lines": code_total + comments_total + blanks_total,
    }
    return {"languages": languages, "total": total}


def collect_language_stats(
    repo: RepoContext,
    snapshot_dir: Path,
    subsystem: Subsystem,
) -> Optional[Dict]:
    if subsystem.is_remainder:
        excludes = build_exclude_patterns(snapshot_dir, repo.explicit_subsystems)
        raw = run_tokei(snapshot_dir, ["."], excludes)
    else:
        existing_paths = []
        for path in subsystem.include_paths:
            real_path = snapshot_dir / path
            if real_path.exists():
                existing_paths.append(path or ".")
        if not existing_paths:
            return None
        raw = run_tokei(snapshot_dir, existing_paths)
    if not raw:
        return None
    return format_language_payload(raw)


def build_exclude_patterns(snapshot_dir: Path, subsystems: List[Subsystem]) -> List[str]:
    patterns: List[str] = []
    for subsystem in subsystems:
        for prefix in subsystem.include_paths:
            target = snapshot_dir / prefix
            if target.is_dir():
                patterns.append(f"{prefix}/**")
            else:
                patterns.append(prefix)
    return sorted(set(patterns))


def update_language_yearly_file(
    stats_root: Path,
    subsystem: Subsystem,
    year: int,
    month_key: str,
    month_payload: Dict,
    generated_at: str,
) -> None:
    yearly_path = subsystem.stats_base_path(stats_root) / "languages" / str(year) / "yearly.json"
    base = {
        "repo": subsystem.repo_rel_path,
        "subsystem": subsystem.display_name,
        "year": year,
        "months": {},
        "updated_at": generated_at,
    }
    if yearly_path.exists():
        try:
            with yearly_path.open(encoding="utf-8") as handle:
                base = json.load(handle)
        except json.JSONDecodeError:
            print(
                f"[WARN] Corrupted language yearly stats for {subsystem.display_name} {year} at {yearly_path}, regenerating"
            )
    base.setdefault("months", {})[month_key] = month_payload
    base["updated_at"] = generated_at
    base["summary"] = build_language_yearly_summary(base["months"])
    write_json_file(yearly_path, base)


def build_language_yearly_summary(months: Dict[str, Dict]) -> Dict:
    if not months:
        return {"months_recorded": [], "latest": None}
    latest_month = sorted(months.keys())[-1]
    latest_payload = months[latest_month]
    return {
        "months_recorded": sorted(months.keys()),
        "latest": {
            "month": latest_month,
            "total": latest_payload.get("total", {}),
        },
    }


def ensure_tokei_available() -> None:
    if shutil.which("tokei") is None:
        print("[ERROR] tokei binary not found in PATH. Please install tokei before running this script.")
        sys.exit(1)


def main() -> None:
    args = parse_args()
    ensure_tokei_available()
    services_config = load_json_file(args.config_dir / "services.json")
    alias_index = load_alias_index(args.config_dir / "alias.json")
    ignored_users = load_ignore_users(args.config_dir / "ignore_user.txt")
    discovered = discover_git_repos(args.repos_dir.resolve())
    if not discovered:
        print(f"[ERROR] No git repositories found under {args.repos_dir}.")
        sys.exit(1)
    if args.repos:
        available_keys = set(discovered.keys())
        selected: set[str] = set()
        missing: List[str] = []
        for raw in args.repos:
            match = resolve_requested_repo(raw, available_keys)
            if match:
                selected.add(match)
            else:
                missing.append(raw.strip())
        if selected:
            discovered = {rel: path for rel, path in discovered.items() if rel in selected}
        else:
            discovered = {}
        if missing:
            print(f"[WARN] Requested repos not found: {', '.join(sorted(set(missing)))}")
    repo_contexts = build_repo_contexts(discovered, services_config)
    if not repo_contexts:
        print("[ERROR] No repositories to process after applying filters.")
        sys.exit(1)
    year = args.year
    month = args.month
    month_key = f"{month:02d}"
    month_start = datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=UTC)
    period_from = month_start.strftime("%Y-%m-%d")
    period_to = (month_end - timedelta(days=1)).strftime("%Y-%m-%d")
    snapshot_timestamp = datetime(year, month, 1, 23, 59, 59, tzinfo=UTC)
    snapshot_iso = snapshot_timestamp.isoformat()
    since_iso = month_start.isoformat()
    until_iso = month_end.isoformat()
    generated_at = datetime.now(tz=UTC).isoformat()
    canonicalize = canonicalize_user_builder(alias_index)
    user_monthly_accumulator: Dict[str, Dict] = {}
    subsystem_monthly_accumulator: Dict[Tuple[str, str], Dict[str, object]] = {}

    for repo in repo_contexts:
        print(f"[INFO] Processing {repo.rel_path}")
        subsystem_lookup = {subsystem.identifier: subsystem for subsystem in repo.subsystems()}
        snapshot_commit = resolve_snapshot_commit(repo, snapshot_iso)
        if not snapshot_commit:
            print(f"[WARN] Skipping {repo.rel_path}: no commit before {snapshot_iso}")
        else:
            files = list_files_at_commit(repo, snapshot_commit)
            files_by_subsystem: Dict[str, List[str]] = {s.identifier: [] for s in repo.subsystems()}
            for file_path in files:
                normalized = normalize_service_path(file_path)
                subsystem = repo.classify(normalized)
                files_by_subsystem.setdefault(subsystem.identifier, []).append(file_path)
            repo_blame_state = {
                "developers": Counter(),
                "services": {},
                "total_lines": 0,
            }
            for subsystem in repo.subsystems():
                subsystem_files = files_by_subsystem.get(subsystem.identifier, [])
                if not subsystem_files:
                    continue
                total_lines, owners, owner_counts = collect_blame_for_subsystem(
                    repo, snapshot_commit, subsystem_files, canonicalize, ignored_users
                )
                blame_payload = {
                    "repo": repo.rel_path,
                    "subsystem": subsystem.display_name,
                    "year": year,
                    "month": month,
                    "snapshot_commit": snapshot_commit,
                    "generated_at": generated_at,
                    "total_lines": total_lines,
                    "owners": owners,
                }
                blame_path = subsystem.stats_base_path(args.output_dir) / "blame" / str(year) / f"{month_key}.json"
                write_json_file(blame_path, blame_payload)
                repo_blame_state["total_lines"] += total_lines
                repo_blame_state["developers"].update(owner_counts)
                repo_blame_state["services"][subsystem.display_name] = {
                    "subsystem": subsystem.display_name,
                    "subsystem_identifier": subsystem.identifier,
                    "total_lines": total_lines,
                    "developers": owners,
                }
            if repo_blame_state["total_lines"] > 0:
                repo_payload = {
                    "repo": repo.rel_path,
                    "year": year,
                    "month": month,
                    "snapshot_commit": snapshot_commit,
                    "generated_at": generated_at,
                    "total_lines": repo_blame_state["total_lines"],
                    "developers": build_owner_entries(
                        repo_blame_state["developers"], repo_blame_state["total_lines"]
                    ),
                    "services": repo_blame_state["services"],
                }
                write_repo_blame_files(args.output_dir, repo, year, month_key, repo_payload)
            snapshot_dir: Optional[Path] = None
            try:
                snapshot_dir = materialize_snapshot(repo, snapshot_commit)
                for subsystem in repo.subsystems():
                    lang_payload = collect_language_stats(repo, snapshot_dir, subsystem)
                    if not lang_payload:
                        continue
                    language_record = {
                        "repo": repo.rel_path,
                        "subsystem": subsystem.display_name,
                        "year": year,
                        "month": month,
                        "snapshot_commit": snapshot_commit,
                        "generated_at": generated_at,
                        **lang_payload,
                    }
                    language_path = (
                        subsystem.stats_base_path(args.output_dir)
                        / "languages"
                        / str(year)
                        / f"{month_key}.json"
                    )
                    write_json_file(language_path, language_record)
                    update_language_yearly_file(
                        args.output_dir, subsystem, year, month_key, language_record, generated_at
                    )
            finally:
                if snapshot_dir and snapshot_dir.exists():
                    shutil.rmtree(snapshot_dir, ignore_errors=True)
        month_entries = parse_git_log_month(repo, since_iso, until_iso)
        aggregate_user_stats(
            repo,
            month_entries,
            canonicalize,
            ignored_users,
            user_monthly_accumulator,
            subsystem_monthly_accumulator,
            subsystem_lookup,
            period_from,
            period_to,
        )

        if args.progress_events:
            progress_payload = {
                "repo": repo.rel_path,
                "year": year,
                "month": month,
            }
            print(f"{MASTER_PROGRESS_PREFIX} {json.dumps(progress_payload)}", flush=True)

    for stats_entry in subsystem_monthly_accumulator.values():
        subsystem: Subsystem = stats_entry["subsystem"]  # type: ignore[index]
        month_payload = prepare_subsystem_month_payload(stats_entry, year, month, generated_at)
        summary_path = (
            subsystem.stats_base_path(args.output_dir) / "summary" / str(year) / f"{month_key}.json"
        )
        write_json_file(summary_path, month_payload)
        update_subsystem_yearly_file(
            args.output_dir, subsystem, year, month_key, month_payload, generated_at
        )

    for user, stats in user_monthly_accumulator.items():
        month_payload = prepare_user_month_payload(user, year, month, stats, generated_at)
        month_path = Path(
            args.output_dir,
            "users",
            safe_user_dirname(user),
            str(year),
            f"{month_key}.json",
        )
        write_json_file(month_path, month_payload)
        update_user_yearly_file(args.output_dir, user, year, month_key, month_payload, generated_at)

    print(
        f"[INFO] Completed statistics for {len(repo_contexts)} repos, "
        f"{len(user_monthly_accumulator)} users, and {len(subsystem_monthly_accumulator)} subsystems"
    )


if __name__ == "__main__":
    main()
