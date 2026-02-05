import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

__all__ = [
    "compute_dead_subsystems",
    "compute_subsystem_top_maintainers",
    "compute_subsystem_maintainer_timeline",
    "compute_subsystem_significant_ownership",
    "compute_subsystem_size_rankings",
]


def _safe_load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _iter_summary_directories(subsystem_path: str) -> List[Tuple[datetime, datetime, str, int, str]]:
    entries: List[Tuple[datetime, datetime, str, int, str]] = []
    if not os.path.isdir(subsystem_path):
        return entries

    for entry in os.listdir(subsystem_path):
        entry_path = os.path.join(subsystem_path, entry)
        if not os.path.isdir(entry_path):
            continue
        if "_" not in entry:
            continue
        try:
            date_from_str, date_to_str = entry.split("_", 1)
            date_from = datetime.strptime(date_from_str, "%Y-%m-%d")
            date_to = datetime.strptime(date_to_str, "%Y-%m-%d")
        except (ValueError, IndexError):
            continue

        summary_file = os.path.join(entry_path, "summary.json")
        if not os.path.isfile(summary_file):
            continue

        day_span = (date_to - date_from).days
        entries.append((date_from, date_to, entry, day_span, summary_file))
    return entries


def compute_dead_subsystems(stats_root: str, threshold_months: int = 3) -> Dict[str, Dict[str, Any]]:
    subsystems_root = os.path.join(stats_root, "subsystems")
    if not os.path.isdir(subsystems_root):
        return {}

    current_date = datetime.utcnow()
    threshold_date = current_date - timedelta(days=30 * threshold_months)
    subsystem_status: Dict[str, Dict[str, Any]] = {}

    for subsystem_name in os.listdir(subsystems_root):
        subsystem_dir = os.path.join(subsystems_root, subsystem_name)
        if not os.path.isdir(subsystem_dir):
            continue

        latest_activity: Optional[datetime] = None
        for date_from, date_to, _folder, day_span, summary_file in _iter_summary_directories(subsystem_dir):
            # Skip yearly/long periods (> ~35 days)
            if day_span > 35:
                continue

            summary_data = _safe_load_json(summary_file)
            if not summary_data:
                continue

            total_commits = summary_data.get("total_commits", 0)
            if total_commits and total_commits > 0:
                if latest_activity is None or date_to > latest_activity:
                    latest_activity = date_to

        if latest_activity is None:
            subsystem_status[subsystem_name] = {
                "is_dead": True,
                "last_activity_date": None,
                "months_since_activity": None,
            }
            continue

        months_since = int((current_date - latest_activity).days / 30.44)
        subsystem_status[subsystem_name] = {
            "is_dead": latest_activity < threshold_date,
            "last_activity_date": latest_activity.strftime("%Y-%m-%d"),
            "months_since_activity": months_since,
        }

    return subsystem_status


def compute_subsystem_top_maintainers(
    stats_root: str,
    subsystem_name: str,
    lookback_days: int = 90,
) -> Dict[str, Any]:
    subsystem_path = os.path.join(stats_root, "subsystems", subsystem_name)
    if not os.path.isdir(subsystem_path):
        return {"maintainers": [], "lookback_days": lookback_days}

    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    maintainer_data: Dict[str, Dict[str, Any]] = {}

    for date_from, _date_to, _folder, day_span, summary_file in _iter_summary_directories(subsystem_path):
        if day_span > 35 or date_from < cutoff:
            continue

        summary_data = _safe_load_json(summary_file)
        if not summary_data:
            continue

        repositories = summary_data.get("repositories", {})
        if not isinstance(repositories, dict):
            continue

        for repo_data in repositories.values():
            developers = repo_data.get("developers", {}) if isinstance(repo_data, dict) else {}
            if not isinstance(developers, dict):
                continue

            for dev_slug, dev_data in developers.items():
                if not isinstance(dev_data, dict):
                    continue
                commits = dev_data.get("commits", 0)
                if commits <= 0:
                    continue
                entry = maintainer_data.setdefault(
                    dev_slug,
                    {
                        "slug": dev_slug,
                        "display_name": dev_data.get("display_name", dev_slug),
                        "commits": 0,
                        "lines_added": 0,
                        "lines_deleted": 0,
                        "changed_lines": 0,
                    },
                )
                entry["commits"] += commits
                entry["lines_added"] += dev_data.get("lines_added", 0)
                entry["lines_deleted"] += dev_data.get("lines_deleted", 0)
                entry["changed_lines"] += dev_data.get("changed_lines", 0)

    top_maintainers = sorted(
        maintainer_data.values(),
        key=lambda item: item["commits"],
        reverse=True,
    )[:5]

    return {
        "maintainers": top_maintainers,
        "lookback_days": lookback_days,
    }


def _extract_developer_lines(developers: Dict[str, Any]) -> Dict[str, int]:
    ownership: Dict[str, int] = {}
    for slug, payload in developers.items():
        if isinstance(payload, dict):
            ownership[slug] = int(payload.get("lines", 0))
        else:
            try:
                ownership[slug] = int(payload)
            except Exception:
                ownership[slug] = 0
    return ownership


def _load_current_ownership(stats_root: str, subsystem_name: str) -> Tuple[Dict[str, int], int]:
    repos_path = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_path):
        return {}, 0

    target_lower = subsystem_name.lower()

    for org_name in os.listdir(repos_path):
        org_path = os.path.join(repos_path, org_name)
        if not os.path.isdir(org_path):
            continue

        for repo_name in os.listdir(org_path):
            repo_path = os.path.join(org_path, repo_name)
            blame_file = os.path.join(repo_path, "blame", "blame.json")
            if not os.path.isfile(blame_file):
                continue

            blame_data = _safe_load_json(blame_file)
            if not blame_data:
                continue

            repo_full = blame_data.get("repo") or f"{org_name}/{repo_name}"
            repo_field = str(repo_full or "")

            if target_lower and target_lower in repo_field.lower():
                developers = blame_data.get("developers", {})
                if isinstance(developers, dict):
                    return _extract_developer_lines(developers), int(blame_data.get("total_lines", 0) or 0)

            services = blame_data.get("services", {})
            if isinstance(services, dict):
                service_data = services.get(subsystem_name)
                if isinstance(service_data, dict):
                    developers = service_data.get("developers", {})
                    if isinstance(developers, dict):
                        return _extract_developer_lines(developers), int(service_data.get("total_lines", 0) or 0)

    return {}, 0


def compute_subsystem_maintainer_timeline(stats_root: str, subsystem_name: str) -> Dict[str, Any]:
    subsystem_path = os.path.join(stats_root, "subsystems", subsystem_name)
    if not os.path.isdir(subsystem_path):
        return {"timeline": {}}

    current_ownership, total_current_lines = _load_current_ownership(stats_root, subsystem_name)
    if not current_ownership or total_current_lines <= 0:
        return {"timeline": {}}

    monthly_net_changes: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    month_labels: List[str] = []

    for date_from, _date_to, _folder, day_span, summary_file in _iter_summary_directories(subsystem_path):
        if day_span > 35:
            continue
        month_label = date_from.strftime("%Y-%m")
        if month_label not in month_labels:
            month_labels.append(month_label)

        summary_data = _safe_load_json(summary_file)
        if not summary_data:
            continue

        repositories = summary_data.get("repositories", {})
        if not isinstance(repositories, dict):
            continue
        for repo_data in repositories.values():
            developers = repo_data.get("developers", {}) if isinstance(repo_data, dict) else {}
            if not isinstance(developers, dict):
                continue
            for dev_slug, dev_data in developers.items():
                if not isinstance(dev_data, dict):
                    continue
                net_lines = dev_data.get("lines_added", 0) - dev_data.get("lines_deleted", 0)
                if net_lines:
                    monthly_net_changes[dev_slug][month_label] += net_lines

    if not month_labels:
        return {"timeline": {}}

    month_labels = sorted(set(month_labels))

    recent_activity = defaultdict(int)
    recent_cutoff = datetime.utcnow() - timedelta(days=90)

    for date_from, _date_to, _folder, day_span, summary_file in _iter_summary_directories(subsystem_path):
        if day_span > 35 or date_from < recent_cutoff:
            continue
        summary_data = _safe_load_json(summary_file)
        if not summary_data:
            continue
        repositories = summary_data.get("repositories", {})
        if not isinstance(repositories, dict):
            continue
        for repo_data in repositories.values():
            developers = repo_data.get("developers", {}) if isinstance(repo_data, dict) else {}
            if not isinstance(developers, dict):
                continue
            for dev_slug, dev_data in developers.items():
                if not isinstance(dev_data, dict):
                    continue
                recent_activity[dev_slug] += dev_data.get("commits", 0)

    if not recent_activity:
        for slug, lines in current_ownership.items():
            recent_activity[slug] = lines

    top_slugs = [slug for slug, _ in sorted(recent_activity.items(), key=lambda item: item[1], reverse=True)[:5]]
    if not top_slugs:
        return {"timeline": {}}

    month_totals = {
        month: sum(changes.get(month, 0) for changes in monthly_net_changes.values())
        for month in month_labels
    }

    timeline: Dict[str, Dict[str, Any]] = {}
    for slug in top_slugs:
        dev_lines = current_ownership.get(slug, 0)
        total_lines = total_current_lines
        ownership_points: List[float] = []

        for month in reversed(month_labels):
            percentage = (dev_lines / total_lines * 100) if total_lines > 0 else 0
            ownership_points.insert(0, round(percentage, 1))
            dev_lines -= monthly_net_changes[slug].get(month, 0)
            total_lines -= month_totals.get(month, 0)
            dev_lines = max(0, dev_lines)
            total_lines = max(1, total_lines)

        timeline[slug] = {
            "months": month_labels,
            "ownership": ownership_points,
        }

    return {"timeline": timeline}


def _extract_significant_owners(
    developers: Dict[str, Any],
    total_lines: int,
    source_label: str,
    threshold: float,
) -> List[Dict[str, Any]]:
    if total_lines <= 0:
        return []

    owners: List[Dict[str, Any]] = []
    for slug, payload in developers.items():
        if isinstance(payload, dict):
            lines = payload.get("lines", 0)
            display_name = payload.get("display_name", slug)
        else:
            lines = payload
            display_name = slug
        try:
            lines = int(lines)
        except Exception:
            lines = 0
        if lines <= 0:
            continue
        share = (lines / total_lines) if total_lines else 0
        owners.append(
            {
                "slug": slug,
                "display_name": display_name,
                "lines": lines,
                "share": share,
                "percentage": round(share * 100, 1),
                "source": source_label,
                "total_lines": total_lines,
            }
        )
    return owners


def compute_subsystem_significant_ownership(
    stats_root: str,
    subsystem_name: str,
    ownership_threshold: float = 0.10,
) -> Dict[str, Any]:
    repos_path = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_path):
        return {"owners": [], "threshold": ownership_threshold}

    owners: List[Dict[str, Any]] = []

    def maybe_process_blame(blame_file: str, repo_label: str, simple_name: str) -> None:
        blame_data = _safe_load_json(blame_file)
        if not blame_data:
            return
        repo_full_name = str(blame_data.get("repo") or repo_label)
        repo_simple_name = repo_full_name.split("/")[-1] if repo_full_name else simple_name
        repo_matches = simple_name == subsystem_name or repo_full_name == subsystem_name

        if repo_matches:
            developers = blame_data.get("developers", {})
            if isinstance(developers, dict):
                owners.extend(
                    _extract_significant_owners(
                        developers,
                        int(blame_data.get("total_lines", 0) or 0),
                        f"repo-{repo_simple_name}",
                        ownership_threshold,
                    )
                )

        services = blame_data.get("services", {})
        if isinstance(services, dict):
            service_data = services.get(subsystem_name)
            if isinstance(service_data, dict):
                developers = service_data.get("developers", {})
                if isinstance(developers, dict):
                    owners.extend(
                        _extract_significant_owners(
                            developers,
                            int(service_data.get("total_lines", 0) or 0),
                            f"service-{subsystem_name}-in-{repo_simple_name}",
                            ownership_threshold,
                        )
                    )

    for org_name in os.listdir(repos_path):
        org_path = os.path.join(repos_path, org_name)
        if not os.path.isdir(org_path):
            continue

        direct_blame = os.path.join(org_path, "blame", "blame.json")
        if os.path.isfile(direct_blame):
            maybe_process_blame(direct_blame, org_name, org_name)
            continue

        for repo_name in os.listdir(org_path):
            repo_path = os.path.join(org_path, repo_name)
            if not os.path.isdir(repo_path):
                continue
            blame_file = os.path.join(repo_path, "blame", "blame.json")
            if not os.path.isfile(blame_file):
                continue
            maybe_process_blame(blame_file, f"{org_name}/{repo_name}", repo_name)

    deduped: Dict[str, Dict[str, Any]] = {}
    for owner in owners:
        slug = owner["slug"]
        existing = deduped.get(slug)
        if not existing:
            deduped[slug] = owner
            continue
        if owner.get("share", 0) > existing.get("share", 0):
            deduped[slug] = owner
            continue
        if owner.get("share", 0) == existing.get("share", 0) and owner.get("total_lines", 0) > existing.get("total_lines", 0):
            deduped[slug] = owner

    filtered = [owner for owner in deduped.values() if owner.get("share", 0) > ownership_threshold]
    sorted_owners = sorted(filtered, key=lambda item: item.get("share", 0), reverse=True)
    return {"owners": sorted_owners, "threshold": ownership_threshold}


def compute_subsystem_size_rankings(stats_root: str) -> Dict[str, Any]:
    subsystems_root = os.path.join(stats_root, "subsystems")
    result: Dict[str, Any] = {
        "rankings": {},
        "buckets": {"big": [], "medium": [], "small": []},
        "total_subsystems": 0,
        "total_system_lines": 0,
        "total_git_lines": 0,
    }

    if not os.path.isdir(subsystems_root):
        return result

    repos_path = os.path.join(stats_root, "repos")
    counted_repos = set()
    if os.path.isdir(repos_path):
        for org_name in os.listdir(repos_path):
            org_path = os.path.join(repos_path, org_name)
            if not os.path.isdir(org_path):
                continue
            for repo_name in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_name)
                blame_file = os.path.join(repo_path, "blame", "blame.json")
                if not os.path.isfile(blame_file):
                    continue
                blame_data = _safe_load_json(blame_file)
                if not blame_data:
                    continue
                repo_full_name = blame_data.get("repo", "")
                repo_simple_name = str(repo_full_name).split("/")[-1] if repo_full_name else repo_name
                if repo_simple_name in counted_repos:
                    continue
                counted_repos.add(repo_simple_name)
                result["total_git_lines"] += int(blame_data.get("total_lines", 0) or 0)

    subsystem_sizes: List[Dict[str, Any]] = []
    for subsystem_name in os.listdir(subsystems_root):
        subsystem_dir = os.path.join(subsystems_root, subsystem_name)
        if not os.path.isdir(subsystem_dir):
            continue
        languages_file = os.path.join(subsystem_dir, "languages.json")
        totals = 0
        if os.path.isfile(languages_file):
            language_data = _safe_load_json(languages_file)
            if language_data:
                totals = int(language_data.get("totals", {}).get("code_lines", 0) or 0)
        if totals > 0:
            subsystem_sizes.append({"name": subsystem_name, "total_lines": totals})

    subsystem_sizes.sort(key=lambda item: item["total_lines"], reverse=True)
    result["total_subsystems"] = len(subsystem_sizes)
    result["total_system_lines"] = sum(item["total_lines"] for item in subsystem_sizes)

    rankings: Dict[str, Dict[str, Any]] = {}
    for idx, subsystem in enumerate(subsystem_sizes, start=1):
        rankings[subsystem["name"]] = {
            "rank": idx,
            "total_lines": subsystem["total_lines"],
            "total_subsystems": len(subsystem_sizes),
        }

    total_count = len(subsystem_sizes)
    if total_count:
        bucket_size = total_count // 3
        remainder = total_count % 3
        big_size = bucket_size + (1 if remainder >= 1 else 0)
        medium_size = bucket_size + (1 if remainder >= 2 else 0)
        small_size = total_count - big_size - medium_size

        result["buckets"] = {
            "big": [s["name"] for s in subsystem_sizes[:big_size]],
            "medium": [s["name"] for s in subsystem_sizes[big_size: big_size + medium_size]],
            "small": [s["name"] for s in subsystem_sizes[big_size + medium_size:]],
        }

        for name in result["buckets"]["big"]:
            rankings[name]["size_bucket"] = "big"
        for name in result["buckets"]["medium"]:
            rankings[name]["size_bucket"] = "medium"
        for name in result["buckets"]["small"]:
            rankings[name]["size_bucket"] = "small"

    result["rankings"] = rankings
    return result


def _build_manifest_entry(repo_rel: str, base_dir: str, display_name: Optional[str]) -> Optional[Dict[str, Any]]:
    summary_dir = os.path.join(base_dir, "summary")
    languages_dir = os.path.join(base_dir, "languages")
    blame_dir = os.path.join(base_dir, "blame")
    if not any(os.path.isdir(path) for path in (summary_dir, languages_dir, blame_dir)):
        return None
    entry = {
        "display_name": display_name or repo_rel.split("/")[-1],
        "repo_rel": repo_rel,
        "summary_dir": summary_dir,
        "languages_dir": languages_dir,
        "blame_dir": blame_dir,
        "subsystem_dir": base_dir,
    }
    return entry


def _collect_repo_subsystem_entries(stats_root: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    repos_root = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_root):
        return entries

    reserved = {"summary", "languages", "blame", "logs", "__pycache__"}
    for owner in sorted(os.listdir(repos_root)):
        owner_dir = os.path.join(repos_root, owner)
        if not os.path.isdir(owner_dir):
            continue
        for repo in sorted(os.listdir(owner_dir)):
            repo_dir = os.path.join(owner_dir, repo)
            if not os.path.isdir(repo_dir):
                continue
            repo_rel = f"{owner}/{repo}"
            base_entry = _build_manifest_entry(repo_rel, repo_dir, repo)
            if base_entry:
                entries.append(base_entry)
            for child in sorted(os.listdir(repo_dir)):
                if child.startswith(".") or child in reserved:
                    continue
                child_dir = os.path.join(repo_dir, child)
                if not os.path.isdir(child_dir):
                    continue
                child_entry = _build_manifest_entry(repo_rel, child_dir, child)
                if child_entry:
                    entries.append(child_entry)
    return entries


def _get_subsystem_entries(stats_root: str) -> Dict[str, Any]:
    stats_root = stats_root or ""
    entries = _collect_repo_subsystem_entries(stats_root)
    manifest: Dict[str, Any] = {
        "entries": entries,
        "by_name": {},
        "by_repo": {},
    }
    for entry in entries:
        name = entry.get("display_name")
        repo_rel = entry.get("repo_rel")
        if name:
            bucket = manifest["by_name"].setdefault(name, [])
            bucket.append(entry)
            lowered = name.lower()
            if lowered != name:
                manifest["by_name"].setdefault(lowered, []).append(entry)
        if repo_rel:
            manifest["by_repo"].setdefault(repo_rel, []).append(entry)
    return manifest
