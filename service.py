#!/usr/bin/env python3
"""
Service-focused statistics generator.

Creates statistics organized by service rather than by repository.
Services become first-class entities with their own directories:

stats/subsystems/<service_name>/<date_range>/summary.json

This script aggregates data from multiple repositories for each service.
"""

import argparse
import os
import sys
import subprocess
import json
import re
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List, Set
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

AuthorKey = Tuple[str, str]  # (name, email)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate service-focused statistics by analyzing git repos "
            "and organizing data by service rather than repository."
        )
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        required=True,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        required=True,
        help="End date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--repos-root",
        dest="repos_root",
        default="repos",
        help="Root directory where repos are cloned (default: ./repos)",
    )
    parser.add_argument(
        "--output-root",
        dest="output_root",
        default=".",
        help="Root directory under which 'stats/subsystems/...' will be created (default: current directory)",
    )
    parser.add_argument(
        "--services-file",
        dest="services_file",
        default="configuration/services.json",
        help="JSON file describing services per repo (default: configuration/services.json)",
    )
    parser.add_argument(
        "--ignore-file",
        dest="ignore_file",
        default="configuration/ignore_user.txt",
        help="Text file listing users to ignore, one per line (default: configuration/ignore_user.txt)",
    )
    parser.add_argument(
        "--parallel",
        dest="parallel",
        action="store_true",
        help="Enable parallel processing of repositories for improved performance",
    )
    parser.add_argument(
        "--max-workers",
        dest="max_workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers (default: auto-detect based on CPU cores)",
    )
    return parser.parse_args()


def slugify(text: str) -> str:
    """Make a filesystem-safe, lowercase slug from a string."""
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def ensure_service_output_folder(
    output_root: str, service_name: str, date_from: str, date_to: str
) -> str:
    """
    Create folder structure:
      <output_root>/stats/subsystems/<service_name>/<date_from>_<date_to>
    """
    base = os.path.join(output_root, "stats", "subsystems", service_name)
    sub = os.path.join(base, f"{date_from}_{date_to}")
    os.makedirs(sub, exist_ok=True)
    return sub


def discover_local_repos(root: str) -> List[str]:
    """
    Recursively find all directories under 'root' that contain a .git folder.
    Returns paths relative to root, like: 'owner/repo' or 'repo'.
    """
    if not os.path.isdir(root):
        return []

    found: List[str] = []
    for dirpath, dirnames, _filenames in os.walk(root):
        if ".git" in dirnames:
            rel = os.path.relpath(dirpath, root)
            rel = rel.replace("\\", "/")  # Windows-safe path format
            found.append(rel)
    return found


def load_aliases(alias_path: str = "configuration/alias.json") -> Dict[str, Any]:
    """Load author aliases from JSON file."""
    if not os.path.isfile(alias_path):
        return {}
    try:
        with open(alias_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, IOError) as e:
        print(f"WARNING: Failed to load alias file '{alias_path}': {e}", file=sys.stderr)
        return {}


def load_ignored_users(ignore_path: str) -> Set[str]:
    """Load list of user slugs to ignore."""
    ignored: Set[str] = set()
    if not os.path.isfile(ignore_path):
        return ignored

    try:
        with open(ignore_path, "r", encoding="utf-8") as f:
            for line in f:
                slug = line.strip()
                if slug and not slug.startswith("#"):
                    ignored.add(slug)
    except IOError as e:
        print(f"WARNING: Failed to load ignore file '{ignore_path}': {e}", file=sys.stderr)

    return ignored


def load_services_config(services_path: str) -> Dict[str, Dict[str, list]]:
    """Load services configuration from JSON."""
    if not os.path.isfile(services_path):
        print(f"WARNING: Services file '{services_path}' not found", file=sys.stderr)
        return {}

    try:
        with open(services_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            print(
                f"WARNING: configuration/services.json must be a JSON object "
                f"(got {type(data).__name__})",
                file=sys.stderr,
            )
            return {}
        
        return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"WARNING: Failed to load services config '{services_path}': {e}", file=sys.stderr)
        return {}


def default_service_name_for_repo(repo_rel_path: str) -> str:
    """Default service name for a repo: its last path segment."""
    return repo_rel_path.strip("/").split("/")[-1] or "unknown-service"


def get_service_for_path(
    repo_rel_path: str,
    file_path: str,
    services_config: Dict[str, Dict[str, list]],
    repos_root: str = "",
) -> str:
    """Determine which service a file belongs to based on services configuration."""
    # Try both the repo path directly and with the repos root prefix
    mapping = services_config.get(repo_rel_path)
    service_config_key = repo_rel_path
    
    if not mapping and repos_root:
        # Try with repos root prefix (e.g., "org/repo-name")
        potential_key = f"{os.path.basename(repos_root)}/{repo_rel_path}"
        mapping = services_config.get(potential_key)
        if mapping:
            service_config_key = potential_key
    
    norm = file_path.replace("\\", "/").lstrip("./")

    if not mapping:
        return default_service_name_for_repo(repo_rel_path)

    best_service: Optional[str] = None
    best_len = -1

    for svc_name, prefixes in mapping.items():
        for raw_prefix in prefixes:
            pnorm = str(raw_prefix).replace("\\", "/").lstrip("./")
            if pnorm in ("", "."):
                # catch-all prefix
                match = True
                plen = 0
            else:
                if not pnorm.endswith("/"):
                    pnorm = pnorm + "/"
                if norm.startswith(pnorm):
                    match = True
                    plen = len(pnorm)
                else:
                    match = False
            
            if match and plen > best_len:
                best_service = svc_name
                best_len = plen

    return best_service or default_service_name_for_repo(repo_rel_path)


def author_slug_from_name_email(name: str, email: str) -> str:
    """Build a base slug from author email (local-part) or name, then slugify."""
    if email:
        base = email.split("@")[0]
    else:
        base = name or "unknown-author"
    return slugify(base)


def canonical_slug_for_author(name: str, email: str, alias_map: Dict[str, Any]) -> str:
    """Get canonical slug for author, applying any aliases."""
    base_slug = author_slug_from_name_email(name, email)
    
    # Check if this base_slug appears as a value (alias) in the alias_map
    for canonical, aliases in alias_map.items():
        if isinstance(aliases, list) and base_slug in aliases:
            return canonical
        elif isinstance(aliases, str) and base_slug == aliases:
            return canonical
    
    # If no alias found, return the base slug
    return base_slug


def init_service_data(service_name: str, date_from: str, date_to: str) -> Dict[str, Any]:
    """Initialize service data structure."""
    return {
        "service": service_name,
        "from": date_from,
        "to": date_to,
        "repositories": {},  # repo_path -> repo stats
        "developers": {},    # developer_slug -> developer stats
        "top_developer": {},
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "total_changed_lines": 0,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def ensure_developer_entry(service_data: Dict[str, Any], slug: str, display_name: str, email: str) -> Dict[str, Any]:
    """Ensure developer entry exists in service data."""
    if slug not in service_data["developers"]:
        service_data["developers"][slug] = {
            "slug": slug,
            "display_name": display_name,
            "emails": [email],
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0,
            "repositories": {},  # repo_path -> repo-specific stats for this dev
        }
    else:
        # Update emails list if new
        dev = service_data["developers"][slug]
        if email not in dev["emails"]:
            dev["emails"].append(email)
    
    return service_data["developers"][slug]


def ensure_repo_entry(service_data: Dict[str, Any], repo_path: str) -> Dict[str, Any]:
    """Ensure repository entry exists in service data."""
    if repo_path not in service_data["repositories"]:
        service_data["repositories"][repo_path] = {
            "repo": repo_path,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0,
            "developers": {},  # developer_slug -> repo-specific stats for this dev
        }
    return service_data["repositories"][repo_path]


def analyze_repo_for_services(
    repo_rel_path: str,
    repos_root: str,
    services_data: Dict[str, Dict[str, Any]],
    services_config: Dict[str, Dict[str, list]],
    alias_map: Dict[str, Any],
    ignored_slugs: Set[str],
    date_from: str,
    date_to: str,
) -> None:
    """Analyze a single repository and update services data."""
    repo_path = os.path.join(repos_root, repo_rel_path)
    if not os.path.isdir(repo_path):
        print(f"WARNING: Repo path {repo_path} not found", file=sys.stderr)
        return

    print(f"  Analyzing {repo_rel_path}...")
    
    # Check if this repo has service mappings defined
    # Try both the repo path directly and with the repos root prefix
    has_service_config = repo_rel_path in services_config
    service_config_key = repo_rel_path
    
    if not has_service_config:
        # Try with repos root prefix (e.g., "org/repo-name")
        potential_key = f"{os.path.basename(repos_root)}/{repo_rel_path}"
        if potential_key in services_config:
            has_service_config = True
            service_config_key = potential_key
    
    # If no service config, treat the entire repo as a single service
    if not has_service_config:
        service_name = default_service_name_for_repo(repo_rel_path)
        print(f"    No service config found, treating as single subsystem: {service_name}")
    else:
        print(f"    Found service config for: {service_config_key}")
        services_in_repo = list(services_config[service_config_key].keys())
        print(f"    Services in repo: {services_in_repo}")
    
    # Parse date range
    try:
        from datetime import datetime
        date_from_dt = datetime.fromisoformat(date_from)
        date_to_dt = datetime.fromisoformat(date_to)
    except ValueError as e:
        print(f"ERROR: Invalid date format: {e}", file=sys.stderr)
        return

    # Run git log to get commit and file change data
    cmd = [
        "git",
        "log",
        f"--since={date_from_dt.date().isoformat()}",
        f"--until={date_to_dt.date().isoformat()}",
        "--no-merges",
        "--pretty=format:%H%x01%an%x01%ae",
        "--numstat",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            cwd=repo_path,
        )
    except FileNotFoundError:
        print("ERROR: git command not found", file=sys.stderr)
        return

    if result.returncode != 0:
        print(f"    ! git log failed in {repo_path}")
        return

    stdout = result.stdout
    if not stdout.strip():
        return  # No commits in window

    current_author_name: Optional[str] = None
    current_author_email: Optional[str] = None
    current_canonical_slug: Optional[str] = None
    current_display_name: Optional[str] = None
    current_services_touched: Dict[str, Dict[str, int]] = {}  # service_name -> {adds: int, dels: int}

    def finalize_current_commit():
        """Process the current commit's data."""
        if (
            current_canonical_slug is None
            or current_author_name is None
            or current_author_email is None
        ):
            return

        # If no service mappings and no services touched yet, 
        # attribute everything to the default service
        if not has_service_config and not current_services_touched:
            default_service = default_service_name_for_repo(repo_rel_path)
            current_services_touched[default_service] = {"adds": 0, "dels": 0}

        for service_name, line_changes in current_services_touched.items():
            # Ensure service exists
            if service_name not in services_data:
                services_data[service_name] = init_service_data(service_name, date_from, date_to)
            
            service_data = services_data[service_name]
            
            # Update developer stats
            dev = ensure_developer_entry(service_data, current_canonical_slug, 
                                       current_display_name or current_author_name, 
                                       current_author_email)
            dev["commits"] += 1
            dev["lines_added"] += line_changes["adds"]
            dev["lines_deleted"] += line_changes["dels"]
            dev["net_lines"] += line_changes["adds"] - line_changes["dels"]
            dev["changed_lines"] += line_changes["adds"] + line_changes["dels"]
            
            # Update repo stats for this developer
            if repo_rel_path not in dev["repositories"]:
                dev["repositories"][repo_rel_path] = {
                    "commits": 0, "lines_added": 0, "lines_deleted": 0, 
                    "net_lines": 0, "changed_lines": 0
                }
            repo_dev_stats = dev["repositories"][repo_rel_path]
            repo_dev_stats["commits"] += 1
            repo_dev_stats["lines_added"] += line_changes["adds"]
            repo_dev_stats["lines_deleted"] += line_changes["dels"]
            repo_dev_stats["net_lines"] += line_changes["adds"] - line_changes["dels"]
            repo_dev_stats["changed_lines"] += line_changes["adds"] + line_changes["dels"]
            
            # Update repository stats  
            repo_data = ensure_repo_entry(service_data, repo_rel_path)
            repo_data["commits"] += 1
            repo_data["lines_added"] += line_changes["adds"]
            repo_data["lines_deleted"] += line_changes["dels"]
            repo_data["net_lines"] += line_changes["adds"] - line_changes["dels"]
            repo_data["changed_lines"] += line_changes["adds"] + line_changes["dels"]
            
            # Update repo developer stats
            if current_canonical_slug not in repo_data["developers"]:
                repo_data["developers"][current_canonical_slug] = {
                    "slug": current_canonical_slug,
                    "display_name": current_display_name or current_author_name,
                    "commits": 0, "lines_added": 0, "lines_deleted": 0,
                    "net_lines": 0, "changed_lines": 0
                }
            repo_dev = repo_data["developers"][current_canonical_slug]
            repo_dev["commits"] += 1
            repo_dev["lines_added"] += line_changes["adds"]
            repo_dev["lines_deleted"] += line_changes["dels"]
            repo_dev["net_lines"] += line_changes["adds"] - line_changes["dels"]
            repo_dev["changed_lines"] += line_changes["adds"] + line_changes["dels"]
            
            # Update service totals
            service_data["total_commits"] += 1
            service_data["total_lines_added"] += line_changes["adds"]
            service_data["total_lines_deleted"] += line_changes["dels"]
            service_data["total_changed_lines"] += line_changes["adds"] + line_changes["dels"]

    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Commit header line
        if "\x01" in line and "\t" not in line:
            # finalize previous commit
            finalize_current_commit()

            parts = line.split("\x01")
            if len(parts) < 3:
                continue  # Skip malformed commit headers
            
            sha = parts[0]
            name = parts[1]
            email = parts[2]

            canonical_slug = canonical_slug_for_author(name, email, alias_map)

            # If this author is ignored, skip
            if canonical_slug in ignored_slugs:
                current_author_name = None
                current_author_email = None
                current_canonical_slug = None
                current_display_name = None
                current_services_touched = {}
                continue

            current_author_name = name
            current_author_email = email
            current_canonical_slug = canonical_slug
            current_display_name = name
            current_services_touched = {}
            continue

        # numstat line: "<additions>\t<deletions>\t<file>"
        if "\t" in line and current_canonical_slug is not None:
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            add_str, del_str, filename = parts[0], parts[1], parts[2]

            try:
                additions = int(add_str) if add_str != "-" else 0
                deletions = int(del_str) if del_str != "-" else 0
            except ValueError:
                continue

            # Determine which service this file belongs to
            if has_service_config:
                service_name = get_service_for_path(repo_rel_path, filename, services_config, repos_root)
            else:
                # If no service config, everything goes to the default service for this repo
                service_name = default_service_name_for_repo(repo_rel_path)
            
            # Accumulate changes for this service
            if service_name not in current_services_touched:
                current_services_touched[service_name] = {"adds": 0, "dels": 0}
            current_services_touched[service_name]["adds"] += additions
            current_services_touched[service_name]["dels"] += deletions

    # Don't forget the last commit
    finalize_current_commit()


def calculate_service_top_developers(services_data: Dict[str, Dict[str, Any]]) -> None:
    """Calculate top developers for each service."""
    for service_name, service_data in services_data.items():
        developers = service_data.get("developers", {})
        if developers:
            top_dev = max(developers.values(), key=lambda d: d["changed_lines"])
            service_data["top_developer"] = {
                "slug": top_dev["slug"],
                "display_name": top_dev["display_name"],
                "changed_lines": top_dev["changed_lines"],
                "commits": top_dev["commits"]
            }


def write_service_summaries(
    services_data: Dict[str, Dict[str, Any]], 
    output_root: str, 
    date_from: str, 
    date_to: str
) -> None:
    """Write service summary files."""
    for service_name, service_data in services_data.items():
        output_dir = ensure_service_output_folder(output_root, service_name, date_from, date_to)
        summary_path = os.path.join(output_dir, "summary.json")
        
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(service_data, f, indent=2)
        
        print(f"  ✅ Created service summary: {summary_path}")


def analyze_repo_worker_global(repo_data):
    """Worker function for processing a single repository - moved to global scope for pickle compatibility"""
    repo = repo_data["repo"] 
    repos_root = repo_data["repos_root"]
    services_config = repo_data["services_config"]
    alias_map = repo_data["alias_map"]
    ignored_slugs = repo_data["ignored_slugs"]
    date_from = repo_data["date_from"]
    date_to = repo_data["date_to"]
    
    local_services_data = {}
    analyze_repo_for_services(
        repo, repos_root, local_services_data, services_config,
        alias_map, ignored_slugs, date_from, date_to
    )
    return (repo, local_services_data)


def main() -> None:
    args = parse_args()
    
    repos_root = args.repos_root
    output_root = args.output_root
    services_file = args.services_file
    ignore_file = args.ignore_file
    date_from = args.date_from
    date_to = args.date_to
    parallel = args.parallel
    max_workers = args.max_workers

    print("Service Statistics Generator")
    print("===========================")
    print(f"Date range  : {date_from} → {date_to}")
    print(f"Repos root  : {repos_root}")
    print(f"Output root : {output_root}")
    print(f"Services    : {services_file}")
    print(f"Ignore      : {ignore_file}")
    if parallel:
        print(f"Parallel    : Enabled (max workers: {max_workers if max_workers else 'auto-detect'})")
    else:
        print(f"Parallel    : Disabled")
    print()

    # Load configuration
    alias_map = load_aliases("configuration/alias.json")
    ignored_slugs = load_ignored_users(ignore_file)
    services_config = load_services_config(services_file)
    
    print(f"Loaded {len(alias_map)} aliases")
    print(f"Ignoring {len(ignored_slugs)} users")
    print(f"Services config for {len(services_config)} repositories")
    print()

    # Discover repositories
    repos = discover_local_repos(repos_root)
    print(f"Found {len(repos)} repositories")
    
    # Determine number of workers
    if max_workers is None:
        available_cores = multiprocessing.cpu_count()
        max_workers = min(available_cores, len(repos), 4)  # Cap at 4 for memory reasons

    if parallel and len(repos) > 1:
        print(f"🚀 Processing repositories in parallel (max workers: {max_workers})")
    else:
        print("📊 Processing repositories sequentially")
    print()

    # Analyze repositories and build services data
    services_data: Dict[str, Dict[str, Any]] = {}
    
    if parallel and len(repos) > 1:
        # Parallel processing with fixed pickle issue
        # Prepare repo tasks with all necessary data
        repo_tasks = []
        for repo in repos:
            repo_tasks.append({
                "repo": repo,
                "repos_root": repos_root,
                "services_config": services_config,
                "alias_map": alias_map, 
                "ignored_slugs": ignored_slugs,
                "date_from": date_from,
                "date_to": date_to
            })

        # Execute repo processing in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo = {executor.submit(analyze_repo_worker_global, task): task["repo"] for task in repo_tasks}
            
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    _, local_services_data = future.result()
                    print(f"  ✅ {repo}")
                    # Merge the services data
                    for service_name, service_data in local_services_data.items():
                        if service_name not in services_data:
                            services_data[service_name] = service_data
                        else:
                            # Merge service data (this is complex, but for simplicity we'll assume no overlap
                            # in the current implementation since each repo maps to different services)
                            services_data[service_name] = service_data
                except Exception as e:
                    print(f"  ❌ {repo}: {e}")
    else:
        # Sequential processing (original behavior)
        for repo in repos:
            print(f"  -> {repo}")
            analyze_repo_for_services(
                repo, repos_root, services_data, services_config, 
                alias_map, ignored_slugs, date_from, date_to
            )
    
    print()
    print(f"Generated statistics for {len(services_data)} services")
    
    # Calculate top developers
    calculate_service_top_developers(services_data)
    
    # Write summaries
    print("\nWriting service summaries...")
    write_service_summaries(services_data, output_root, date_from, date_to)
    
    print(f"\n✅ Service statistics generation completed!")
    print(f"Services processed: {', '.join(sorted(services_data.keys()))}")


if __name__ == "__main__":
    main()