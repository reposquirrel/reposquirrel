#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import json
import re
from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# -----------------------------
# Argument parsing
# -----------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute blame-based ownership per repo and per service. "
            "Uses git blame to see who last touched each line, grouped by configuration/services.json, "
            "with configuration/alias.json and configuration/ignore_user.txt."
        )
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
        help="Root directory under which 'stats/repos/...' will be created (default: current directory)",
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
        "--alias-file",
        dest="alias_file",
        default="configuration/alias.json",
        help="JSON file mapping user aliases to canonical names (default: configuration/alias.json)",
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


# -----------------------------
# Shared helpers
# -----------------------------


def slugify(text: str) -> str:
    """Make a filesystem-safe, lowercase slug from a string."""
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def ensure_repo_blame_output_folder(output_root: str, repo_rel_path: str) -> str:
    """
    Create folder structure:
      <output_root>/stats/repos/<repo_rel_path>/blame
    """
    base = os.path.join(output_root, "stats", "repos", repo_rel_path, "blame")
    os.makedirs(base, exist_ok=True)
    return base


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


def load_aliases(alias_path: str = "configuration/alias.json") -> Dict[str, str]:
    """
    Load alias configuration.

    Expected format (canonical -> [aliases]):

    {
      "viola-sorgato": [
        "114474500-violasorgato"
      ]
    }

    Returns mapping: alias_slug -> canonical_slug
    """
    if not os.path.isfile(alias_path):
        return {}

    try:
        with open(alias_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"WARNING: Failed to load alias file '{alias_path}': {e}", file=sys.stderr)
        return {}

    alias_map: Dict[str, str] = {}

    if not isinstance(data, dict):
        print(f"WARNING: configuration/alias.json must be a JSON object (canonical -> [aliases])", file=sys.stderr)
        return {}

    for canonical, aliases in data.items():
        canonical_slug = slugify(canonical)
        # canonical maps to itself as well
        alias_map[canonical_slug] = canonical_slug

        if isinstance(aliases, list):
            for alias in aliases:
                alias_slug = slugify(str(alias))
                alias_map[alias_slug] = canonical_slug
        else:
            print(f"WARNING: alias value for '{canonical}' should be a list", file=sys.stderr)

    return alias_map


def load_ignored_users(ignore_path: str = "configuration/ignore_user.txt") -> Set[str]:
    """
    Load ignored users from a text file.

    Each non-empty, non-comment line is interpreted as an identifier for a user,
    and converted into one or more possible slugs.

    Examples of valid entries:
      - 98109129-renovate-appgate-bot          (canonical slug)
      - 98109129+renovate-appgate[bot]@users.noreply.github.com
      - renovate-appgate[bot]

    For each line:
      - we slugify the whole line
      - if it looks like an email, we also slugify the local-part (before '@')
    """
    ignored: Set[str] = set()

    if not os.path.isfile(ignore_path):
        return ignored

    try:
        with open(ignore_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                # slugify full line
                ignored.add(slugify(line))

                # if it looks like an email, also slugify local part
                if "@" in line:
                    local = line.split("@", 1)[0]
                    ignored.add(slugify(local))
    except Exception as e:
        print(f"WARNING: Failed to load ignore file '{ignore_path}': {e}", file=sys.stderr)

    return ignored


def load_services_config(services_path: str) -> Dict[str, Dict[str, list]]:
    """
    Load services configuration from JSON.

    Expected format:

    {
      "repo-name": {
        "service1": ["service1/"],
        "service2": ["service2/"],
        "main": [""]
      }
    }

    Keys:
      - repo_rel_path (relative to repos_root)

    Values:
      - mapping: service_name -> list of path prefixes (relative to repo root)
    """
    if not os.path.isfile(services_path):
        return {}

    try:
        with open(services_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"WARNING: Failed to load services file '{services_path}': {e}", file=sys.stderr)
        return {}

    if not isinstance(data, dict):
        print(
            "WARNING: configuration/services.json must be a JSON object "
            "(repo -> {service: [paths]})",
            file=sys.stderr,
        )
        return {}

    normalized: Dict[str, Dict[str, list]] = {}
    for repo_rel, services in data.items():
        if not isinstance(services, dict):
            print(f"WARNING: services for repo '{repo_rel}' should be an object; skipping.", file=sys.stderr)
            continue
        norm_services: Dict[str, list] = {}
        for svc_name, prefixes in services.items():
            if isinstance(prefixes, list):
                norm_services[svc_name] = [str(p) for p in prefixes]
            else:
                # Allow a single string as shorthand
                norm_services[svc_name] = [str(prefixes)]
        normalized[repo_rel] = norm_services

    return normalized


def default_service_name_for_repo(repo_rel_path: str) -> str:
    """Default service name for a repo: its last path segment."""
    return repo_rel_path.strip("/").split("/")[-1] or "unknown-service"


def get_service_for_path(
    repo_rel_path: str,
    file_path: str,
    services_config: Dict[str, Dict[str, list]],
) -> str:
    """
    Decide which service a file belongs to, based on services_config.

    Rules:
      - If repo has an entry in services_config:
          * For each service, we have a list of prefixes.
          * We choose the service whose prefix is the longest match at the start of file_path.
          * Prefix "" or "." is treated as a catch-all.
      - If repo is NOT in services_config:
          * Treat the entire repo as a single service:
                default_service_name_for_repo(repo_rel_path)
    """
    mapping = services_config.get(repo_rel_path)
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
                best_len = plen
                best_service = svc_name

    if best_service is not None:
        return best_service

    # Fallback: single-service repo name
    return default_service_name_for_repo(repo_rel_path)


def author_slug_from_name_email(name: str, email: str) -> str:
    """
    Build a base slug from author email (local-part) or name, then slugify.
    """
    if email:
        base = email.split("@")[0]
    else:
        base = name or "unknown-author"
    return slugify(base)


def canonical_slug_for_author(name: str, email: str, alias_map: Dict[str, str]) -> str:
    """
    Map an author to a canonical slug using alias_map.
    """
    base_slug = author_slug_from_name_email(name, email)
    return alias_map.get(base_slug, base_slug)


# -----------------------------
# Blame-specific data helpers
# -----------------------------


def ensure_service_entry(repo_data: Dict[str, Any], service_name: str) -> Dict[str, Any]:
    """
    Ensure the data structure for a given service exists inside repo_data.
    """
    if "services" not in repo_data:
        repo_data["services"] = {}
    services = repo_data["services"]
    if service_name not in services:
        services[service_name] = {
            "total_lines": 0,
            "developers": {},
        }
    return services[service_name]


def ensure_service_dev(
    svc_data: Dict[str, Any],
    canonical_slug: str,
    display_name: str,
    email: str,
) -> Dict[str, Any]:
    """
    Ensure a developer stats record within a service.
    """
    devs = svc_data.setdefault("developers", {})
    if canonical_slug not in devs:
        devs[canonical_slug] = {
            "slug": canonical_slug,
            "display_name": display_name,
            "emails": set(),  # will turn into list later
            "lines": 0,
        }
    dev = devs[canonical_slug]
    if email:
        dev["emails"].add(email)
    if display_name and not dev.get("display_name"):
        dev["display_name"] = display_name
    return dev


def ensure_repo_dev(
    repo_data: Dict[str, Any],
    canonical_slug: str,
    display_name: str,
    email: str,
) -> Dict[str, Any]:
    """
    Ensure a developer stats record at the repo level.
    """
    devs = repo_data.setdefault("developers", {})
    if canonical_slug not in devs:
        devs[canonical_slug] = {
            "slug": canonical_slug,
            "display_name": display_name,
            "emails": set(),
            "lines": 0,
            "services": {},  # service_name -> {"lines": ...}
        }
    dev = devs[canonical_slug]
    if email:
        dev["emails"].add(email)
    if display_name and not dev.get("display_name"):
        dev["display_name"] = display_name
    return dev


def ensure_repo_dev_service_entry(dev: Dict[str, Any], service_name: str) -> Dict[str, Any]:
    """
    Ensure that a repo-level dev has a per-service entry.
    """
    services = dev.setdefault("services", {})
    if service_name not in services:
        services[service_name] = {"lines": 0}
    return services[service_name]


def pick_top_developer_by_lines(devs: Dict[str, Any], total_lines: int) -> Optional[Dict[str, Any]]:
    """
    From a developers dict, pick the top dev by 'lines'.
    Also compute share = lines / total_lines if total_lines > 0.
    """
    if not devs or total_lines <= 0:
        return None

    best_slug = None
    best_val = -1
    for slug, d in devs.items():
        lines = d.get("lines", 0)
        if lines > best_val:
            best_val = lines
            best_slug = slug

    if best_slug is None:
        return None

    best_dev = devs[best_slug]
    share = best_dev.get("lines", 0) / float(total_lines) if total_lines > 0 else 0.0

    return {
        "slug": best_dev["slug"],
        "display_name": best_dev.get("display_name") or "",
        "lines": best_dev.get("lines", 0),
        "share": share,
    }


def finalize_repo_data(repo_data: Dict[str, Any]) -> None:
    """
    Convert internal sets to lists and compute top_developer per service
    and overall for the repo based on blame line counts.
    """
    # Per-service: finalize emails + top_developer
    for svc_name, svc_data in repo_data.get("services", {}).items():
        devs = svc_data.get("developers", {})
        for d in devs.values():
            if isinstance(d.get("emails"), set):
                d["emails"] = sorted(d["emails"])
        total_lines = svc_data.get("total_lines", 0)
        top = pick_top_developer_by_lines(devs, total_lines)
        if top is not None:
            svc_data["top_developer"] = top

    # Repo-level: finalize emails + per-service lines, plus top_developer
    for dev in repo_data.get("developers", {}).values():
        if isinstance(dev.get("emails"), set):
            dev["emails"] = sorted(dev["emails"])

    total_repo_lines = repo_data.get("total_lines", 0)
    repo_top = pick_top_developer_by_lines(repo_data.get("developers", {}), total_repo_lines)
    if repo_top is not None:
        repo_data["top_developer"] = repo_top


# -----------------------------
# Core blame analysis
# -----------------------------


def get_tracked_files(repo_path: str) -> List[str]:
    """
    Use `git ls-files` to get a list of tracked files in the repo.
    """
    cmd = ["git", "-C", repo_path, "ls-files"]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        print("ERROR: git command not found. Make sure git is installed and in PATH.", file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        print(f"    ! git ls-files failed in {repo_path}")
        if result.stderr:
            print(f"      stderr: {result.stderr.strip()}")
        return []

    files = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    return files


def analyze_file_blame(
    repo_rel_path: str,
    repo_path: str,
    file_path: str,
    repo_data: Dict[str, Any],
    alias_map: Dict[str, str],
    services_config: Dict[str, Dict[str, list]],
    ignored_slugs: Set[str],
) -> None:
    """
    Run git blame on a single file and update repo_data with line ownership.

    Uses git blame --line-porcelain and parses author / author-mail per block.

    IMPORTANT: We run subprocess in binary mode and decode with 'utf-8' + errors='replace'
    to avoid UnicodeDecodeError on non-UTF8 content.
    """
    full_path = os.path.join(repo_path, file_path)
    if not os.path.exists(full_path):
        return

    cmd = [
        "git",
        "-C",
        repo_path,
        "blame",
        "--line-porcelain",
        "--",
        file_path,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=False,  # binary mode
            check=False,
            timeout=300  # 5 minute timeout for individual file blame
        )
    except subprocess.TimeoutExpired:
        print(f"WARNING: git blame timed out for {file_path}, skipping...", file=sys.stderr)
        return
    except FileNotFoundError:
        print("ERROR: git command not found. Make sure git is installed and in PATH.", file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        # Could be a binary file or something odd; skip it.
        return

    # Decode safely to avoid UnicodeDecodeError
    try:
        stdout = (result.stdout or b"").decode("utf-8", errors="replace")
    except Exception:
        # If decoding really fails, skip file
        return

    if not stdout.strip():
        return

    current_author_name: Optional[str] = None
    current_author_email: Optional[str] = None
    current_canonical_slug: Optional[str] = None
    current_ignored: bool = False

    # Determine service once per file
    service_name = get_service_for_path(repo_rel_path, file_path, services_config)

    for line in stdout.splitlines():
        # Porcelain blame metadata lines
        if line.startswith("author "):
            current_author_name = line[len("author "):].strip()
            continue

        if line.startswith("author-mail "):
            mail = line[len("author-mail "):].strip()
            if mail.startswith("<") and mail.endswith(">"):
                mail = mail[1:-1]
            current_author_email = mail

            canonical = canonical_slug_for_author(
                current_author_name or "",
                current_author_email or "",
                alias_map,
            )
            current_canonical_slug = canonical
            current_ignored = canonical in ignored_slugs
            continue

        # Content lines start with a tab
        if not line.startswith("\t"):
            continue

        if current_canonical_slug is None or current_ignored:
            continue

        display_name = current_author_name or current_canonical_slug

        # repo totals
        repo_data["total_lines"] = repo_data.get("total_lines", 0) + 1

        # service-level
        svc_data = ensure_service_entry(repo_data, service_name)
        svc_data["total_lines"] = svc_data.get("total_lines", 0) + 1
        svc_dev = ensure_service_dev(
            svc_data,
            current_canonical_slug,
            display_name,
            current_author_email or "",
        )
        svc_dev["lines"] += 1

        # repo developer totals
        repo_dev = ensure_repo_dev(
            repo_data,
            current_canonical_slug,
            display_name,
            current_author_email or "",
        )
        repo_dev["lines"] += 1
        per_svc = ensure_repo_dev_service_entry(repo_dev, service_name)
        per_svc["lines"] += 1


def analyze_repo(
    repo_rel_path: str,
    repo_path: str,
    alias_map: Dict[str, str],
    services_config: Dict[str, Dict[str, list]],
    ignored_slugs: Set[str],
) -> Optional[Dict[str, Any]]:
    """
    Analyze one repo using git blame on all tracked files.

    Returns:
      repo_data dict or None if nothing was processed.
    """
    if not os.path.isdir(repo_path):
        print(f"    ! Repo path does not exist: {repo_path}")
        return None

    git_dir = os.path.join(repo_path, ".git")
    if not os.path.isdir(git_dir):
        print(f"    ! Not a git repo (no .git directory): {repo_path}")
        return None

    repo_data: Dict[str, Any] = {
        "repo": repo_rel_path,
        "total_lines": 0,
        "services": {},
        "developers": {},
    }

    files = get_tracked_files(repo_path)
    if not files:
        return None

    total_files = len(files)
    print(f"     Analyzing {total_files} files...")
    
    for i, f in enumerate(files):
        if i > 0 and i % 100 == 0:  # Progress every 100 files
            print(f"     Progress: {i}/{total_files} files ({i/total_files*100:.1f}%)")
            sys.stdout.flush()
            
        analyze_file_blame(
            repo_rel_path,
            repo_path,
            f,
            repo_data,
            alias_map,
            services_config,
            ignored_slugs,
        )

    if repo_data.get("total_lines", 0) == 0:
        return None

    return repo_data


# -----------------------------
# Worker function for parallel processing
# -----------------------------

def blame_repo_worker(repo_data):
    """Worker function for processing a single repository in parallel mode"""
    repo_rel = repo_data["repo_rel"]
    repo_path = repo_data["repo_path"]
    alias_map = repo_data["alias_map"]
    services_config = repo_data["services_config"]
    ignored_slugs = repo_data["ignored_slugs"]
    output_root = repo_data["output_root"]
    repos_root = repo_data["repos_root"]
    
    repo_result = analyze_repo(
        repo_rel,
        repo_path,
        alias_map,
        services_config,
        ignored_slugs,
    )
    if not repo_result:
        return (repo_rel, None, "no blame data")

    finalize_repo_data(repo_result)

    out_folder = ensure_repo_blame_output_folder(output_root, repo_rel)
    out_path = os.path.join(out_folder, "blame.json")

    summary = {
        "repo": repo_result["repo"],
        "services": repo_result["services"],
        "developers": repo_result["developers"],
        "top_developer": repo_result.get("top_developer"),
        "total_lines": repo_result.get("total_lines", 0),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "repos_root": os.path.abspath(repos_root),
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return (repo_rel, out_path, "success")


# -----------------------------
# Main
# -----------------------------


def main() -> None:
    args = parse_args()

    repos_root = args.repos_root
    output_root = args.output_root
    services_file = args.services_file
    ignore_file = args.ignore_file
    alias_file = args.alias_file
    parallel = args.parallel
    max_workers = args.max_workers

    print("Analyzing LOCAL git repos for blame-based ownership...")
    print(f"Repos root: {repos_root}")

    repo_list = discover_local_repos(repos_root)
    if not repo_list:
        print(f"No git repos found under '{repos_root}'. Nothing to do.")
        sys.exit(0)

    # Determine number of workers
    if max_workers is None:
        available_cores = multiprocessing.cpu_count()
        # Increase cap to 6 for systems with more cores, as blame analysis is often I/O bound  
        worker_cap = 6 if available_cores >= 8 else 4
        max_workers = min(available_cores, len(repo_list), worker_cap)

    print(f"Discovered {len(repo_list)} repos:")
    if parallel and len(repo_list) > 1:
        print(f"🚀 Processing repositories in parallel (max workers: {max_workers})")
    else:
        print("📊 Processing repositories sequentially")

    alias_map = load_aliases(alias_file)
    if alias_map:
        print(f"Loaded {len(alias_map)} alias mappings from {alias_file}")

    services_config = load_services_config(services_file)
    if services_config:
        print(f"Loaded services configuration from {services_file}")

    ignored_slugs = load_ignored_users(ignore_file)
    if ignored_slugs:
        print(f"Loaded {len(ignored_slugs)} ignored user identifiers from {ignore_file}")

    repo_list = sorted(repo_list)
    total_repos = len(repo_list)

    if parallel and len(repo_list) > 1:
        # Parallel processing
        # Prepare repo tasks with all necessary data
        repo_tasks = []
        for repo_rel in repo_list:
            repo_path = os.path.join(repos_root, repo_rel)
            repo_tasks.append({
                "repo_rel": repo_rel,
                "repo_path": repo_path,
                "alias_map": alias_map,
                "services_config": services_config,
                "ignored_slugs": ignored_slugs,
                "output_root": output_root,
                "repos_root": repos_root
            })

        # Execute repo processing in parallel
        completed_repos = 0
        failed_repos = 0
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo = {executor.submit(blame_repo_worker, task): task["repo_rel"] for task in repo_tasks}
            
            for future in as_completed(future_to_repo):
                repo_rel = future_to_repo[future]
                try:
                    repo_rel, out_path, status = future.result()
                    completed_repos += 1
                    print(f"  ✅ {repo_rel} ({completed_repos}/{total_repos})")
                    if status == "success":
                        print(f"     -> blame stats written to {out_path}")
                    elif status == "no blame data":
                        print(f"     (no blame data)")
                    sys.stdout.flush()
                except Exception as e:
                    failed_repos += 1
                    print(f"  ❌ {repo_rel} ({completed_repos + failed_repos}/{total_repos}): {e}")
                    
        if failed_repos > 0:
            print(f"\n⚠️  Completed with {failed_repos} failures out of {total_repos} repositories")
        else:
            print(f"\n✅ All {completed_repos} repositories processed successfully in parallel!")
            
    else:
        # Sequential processing (original behavior)
        for repo_index, repo_rel in enumerate(repo_list, 1):
            repo_path = os.path.join(repos_root, repo_rel)
            print(f"  -> {repo_rel} ({repo_index}/{total_repos})")
            sys.stdout.flush()  # Ensure immediate output
            
            repo_data = analyze_repo(
                repo_rel,
                repo_path,
                alias_map,
                services_config,
                ignored_slugs,
            )
            if not repo_data:
                print("     (no blame data)")
                continue

            finalize_repo_data(repo_data)

            out_folder = ensure_repo_blame_output_folder(output_root, repo_rel)
            out_path = os.path.join(out_folder, "blame.json")

            summary = {
                "repo": repo_data["repo"],
                "services": repo_data["services"],
                "developers": repo_data["developers"],
                "top_developer": repo_data.get("top_developer"),
                "total_lines": repo_data.get("total_lines", 0),
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "repos_root": os.path.abspath(repos_root),
            }

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)

            print(f"     -> blame stats written to {out_path}")
            sys.stdout.flush()  # Ensure immediate output

    print("\n=== Done ===")


if __name__ == "__main__":
    main()

