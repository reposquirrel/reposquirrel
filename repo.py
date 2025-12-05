#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import json
import re
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List, Set

AuthorKey = Tuple[str, str]  # (name, email)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze local git repos for per-repo and per-service stats "
            "within a time window, grouped by developer, using configuration/alias.json, "
            "configuration/ignore_user.txt and optional configuration/services.json."
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
    return parser.parse_args()


def slugify(text: str) -> str:
    """Make a filesystem-safe, lowercase slug from a string."""
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def ensure_repo_output_folder(
    output_root: str, repo_rel_path: str, date_from: str, date_to: str
) -> str:
    """
    Create folder structure:
      <output_root>/stats/repos/<repo_rel_path>/<date_from>_<date_to>
    """
    base = os.path.join(output_root, "stats", "repos", repo_rel_path)
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


def ensure_service_entry(repo_data: Dict[str, Any], service_name: str) -> Dict[str, Any]:
    """
    Ensure the data structure for a given service exists inside repo_data.
    """
    if "services" not in repo_data:
        repo_data["services"] = {}
    services = repo_data["services"]
    if service_name not in services:
        services[service_name] = {
            "developers": {}
        }
    return services[service_name]


def ensure_dev_stats(
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
            "emails": set(),  # convert to list later
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0,
        }
    dev = devs[canonical_slug]
    if email:
        dev["emails"].add(email)
    if display_name and not dev.get("display_name"):
        dev["display_name"] = display_name
    return dev


def analyze_repo(
    repo_rel_path: str,
    repo_path: str,
    date_from_dt: datetime,
    date_to_dt: datetime,
    alias_map: Dict[str, str],
    services_config: Dict[str, Dict[str, list]],
    ignored_slugs: Set[str],
) -> Optional[Dict[str, Any]]:
    """
    Analyze a single repo and return its stats structure for the given time window only.

    We use git log with --since/--until matching the window, so every commit
    we see is inside [date_from_dt, date_to_dt].

    Commits whose author's canonical slug is in ignored_slugs are completely skipped.
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
        "services": {},
    }

    # git log: each commit line:
    #   "<sha>\x01<author_name>\x01<author_email>"
    # followed by numstat lines for that commit.
    cmd = [
        "git",
        "-C",
        repo_path,
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
        )
    except FileNotFoundError:
        print("ERROR: git command not found. Make sure git is installed and in PATH.", file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        print(f"    ! git log failed in {repo_path}")
        print(f"      return code: {result.returncode}")
        if result.stderr:
            print(f"      stderr: {result.stderr.strip()}")
        return None

    stdout = result.stdout
    if not stdout.strip():
        # No commits in window
        return None

    current_author_name: Optional[str] = None
    current_author_email: Optional[str] = None
    current_canonical_slug: Optional[str] = None
    current_display_name: Optional[str] = None
    current_services_touched: set = set()

    def finalize_current_commit():
        """
        When we reach a new commit header or end of log, record
        one "commit" per service this commit touched.
        """
        if (
            current_canonical_slug is None
            or current_author_name is None
            or current_author_email is None
        ):
            return

        for svc in current_services_touched:
            svc_data = ensure_service_entry(repo_data, svc)
            dev = ensure_dev_stats(
                svc_data,
                current_canonical_slug,
                current_display_name or current_author_name,
                current_author_email,
            )
            dev["commits"] += 1

    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Commit header line
        if "\x01" in line and "\t" not in line:
            # finalize previous commit
            finalize_current_commit()

            parts = line.split("\x01")
            sha = parts[0] if len(parts) > 0 else ""
            name = parts[1] if len(parts) > 1 else ""
            email = parts[2] if len(parts) > 2 else ""

            canonical_slug = canonical_slug_for_author(name, email, alias_map)

            # If this author is ignored, mark commit as ignored
            if canonical_slug in ignored_slugs:
                current_author_name = None
                current_author_email = None
                current_canonical_slug = None
                current_display_name = None
                current_services_touched = set()
                continue

            display_name = name

            current_author_name = name
            current_author_email = email
            current_canonical_slug = canonical_slug
            current_display_name = display_name
            current_services_touched = set()
            continue

        # numstat line: "<additions>\t<deletions>\t<file>"
        if "\t" in line and current_canonical_slug is not None:
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            add_str, del_str, filename = parts[0], parts[1], parts[2]

            # For binary files, git prints '-' instead of numbers
            try:
                add = int(add_str) if add_str != "-" else 0
            except ValueError:
                add = 0
            try:
                dele = int(del_str) if del_str != "-" else 0
            except ValueError:
                dele = 0

            if add == 0 and dele == 0:
                # nothing changed or only binary, skip for line stats
                continue

            norm_filename = filename.replace("\\", "/").lstrip("./")
            service_name = get_service_for_path(repo_rel_path, norm_filename, services_config)

            current_services_touched.add(service_name)

            svc_data = ensure_service_entry(repo_data, service_name)
            dev = ensure_dev_stats(
                svc_data,
                current_canonical_slug,
                current_display_name or current_author_name,
                current_author_email,
            )

            dev["lines_added"] += add
            dev["lines_deleted"] += dele
            dev["net_lines"] = dev["lines_added"] - dev["lines_deleted"]
            dev["changed_lines"] = dev["lines_added"] + dev["lines_deleted"]

    # finalize last commit
    finalize_current_commit()

    if not repo_data.get("services"):
        return None

    return repo_data


def pick_top_developer(devs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    From a developers dict, pick the top dev by changed_lines.
    """
    if not devs:
        return None

    best_slug = None
    best_val = -1
    for slug, d in devs.items():
        changed = d.get("changed_lines", 0)
        if changed > best_val:
            best_val = changed
            best_slug = slug

    if best_slug is None:
        return None

    best_dev = devs[best_slug]
    return {
        "slug": best_dev["slug"],
        "display_name": best_dev.get("display_name") or "",
        "changed_lines": best_dev.get("changed_lines", 0),
        "commits": best_dev.get("commits", 0),
    }


def finalize_repo_data(repo_data: Dict[str, Any]) -> None:
    """
    Convert internal sets to lists and compute top_developer per service
    and overall for the repo.
    """
    overall_dev_agg: Dict[str, Dict[str, Any]] = {}

    for svc_name, svc_data in repo_data.get("services", {}).items():
        devs = svc_data.get("developers", {})
        # convert emails set -> list, and aggregate into overall_dev_agg
        for slug, d in devs.items():
            if isinstance(d.get("emails"), set):
                d["emails"] = sorted(d["emails"])

            # aggregate into overall
            agg = overall_dev_agg.setdefault(
                slug,
                {
                    "slug": slug,
                    "display_name": d.get("display_name") or "",
                    "emails": set(),
                    "commits": 0,
                    "lines_added": 0,
                    "lines_deleted": 0,
                    "net_lines": 0,
                    "changed_lines": 0,
                },
            )
            for e in d.get("emails", []):
                agg["emails"].add(e)
            agg["commits"] += d.get("commits", 0)
            agg["lines_added"] += d.get("lines_added", 0)
            agg["lines_deleted"] += d.get("lines_deleted", 0)
            agg["net_lines"] = agg["lines_added"] - agg["lines_deleted"]
            agg["changed_lines"] = agg["lines_added"] + agg["lines_deleted"]

        # top dev for this service
        top = pick_top_developer(devs)
        if top is not None:
            svc_data["top_developer"] = top

    # finalize overall dev agg and top dev for repo
    for d in overall_dev_agg.values():
        if isinstance(d.get("emails"), set):
            d["emails"] = sorted(d["emails"])

    top_overall = pick_top_developer(overall_dev_agg)
    if top_overall is not None:
        repo_data["top_developer"] = top_overall
    repo_data["developers"] = overall_dev_agg


def main() -> None:
    args = parse_args()

    date_from = args.date_from
    date_to = args.date_to
    repos_root = args.repos_root
    output_root = args.output_root
    services_file = args.services_file
    ignore_file = args.ignore_file

    # Basic date validation
    try:
        date_from_dt = datetime.fromisoformat(date_from)
        date_to_dt = datetime.fromisoformat(date_to)
    except ValueError as e:
        print(f"ERROR: Invalid date format: {e}", file=sys.stderr)
        sys.exit(1)

    if date_from_dt > date_to_dt:
        print("ERROR: --from date must be <= --to date", file=sys.stderr)
        sys.exit(1)

    print("Analyzing LOCAL git repos for per-repo/service stats in time window...")
    print(f"Date window: {date_from} to {date_to}")
    print(f"Repos root: {repos_root}")

    repo_list = discover_local_repos(repos_root)
    if not repo_list:
        print(f"No git repos found under '{repos_root}'. Nothing to do.")
        sys.exit(0)

    print(f"Discovered {len(repo_list)} repos:")

    alias_map = load_aliases("configuration/alias.json")
    if alias_map:
        print(f"Loaded {len(alias_map)} alias mappings from configuration/alias.json")

    services_config = load_services_config(services_file)
    if services_config:
        print(f"Loaded services configuration from {services_file}")

    ignored_slugs = load_ignored_users(ignore_file)
    if ignored_slugs:
        print(f"Loaded {len(ignored_slugs)} ignored user identifiers from {ignore_file}")

    repo_list = sorted(repo_list)

    for repo_rel in repo_list:
        repo_path = os.path.join(repos_root, repo_rel)
        print(f"  -> {repo_rel}")
        repo_data = analyze_repo(
            repo_rel,
            repo_path,
            date_from_dt,
            date_to_dt,
            alias_map,
            services_config,
            ignored_slugs,
        )
        if not repo_data:
            print("     (no commits in window after filtering)")
            continue

        finalize_repo_data(repo_data)

        out_folder = ensure_repo_output_folder(output_root, repo_rel, date_from, date_to)
        out_path = os.path.join(out_folder, "summary.json")
        summary = {
            "repo": repo_data["repo"],
            "from": date_from,
            "to": date_to,
            "services": repo_data["services"],
            "developers": repo_data["developers"],
            "top_developer": repo_data.get("top_developer"),
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "repos_root": os.path.abspath(repos_root),
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print(f"     -> stats written to {out_path}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()

