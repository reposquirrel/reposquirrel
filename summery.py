#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import json
import re
import csv
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

AuthorKey = Tuple[str, str]  # (name, email)


def load_aliases(alias_path: str = "configuration/alias.json") -> Dict[str, str]:
    """
    Load author aliases from JSON file.
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
    except (json.JSONDecodeError, IOError) as e:
        print(f"WARNING: Failed to load alias file '{alias_path}': {e}", file=sys.stderr)
        return {}
    
    alias_map: Dict[str, str] = {}
    
    if not isinstance(data, dict):
        print(f"WARNING: {alias_path} must be a JSON object (canonical -> [aliases])", file=sys.stderr)
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

# Languages that we treat as "documentation" when cloc reports them
DOC_LANGUAGES = {
    "markdown",
    "text",
    "restructuredtext",
    "asciidoc",
}

WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze local git repos for commits and line changes in a date range, "
            "grouped by author, with language, prod/test, documentation, weekday, "
            "and hour-of-day stats."
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
        help="Root directory under which 'stats/users/...' will be created (default: current directory)",
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
    parser.add_argument(
        "--alias-file",
        dest="alias_file",
        default="configuration/alias.json",
        help="JSON file mapping user emails to display names (default: configuration/alias.json)",
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


def ensure_output_folder(
    output_root: str, author_slug: str, date_from: str, date_to: str
) -> str:
    """
    Create folder structure:
      <output_root>/stats/users/<author_slug>/<date_from>_<date_to>
    """
    base = os.path.join(output_root, "stats", "users", author_slug)
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


def init_author_record(name: str, email: str) -> Dict[str, Any]:
    return {
        "name": name,
        "email": email,
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "per_repo": {},      # repo_id -> {commits, additions, deletions, net_lines, languages, code_type, documentation}
        "languages": {},     # lang -> {additions, deletions, net_lines}
        "code_type": {},     # "prod"/"test" -> {additions, deletions, net_lines}
        "documentation": {   # overall documentation stats across all repos
            "additions": 0,
            "deletions": 0,
            "net_lines": 0,
        },
        "per_weekday": {},   # weekday -> {commits, additions, deletions, net_lines}
        "per_hour": {},      # "00".."23" -> {commits, additions, deletions, net_lines}
        "per_date": {},      # "YYYY-MM-DD" -> {commits, additions, deletions, net_lines}
    }


def ensure_repo_record(author_data: Dict[str, Any], repo_id: str) -> Dict[str, Any]:
    per_repo = author_data["per_repo"]
    if repo_id not in per_repo:
        per_repo[repo_id] = {
            "commits": 0,
            "additions": 0,
            "deletions": 0,
            "net_lines": 0,
            "languages": {},      # lang -> {additions, deletions, net_lines}
            "code_type": {},      # "prod"/"test" -> {additions, deletions, net_lines}
            "documentation": {    # doc lines in this repo
                "additions": 0,
                "deletions": 0,
                "net_lines": 0,
            },
        }
    return per_repo[repo_id]


def ensure_lang_record(container: Dict[str, Any], lang: str) -> Dict[str, int]:
    """
    Ensure a language record inside either:
      - author_data["languages"], or
      - repo_stats["languages"]
    """
    if "languages" not in container:
        container["languages"] = {}
    langs = container["languages"]
    if lang not in langs:
        langs[lang] = {"additions": 0, "deletions": 0, "net_lines": 0}
    return langs[lang]


def ensure_code_type_record(container: Dict[str, Any], code_kind: str) -> Dict[str, int]:
    """
    Ensure a code-type record inside either:
      - author_data["code_type"], or
      - repo_stats["code_type"]

    code_kind is typically "prod" or "test".
    """
    if "code_type" not in container:
        container["code_type"] = {}
    cts = container["code_type"]
    if code_kind not in cts:
        cts[code_kind] = {"additions": 0, "deletions": 0, "net_lines": 0}
    return cts[code_kind]


def ensure_docs_record(container: Dict[str, Any]) -> Dict[str, int]:
    """
    Ensure a documentation record inside:
      - author_data["documentation"], or
      - repo_stats["documentation"]
    """
    if "documentation" not in container:
        container["documentation"] = {"additions": 0, "deletions": 0, "net_lines": 0}
    return container["documentation"]


def ensure_weekday_record(container: Dict[str, Any], weekday_name: str) -> Dict[str, int]:
    """
    Ensure a weekday record inside author_data["per_weekday"].
    """
    if "per_weekday" not in container:
        container["per_weekday"] = {}
    wd = container["per_weekday"]
    if weekday_name not in wd:
        wd[weekday_name] = {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}
    return wd[weekday_name]


def ensure_hour_record(container: Dict[str, Any], hour_str: str) -> Dict[str, int]:
    """
    Ensure an hour-of-day record inside author_data["per_hour"].
    hour_str should be "00".."23".
    """
    if "per_hour" not in container:
        container["per_hour"] = {}
    ph = container["per_hour"]
    if hour_str not in ph:
        ph[hour_str] = {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}
    return ph[hour_str]


def ensure_date_record(container: Dict[str, Any], date_str: str) -> Dict[str, int]:
    """
    Ensure a per-date record inside author_data["per_date"].
    date_str should be "YYYY-MM-DD".
    """
    if "per_date" not in container:
        container["per_date"] = {}
    pd = container["per_date"]
    if date_str not in pd:
        pd[date_str] = {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}
    return pd[date_str]


def is_test_file(path: str) -> bool:
    """
    Heuristic to decide whether a file path is test code.

    Rules (all case-insensitive):
      - path segments named 'test', 'tests', 'testing', 'spec'
      - Java / general: any path containing '/test/' or '/tests/' or '/src/test/'
      - Go: *_test.go
      - Python: test_*.py or *_test.py
      - JS/TS: test_*.js/ts, *.test.js/ts, *.spec.js/ts
    """
    p = path.replace("\\", "/").lower()

    # Common test directories
    if "/test/" in p or "/tests/" in p or "/testing/" in p or "/spec/" in p:
        return True
    if "/src/test/" in p:
        return True

    # Split into filename
    filename = p.split("/")[-1]

    # Go tests: *_test.go
    if filename.endswith("_test.go"):
        return True

    # Python tests: test_*.py or *_test.py
    if filename.endswith(".py") and (filename.startswith("test_") or filename.endswith("_test.py")):
        return True

    # JS/TS tests
    if filename.endswith((".js", ".jsx", ".ts", ".tsx")):
        if filename.startswith("test_") or filename.endswith((
            ".test.js", ".test.jsx", ".test.ts", ".test.tsx",
            ".spec.js", ".spec.jsx", ".spec.ts", ".spec.tsx"
        )):
            return True

    return False


def is_doc_language(lang: str) -> bool:
    """
    Decide if a cloc language should count as documentation (Markdown, Text, etc.).
    """
    if not lang:
        return False
    return lang.strip().lower() in DOC_LANGUAGES


def get_cloc_file_languages(repo_path: str) -> Dict[str, str]:
    """
    Run cloc on the repo and return a mapping: relative_path -> language.

    Uses: cloc --by-file --csv --quiet .
    Requires: cloc installed and available in PATH.

    Handles:
      • leading './' in cloc output
      • Windows '\' slashes
      • ensures both './path' and 'path' map to the same language
    """
    cmd = ["cloc", "--by-file", "--csv", "--quiet", "."]

    try:
        result = subprocess.run(
            cmd,
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False,
            timeout=300  # 5 minute timeout for cloc
        )
    except FileNotFoundError:
        print(
            "WARNING: 'cloc' not found. Language stats will be 'Unknown'. "
            "Install cloc to enable language breakdown.",
            file=sys.stderr,
        )
        return {}
    except subprocess.TimeoutExpired:
        print(
            f"WARNING: cloc timed out in {repo_path} (5 minutes). "
            "Language stats will be 'Unknown'.",
            file=sys.stderr,
        )
        return {}

    if result.returncode != 0:
        print(
            f"WARNING: cloc failed in {repo_path} (exit {result.returncode}). "
            f"stderr: {result.stderr.strip() if result.stderr else ''}",
            file=sys.stderr,
        )
        return {}

    text = result.stdout or ""
    if not text.strip():
        return {}

    file_langs: Dict[str, str] = {}

    reader = csv.reader(text.splitlines())
    header_found = False
    lang_idx = None
    file_idx = None

    # Find header: language,filename,blank,comment,code,...
    for row in reader:
        if not row:
            continue

        # Skip version/header line
        if row[0].startswith("github.com/AlDanial/cloc"):
            continue

        lowered = [col.strip().lower() for col in row]
        if "language" in lowered and "filename" in lowered:
            lang_idx = lowered.index("language")
            file_idx = lowered.index("filename")
            header_found = True
            break

    if not header_found or lang_idx is None or file_idx is None:
        print(
            f"WARNING: Could not find cloc CSV header with 'language' and 'filename' in {repo_path}",
            file=sys.stderr,
        )
        return {}

    # Parse data rows
    for row in reader:
        if not row:
            continue
        if len(row) <= max(lang_idx, file_idx):
            continue

        lang = row[lang_idx].strip()
        fname = row[file_idx].strip()

        # Skip summary rows etc.
        if not lang or not fname or lang.upper() == "LANGUAGE" or lang.upper() == "SUM":
            continue

        # Normalize path:
        # - convert backslashes to forward slashes
        # - remove leading "./" if present
        norm_path = fname.replace("\\", "/")
        stripped = norm_path.lstrip("./")

        # Store mapping for both "service/..." and "./service/..." just in case
        if stripped:
            file_langs[stripped] = lang
            file_langs[f"./{stripped}"] = lang
        else:
            file_langs[norm_path] = lang

    return file_langs


def weekday_name_from_date_str(date_str: str) -> Optional[str]:
    """
    Given a date string in YYYY-MM-DD format, return weekday name ("Monday", ...).
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return WEEKDAY_NAMES[dt.weekday()]
    except Exception:
        return None


def hour_from_hour_str(hour_str: str) -> Optional[str]:
    """
    Given an hour string 'HH', validate and normalize to '00'..'23'.
    """
    try:
        h = int(hour_str)
        if 0 <= h <= 23:
            return f"{h:02d}"
        return None
    except Exception:
        return None


def analyze_repo(
    repo_rel_path: str,
    repo_path: str,
    date_from: str,
    date_to: str,
    authors: Dict[AuthorKey, Dict[str, Any]],
) -> None:
    """
    Use git log locally to count commits and line changes for ALL authors in a date range.

    Updates the 'authors' dict in-place, including language, prod/test, documentation,
    weekday stats, and hour-of-day stats.
    """
    if not os.path.isdir(repo_path):
        print(f"    ! Repo path does not exist: {repo_path}")
        return

    git_dir = os.path.join(repo_path, ".git")
    if not os.path.isdir(git_dir):
        print(f"    ! Not a git repo (no .git directory): {repo_path}")
        return

    # Build file -> language map using cloc
    file_langs = get_cloc_file_languages(repo_path)

    # git log: each commit line:
    # "<sha>\x01<author_name>\x01<author_email>\x01<date>"
    # where <date> is "YYYY-MM-DD HH"
    # followed by numstat lines for that commit.
    cmd = [
        "git",
        "-C",
        repo_path,
        "log",
        f"--since={date_from}",
        f"--until={date_to}",
        "--no-merges",  # exclude merge commits as requested
        "--date=format:%Y-%m-%d %H",
        "--pretty=format:%H%x01%an%x01%ae%x01%ad",
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
        return

    stdout = result.stdout
    if not stdout.strip():
        # No commits matched
        return

    current_author_key: Optional[AuthorKey] = None
    current_weekday_name: Optional[str] = None
    current_hour_str: Optional[str] = None
    current_date_str: Optional[str] = None

    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Commit header line
        if "\x01" in line and "\t" not in line:
            parts = line.split("\x01")
            sha = parts[0] if len(parts) > 0 else ""
            name = parts[1] if len(parts) > 1 else ""
            email = parts[2] if len(parts) > 2 else ""
            date_str = parts[3] if len(parts) > 3 else ""

            # date_str format: "YYYY-MM-DD HH"
            date_part = None
            hour_part = None
            if " " in date_str:
                date_part, hour_part = date_str.split(" ", 1)
            else:
                date_part = date_str

            weekday = weekday_name_from_date_str(date_part) if date_part else None
            hour = hour_from_hour_str(hour_part) if hour_part else None

            key: AuthorKey = (name, email)
            if key not in authors:
                authors[key] = init_author_record(name, email)

            author_data = authors[key]
            author_data["total_commits"] += 1

            repo_stats = ensure_repo_record(author_data, repo_rel_path)
            repo_stats["commits"] += 1

            # Weekday commit count
            if weekday:
                wd_stats = ensure_weekday_record(author_data, weekday)
                wd_stats["commits"] += 1

            # Hour-of-day commit count
            if hour:
                hr_stats = ensure_hour_record(author_data, hour)
                hr_stats["commits"] += 1

            # Daily commit count
            if date_part:
                date_stats = ensure_date_record(author_data, date_part)
                date_stats["commits"] += 1

            current_author_key = key
            current_weekday_name = weekday
            current_hour_str = hour
            current_date_str = date_part
            continue

        # numstat line: "<additions>\t<deletions>\t<file>"
        if "\t" in line and current_author_key is not None:
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

            # Normalize filename similar to cloc mapping (no leading ./)
            norm_filename = filename.replace("\\", "/").lstrip("./")

            # Language detection via cloc mapping
            lang = file_langs.get(norm_filename, "Unknown")

            # Prod vs test classification (for code files)
            code_kind = "test" if is_test_file(norm_filename) else "prod"

            author_data = authors[current_author_key]
            author_data["total_lines_added"] += add
            author_data["total_lines_deleted"] += dele

            # Per-repo totals
            repo_stats = ensure_repo_record(author_data, repo_rel_path)
            repo_stats["additions"] += add
            repo_stats["deletions"] += dele
            repo_stats["net_lines"] = repo_stats["additions"] - repo_stats["deletions"]

            # Per-repo per-language stats
            repo_lang_stats = ensure_lang_record(repo_stats, lang)
            repo_lang_stats["additions"] += add
            repo_lang_stats["deletions"] += dele
            repo_lang_stats["net_lines"] = (
                repo_lang_stats["additions"] - repo_lang_stats["deletions"]
            )

            # Per-author per-language stats
            author_lang_stats = ensure_lang_record(author_data, lang)
            author_lang_stats["additions"] += add
            author_lang_stats["deletions"] += dele
            author_lang_stats["net_lines"] = (
                author_lang_stats["additions"] - author_lang_stats["deletions"]
            )

            # Per-repo prod/test stats
            repo_ct_stats = ensure_code_type_record(repo_stats, code_kind)
            repo_ct_stats["additions"] += add
            repo_ct_stats["deletions"] += dele
            repo_ct_stats["net_lines"] = (
                repo_ct_stats["additions"] - repo_ct_stats["deletions"]
            )

            # Per-author prod/test stats
            author_ct_stats = ensure_code_type_record(author_data, code_kind)
            author_ct_stats["additions"] += add
            author_ct_stats["deletions"] += dele
            author_ct_stats["net_lines"] = (
                author_ct_stats["additions"] - author_ct_stats["deletions"]
            )

            # Documentation stats (when language is a doc language)
            if is_doc_language(lang):
                # Per-repo documentation
                repo_doc_stats = ensure_docs_record(repo_stats)
                repo_doc_stats["additions"] += add
                repo_doc_stats["deletions"] += dele
                repo_doc_stats["net_lines"] = (
                    repo_doc_stats["additions"] - repo_doc_stats["deletions"]
                )

                # Per-author documentation
                author_doc_stats = ensure_docs_record(author_data)
                author_doc_stats["additions"] += add
                author_doc_stats["deletions"] += dele
                author_doc_stats["net_lines"] = (
                    author_doc_stats["additions"] - author_doc_stats["deletions"]
                )

            # Per-author weekday stats: lines
            if current_weekday_name:
                wd_stats = ensure_weekday_record(author_data, current_weekday_name)
                wd_stats["additions"] += add
                wd_stats["deletions"] += dele
                wd_stats["net_lines"] = wd_stats["additions"] - wd_stats["deletions"]

            # Per-author hour-of-day stats: lines
            if current_hour_str:
                hr_stats = ensure_hour_record(author_data, current_hour_str)
                hr_stats["additions"] += add
                hr_stats["deletions"] += dele
                hr_stats["net_lines"] = hr_stats["additions"] - hr_stats["deletions"]

            # Per-author daily stats: lines
            if current_date_str:
                date_stats = ensure_date_record(author_data, current_date_str)
                date_stats["additions"] += add
                date_stats["deletions"] += dele
                date_stats["net_lines"] = date_stats["additions"] - date_stats["deletions"]

    # Done with repo


def analyze_repo_worker(repo_data: dict) -> dict:
    """Worker function to analyze a single repository. Returns author data from this repo."""
    repo_rel = repo_data["repo_rel"]
    repo_path = repo_data["repo_path"]
    date_from = repo_data["date_from"]
    date_to = repo_data["date_to"]
    
    # Local authors dict for this repo
    local_authors: Dict[AuthorKey, Dict[str, Any]] = {}
    
    # Analyze the repo
    analyze_repo(repo_rel, repo_path, date_from, date_to, local_authors)
    
    return {
        "repo_rel": repo_rel,
        "authors": local_authors
    }


def merge_author_data(target_authors: Dict[AuthorKey, Dict[str, Any]], 
                     source_authors: Dict[AuthorKey, Dict[str, Any]]) -> None:
    """Merge author data from source into target."""
    for author_key, source_data in source_authors.items():
        if author_key not in target_authors:
            target_authors[author_key] = source_data
        else:
            # Merge the data
            target_data = target_authors[author_key]
            
            # Merge basic stats
            target_data["total_commits"] += source_data.get("total_commits", 0)
            target_data["total_additions"] += source_data.get("total_additions", 0)
            target_data["total_deletions"] += source_data.get("total_deletions", 0)
            target_data["total_net_lines"] = target_data["total_additions"] - target_data["total_deletions"]
            target_data["total_changed_lines"] += source_data.get("total_changed_lines", 0)
            
            # Merge language stats
            for lang, source_lang_data in source_data.get("languages", {}).items():
                if lang not in target_data["languages"]:
                    target_data["languages"][lang] = source_lang_data.copy()
                else:
                    target_lang_data = target_data["languages"][lang]
                    target_lang_data["additions"] += source_lang_data.get("additions", 0)
                    target_lang_data["deletions"] += source_lang_data.get("deletions", 0)
                    target_lang_data["net_lines"] = target_lang_data["additions"] - target_lang_data["deletions"]
            
            # Merge prod/test/doc stats
            for category in ["production", "test", "documentation"]:
                if category in source_data:
                    if category not in target_data:
                        target_data[category] = source_data[category].copy()
                    else:
                        target_data[category]["additions"] += source_data[category].get("additions", 0)
                        target_data[category]["deletions"] += source_data[category].get("deletions", 0)
                        target_data[category]["net_lines"] = target_data[category]["additions"] - target_data[category]["deletions"]
            
            # Merge weekday stats
            for weekday, source_wd_data in source_data.get("weekdays", {}).items():
                if weekday not in target_data["weekdays"]:
                    target_data["weekdays"][weekday] = source_wd_data.copy()
                else:
                    target_wd_data = target_data["weekdays"][weekday]
                    target_wd_data["commits"] += source_wd_data.get("commits", 0)
                    target_wd_data["additions"] += source_wd_data.get("additions", 0)
                    target_wd_data["deletions"] += source_wd_data.get("deletions", 0)
                    target_wd_data["net_lines"] = target_wd_data["additions"] - target_wd_data["deletions"]
            
            # Merge hour stats
            for hour, source_hr_data in source_data.get("hours", {}).items():
                if hour not in target_data["hours"]:
                    target_data["hours"][hour] = source_hr_data.copy()
                else:
                    target_hr_data = target_data["hours"][hour]
                    target_hr_data["commits"] += source_hr_data.get("commits", 0)
                    target_hr_data["additions"] += source_hr_data.get("additions", 0)
                    target_hr_data["deletions"] += source_hr_data.get("deletions", 0)
                    target_hr_data["net_lines"] = target_hr_data["additions"] - target_hr_data["deletions"]
            
            # Merge daily stats
            for date, source_date_data in source_data.get("dates", {}).items():
                if date not in target_data["dates"]:
                    target_data["dates"][date] = source_date_data.copy()
                else:
                    target_date_data = target_data["dates"][date]
                    target_date_data["commits"] += source_date_data.get("commits", 0)
                    target_date_data["additions"] += source_date_data.get("additions", 0)
                    target_date_data["deletions"] += source_date_data.get("deletions", 0)
                    target_date_data["net_lines"] = target_date_data["additions"] - target_date_data["deletions"]


def main() -> None:
    args = parse_args()

    date_from = args.date_from
    date_to = args.date_to
    repos_root = args.repos_root
    output_root = args.output_root
    parallel = args.parallel
    max_workers = args.max_workers
    alias_file = args.alias_file
    ignore_file = args.ignore_file

    # Load configuration files
    print(f"Loading configuration...")
    alias_map = load_aliases(alias_file)
    ignored_users = load_ignored_users(ignore_file)
    
    print(f"Loaded {len(alias_map)} aliases")
    print(f"Ignoring {len(ignored_users)} users: {', '.join(sorted(ignored_users)) if ignored_users else 'none'}")
    print()

    # Basic date validation
    try:
        datetime.fromisoformat(date_from)
        datetime.fromisoformat(date_to)
    except ValueError as e:
        print(f"ERROR: Invalid date format: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Analyzing LOCAL git commits from {date_from} to {date_to}...")
    print(f"Repos root: {repos_root}")

    repo_list = discover_local_repos(repos_root)
    if not repo_list:
        print(f"No git repos found under '{repos_root}'. Nothing to do.")
        sys.exit(0)

    # Determine number of workers
    if max_workers is None:
        available_cores = multiprocessing.cpu_count()
        max_workers = min(available_cores, len(repo_list), 4)  # Cap at 4 for memory reasons

    print(f"Discovered {len(repo_list)} repos:")
    if parallel and len(repo_list) > 1:
        print(f"🚀 Processing repositories in parallel (max workers: {max_workers})")
    else:
        print("📊 Processing repositories sequentially")

    authors: Dict[AuthorKey, Dict[str, Any]] = {}

    if parallel and len(repo_list) > 1:
        # Parallel processing
        repo_tasks = []
        for repo_rel in sorted(repo_list):
            repo_path = os.path.join(repos_root, repo_rel)
            repo_tasks.append({
                "repo_rel": repo_rel,
                "repo_path": repo_path,
                "date_from": date_from,
                "date_to": date_to
            })

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo = {executor.submit(analyze_repo_worker, task): task["repo_rel"] for task in repo_tasks}
            
            for future in as_completed(future_to_repo):
                repo_rel = future_to_repo[future]
                try:
                    result = future.result()
                    print(f"  ✅ {repo_rel}")
                    # Merge results
                    merge_author_data(authors, result["authors"])
                except Exception as e:
                    print(f"  ❌ {repo_rel}: {e}")

    else:
        # Sequential processing (original behavior)
        for repo_rel in sorted(repo_list):
            repo_path = os.path.join(repos_root, repo_rel)
            print(f"  -> {repo_rel}")
            analyze_repo(repo_rel, repo_path, date_from, date_to, authors)

    if not authors:
        print("No commits found in the specified date range.")
        sys.exit(0)

    # Use alias mappings passed from main function
    if alias_map:
        print(f"\nUsing {len(alias_map)} alias mappings from {alias_file}")
    else:
        print("\nNo alias mappings loaded")

    # Build list of author entries with slugs
    author_entries = []
    for (name, email), data in authors.items():
        if data["total_commits"] == 0:
            continue

        if email:
            base = email.split("@")[0]
        else:
            base = name or "unknown-author"

        slug = slugify(base)
        author_entries.append(
            {
                "slug": slug,
                "name": name,
                "email": email,
                "data": data,
            }
        )

    # Group by canonical slug (apply aliases)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in author_entries:
        slug = entry["slug"]
        canonical_slug = alias_map.get(slug, slug)
        grouped.setdefault(canonical_slug, []).append(entry)

    # Write per-canonical-author summaries
    print("\nWriting per-author summaries (with aliases applied)...")
    skipped_count = 0
    for canonical_slug, entries in grouped.items():
        # Skip ignored users
        if canonical_slug in ignored_users:
            skipped_count += 1
            continue
        # Merge multiple entries into one
        merged = init_author_record(name="", email="")

        # Choose a display name/email: prefer the entry whose slug == canonical_slug
        display_name = ""
        display_email = ""

        for e in entries:
            d = e["data"]
            merged["total_commits"] += d["total_commits"]
            merged["total_lines_added"] += d["total_lines_added"]
            merged["total_lines_deleted"] += d["total_lines_deleted"]

            # Merge per-repo stats
            for repo_id, repo_stats in d["per_repo"].items():
                m_repo = ensure_repo_record(merged, repo_id)
                m_repo["commits"] += repo_stats["commits"]
                m_repo["additions"] += repo_stats["additions"]
                m_repo["deletions"] += repo_stats["deletions"]
                m_repo["net_lines"] = m_repo["additions"] - m_repo["deletions"]

                # Merge per-repo language stats
                for lang, lstats in repo_stats.get("languages", {}).items():
                    m_repo_lang = ensure_lang_record(m_repo, lang)
                    m_repo_lang["additions"] += lstats["additions"]
                    m_repo_lang["deletions"] += lstats["deletions"]
                    m_repo_lang["net_lines"] = (
                        m_repo_lang["additions"] - m_repo_lang["deletions"]
                    )

                # Merge per-repo prod/test stats
                for ct, cstats in repo_stats.get("code_type", {}).items():
                    m_repo_ct = ensure_code_type_record(m_repo, ct)
                    m_repo_ct["additions"] += cstats["additions"]
                    m_repo_ct["deletions"] += cstats["deletions"]
                    m_repo_ct["net_lines"] = (
                        m_repo_ct["additions"] - m_repo_ct["deletions"]
                    )

                # Merge per-repo documentation stats
                rdoc = repo_stats.get("documentation")
                if rdoc:
                    m_repo_doc = ensure_docs_record(m_repo)
                    m_repo_doc["additions"] += rdoc.get("additions", 0)
                    m_repo_doc["deletions"] += rdoc.get("deletions", 0)
                    m_repo_doc["net_lines"] = (
                        m_repo_doc["additions"] - m_repo_doc["deletions"]
                    )

            # Merge per-author language stats
            for lang, lstats in d.get("languages", {}).items():
                m_lang = ensure_lang_record(merged, lang)
                m_lang["additions"] += lstats["additions"]
                m_lang["deletions"] += lstats["deletions"]
                m_lang["net_lines"] = m_lang["additions"] - m_lang["deletions"]

            # Merge per-author prod/test stats
            for ct, cstats in d.get("code_type", {}).items():
                m_ct = ensure_code_type_record(merged, ct)
                m_ct["additions"] += cstats["additions"]
                m_ct["deletions"] += cstats["deletions"]
                m_ct["net_lines"] = (
                    m_ct["additions"] - m_ct["deletions"]
                )

            # Merge per-author documentation stats
            doc_stats = d.get("documentation")
            if doc_stats:
                m_doc = ensure_docs_record(merged)
                m_doc["additions"] += doc_stats.get("additions", 0)
                m_doc["deletions"] += doc_stats.get("deletions", 0)
                m_doc["net_lines"] = (
                    m_doc["additions"] - m_doc["deletions"]
                )

            # Merge per-author weekday stats
            for wd_name, wd_stats in d.get("per_weekday", {}).items():
                m_wd = ensure_weekday_record(merged, wd_name)
                m_wd["commits"] += wd_stats.get("commits", 0)
                m_wd["additions"] += wd_stats.get("additions", 0)
                m_wd["deletions"] += wd_stats.get("deletions", 0)
                m_wd["net_lines"] = m_wd["additions"] - m_wd["deletions"]

            # Merge per-author hour-of-day stats
            for hr_name, hr_stats in d.get("per_hour", {}).items():
                m_hr = ensure_hour_record(merged, hr_name)
                m_hr["commits"] += hr_stats.get("commits", 0)
                m_hr["additions"] += hr_stats.get("additions", 0)
                m_hr["deletions"] += hr_stats.get("deletions", 0)
                m_hr["net_lines"] = m_hr["additions"] - m_hr["deletions"]

            # Merge per-author daily stats
            for date_name, date_stats in d.get("per_date", {}).items():
                m_date = ensure_date_record(merged, date_name)
                m_date["commits"] += date_stats.get("commits", 0)
                m_date["additions"] += date_stats.get("additions", 0)
                m_date["deletions"] += date_stats.get("deletions", 0)
                m_date["net_lines"] = m_date["additions"] - m_date["deletions"]

            # Prefer canonical slug's own entry for display, else first non-empty
            if e["slug"] == canonical_slug:
                if e["name"]:
                    display_name = e["name"]
                if e["email"]:
                    display_email = e["email"]

        if not display_name and entries:
            # Fallback: first name
            display_name = entries[0]["name"] or ""
        if not display_email and entries:
            # Fallback: first email
            display_email = entries[0]["email"] or ""

        merged["name"] = display_name
        merged["email"] = display_email

        if merged["total_commits"] == 0:
            continue

        output_folder = ensure_output_folder(output_root, canonical_slug, date_from, date_to)

        summary = {
            "author_name": merged["name"],
            "author_email": merged["email"],
            "author_slug": canonical_slug,
            "from": date_from,
            "to": date_to,
            "total_commits": merged["total_commits"],
            "total_lines_added": merged["total_lines_added"],
            "total_lines_deleted": merged["total_lines_deleted"],
            "net_lines": merged["total_lines_added"] - merged["total_lines_deleted"],
            "per_repo": merged["per_repo"],
            "languages": merged["languages"],          # overall per-author language stats
            "code_type": merged["code_type"],          # overall per-author prod/test stats
            "documentation": merged["documentation"],  # overall per-author documentation stats
            "per_weekday": merged["per_weekday"],      # overall per-author weekday stats
            "per_hour": merged["per_hour"],            # overall per-author hour-of-day stats
            "per_date": merged["per_date"],            # overall per-author daily stats
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "repos_root": os.path.abspath(repos_root),
        }

        output_path = os.path.join(output_folder, "summary.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        display_label = merged["name"] or merged["email"] or canonical_slug
        print(f"  - {display_label} -> {output_path}")

    # Summary of ignored users
    if skipped_count > 0:
        print(f"\n✅ Skipped {skipped_count} ignored users")
    
    print("\n=== Done ===")


if __name__ == "__main__":
    main()

