#!/usr/bin/env python3
import argparse
import calendar
import os
import sys
import subprocess
import json
import re
import logging
import tempfile
import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
from collections import defaultdict
import importlib
import threading
from typing import Any, Optional, List, Tuple

try:
    from pagerduty_sync import sync_pagerduty_data
except Exception:  # pragma: no cover - optional dependency
    sync_pagerduty_data = None  # type: ignore

CLOC_EXCLUDE_DIRS = ".git,node_modules,.venv,__pycache__,vendor,target,build,dist"

# Setup file logging
log_file = "master_analysis.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

TOKEI_BIN = os.environ.get("TOKEI_BIN", "tokei")
_TOKEI_VERSION_CACHE: Optional[str] = None
_TOKEI_VERSION_FAILED = False
_TOKEI_NOT_FOUND_LOGGED = False


def _emit_tokei_missing_message() -> None:
    global _TOKEI_NOT_FOUND_LOGGED
    if _TOKEI_NOT_FOUND_LOGGED:
        return
    _TOKEI_NOT_FOUND_LOGGED = True
    logger.info(
        "tokei binary not found. Install it from https://github.com/XAMPPRocky/tokei "
        "or set TOKEI_BIN to the executable path."
    )


def _tokei_exclude_patterns() -> list[str]:
    entries = [entry.strip() for entry in CLOC_EXCLUDE_DIRS.split(",") if entry.strip()]
    if not entries:
        return []

    patterns: set[str] = set()
    for entry in entries:
        normalized = entry.strip().strip("/")
        if not normalized:
            continue
        patterns.add(normalized)
        patterns.add(f"{normalized}/**")
        patterns.add(f"**/{normalized}")
        patterns.add(f"**/{normalized}/**")
    return sorted(patterns)


def _run_tokei_json(target_path: str, exclude_patterns: Optional[list[str]] = None) -> tuple[Optional[dict], float]:
    cmd = [TOKEI_BIN, "--output", "json"]
    if exclude_patterns:
        for pattern in exclude_patterns:
            cmd.extend(["--exclude", pattern])
    cmd.append(target_path)
    start_time = time.perf_counter()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        _emit_tokei_missing_message()
        return None, time.perf_counter() - start_time
    except Exception as exc:
        logger.info(f"Warning: Failed to run {TOKEI_BIN} for {target_path}: {exc}")
        return None, time.perf_counter() - start_time

    elapsed = time.perf_counter() - start_time

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if stderr:
            logger.info(
                "Warning: %s returned code %s for %s (stderr: %s)",
                TOKEI_BIN,
                result.returncode,
                target_path,
                stderr,
            )
        else:
            logger.info(
                "Warning: %s returned code %s for %s",
                TOKEI_BIN,
                result.returncode,
                target_path,
            )
        return None, elapsed

    stdout = (result.stdout or "").strip()
    if not stdout:
        return None, elapsed

    try:
        return json.loads(stdout), elapsed
    except json.JSONDecodeError as exc:
        logger.info(f"Warning: Failed to parse {TOKEI_BIN} output for {target_path}: {exc}")
        return None, elapsed


def _count_files_from_tokei_total(total_data: dict) -> int:
    children = total_data.get("children")
    if not isinstance(children, dict):
        return 0
    files = set()
    for entries in children.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if name:
                files.add(name)
    return len(files)


def _get_tokei_version() -> str:
    global _TOKEI_VERSION_CACHE, _TOKEI_VERSION_FAILED
    if _TOKEI_VERSION_CACHE:
        return _TOKEI_VERSION_CACHE
    if _TOKEI_VERSION_FAILED:
        return ""

    try:
        result = subprocess.run([TOKEI_BIN, "--version"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        _TOKEI_VERSION_FAILED = True
        _emit_tokei_missing_message()
        return ""
    except Exception:
        _TOKEI_VERSION_FAILED = True
        return ""

    if result.returncode != 0:
        _TOKEI_VERSION_FAILED = True
        return ""

    version = (result.stdout or result.stderr or "").strip() or "unknown"
    _TOKEI_VERSION_CACHE = version
    return version


def _write_json_atomic(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="reposquirrel-subsystem-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
            json.dump(payload, tmp_file, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Master script to run yearly user, subsystem, and blame analysis.\n"
            "For a given year it will:\n"
            "  - Run summery.py for each month (user statistics)\n"
            "  - Run service.py for each month (unified subsystem statistics)\n"
            "  - Generate yearly summaries for users and subsystems\n"
            "  - Optionally run blame.py (full-history ownership)\n"
        )
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year for the analysis (e.g. 2025)",
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
        help="Root directory under which stats/ will be created (default: current directory)",
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
        help="Text file listing users to ignore (default: configuration/ignore_user.txt)",
    )
    parser.add_argument(
        "--alias-file",
        dest="alias_file",
        default="configuration/alias.json",
        help="JSON file mapping user aliases to canonical names (default: configuration/alias.json)",
    )
    parser.add_argument(
        "--skip-git-pull",
        action="store_true",
        help="Skip running git pull on repositories before analysis",
    )
    parser.add_argument(
        "--skip-blame",
        action="store_true",
        help="Skip running blame.py (ownership analysis) to save time",
    )
    parser.add_argument(
        "--parallel",
        dest="parallel",
        action="store_true",
        help="Enable parallel processing for improved performance",
    )
    parser.add_argument(
        "--cpu-count",
        dest="cpu_count",
        type=int,
        default=None,
        help="Number of CPU workers to base parallelism on (default: auto-detect)",
    )
    parser.add_argument(
        "--max-workers",
        dest="cpu_count",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--pagerduty-only",
        action="store_true",
        help="Skip repo analyses and only sync PagerDuty data into stats/pagerduty.",
    )
    parser.add_argument(
        "--pagerduty-lookback-days",
        type=int,
        default=365,
        help="When used with --pagerduty-only, number of days of PagerDuty incidents to fetch (default: 365).",
    )
    return parser.parse_args()


def compute_month_range(year: int, month: int) -> tuple[str, str]:
    """Return (from_date_str, to_date_str) for the given year/month."""
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")

    first_day = 1
    last_day = calendar.monthrange(year, month)[1]

    date_from = f"{year:04d}-{month:02d}-{first_day:02d}"
    date_to = f"{year:04d}-{month:02d}-{last_day:02d}"
    return date_from, date_to


def run_cmd(cmd: list[str], desc: str) -> None:
    """Run a subprocess command with some logging and error handling."""
    logger.info(f"\n=== Running: {desc} ===")
    logger.info("Command: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=False)
    except FileNotFoundError as e:
        logger.info(f"ERROR: Failed to run '{desc}': {e}")
        sys.exit(1)

    if result.returncode != 0:
        logger.info(f"ERROR: '{desc}' exited with code {result.returncode}")
        sys.exit(result.returncode)
    else:
        logger.info(f"=== Done: {desc} ===")


def _list_git_repositories(repos_root: str) -> List[Tuple[str, str]]:
    repos: List[Tuple[str, str]] = []
    if not os.path.isdir(repos_root):
        return repos
    for org_name in sorted(os.listdir(repos_root)):
        org_path = os.path.join(repos_root, org_name)
        if not os.path.isdir(org_path):
            continue
        for repo_name in sorted(os.listdir(org_path)):
            repo_path = os.path.join(org_path, repo_name)
            git_dir = os.path.join(repo_path, ".git")
            if os.path.isdir(repo_path) and os.path.isdir(git_dir):
                repos.append((f"{org_name}/{repo_name}", repo_path))
    return repos


def git_pull_repositories(repos_root: str, timeout: int = 300) -> bool:
    """Run git pull --ff-only for all repositories under repos_root."""
    repos = _list_git_repositories(repos_root)
    if not repos:
        logger.info("No git repositories found under %s; skipping git pull", repos_root)
        return False

    logger.info("\n===========================================")
    logger.info("Updating git repositories")
    logger.info("===========================================")

    success = True
    git_env = os.environ.copy()
    git_env.setdefault("GIT_TERMINAL_PROMPT", "0")
    total = len(repos)
    for idx, (repo_name, repo_path) in enumerate(repos, start=1):
        logger.info("(%s/%s) git pull %s", idx, total, repo_name)
        try:
            process = subprocess.Popen(
                ["git", "-C", repo_path, "pull", "--ff-only"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=git_env,
            )
        except FileNotFoundError:
            logger.info("ERROR: git executable not found while updating %s", repo_name)
            return False

        last_line: str = ""

        def _stream_output(pipe: Optional[Any]) -> None:
            nonlocal last_line
            if pipe is None:
                return
            for raw_line in iter(pipe.readline, ""):
                line = raw_line.rstrip()
                if not line:
                    continue
                last_line = line
                logger.info("    %s", line)
            pipe.close()

        reader_thread = threading.Thread(target=_stream_output, args=(process.stdout,))
        reader_thread.daemon = True
        reader_thread.start()

        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            success = False
            process.kill()
            logger.info(
                "WARNING: git pull timed out for %s after %s seconds (continuing with existing data)",
                repo_name,
                timeout,
            )
        finally:
            reader_thread.join()

        if timed_out:
            continue

        if process.returncode != 0:
            success = False
            if last_line:
                logger.info("    git pull failed: %s", last_line)
            else:
                logger.info("    git pull failed with exit code %s", process.returncode)
            continue

        if not last_line:
            last_line = "Already up to date."
            logger.info("    %s", last_line)

    return success

def load_services_config_file(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except Exception as exc:
        logger.info(f"Warning: Failed to load services config '{path}': {exc}")
        return {}


def run_cloc(paths: list[str]) -> dict:
    existing = [p for p in paths if p and os.path.exists(p)]
    if not existing:
        return {}

    aggregated = defaultdict(int)
    exclude_patterns = _tokei_exclude_patterns()
    for path in existing:
        data, _ = _run_tokei_json(path, exclude_patterns)
        if not data:
            continue
        for lang, info in data.items():
            if lang == "Total" or not isinstance(info, dict):
                continue
            code_lines = info.get("code")
            if code_lines is None:
                continue
            aggregated[lang] += int(code_lines)

    return dict(aggregated)


def discover_repo_candidates(repos_root: str, output_root: str, services_config: dict) -> list[str]:
    repo_names = set(services_config.keys())
    if os.path.isdir(repos_root):
        for dirpath, dirnames, _ in os.walk(repos_root):
            if ".git" in dirnames:
                rel_path = os.path.relpath(dirpath, repos_root)
                repo_names.add(rel_path)
                dirnames[:] = []
            else:
                dirnames[:] = [d for d in dirnames if not d.startswith('.')]
    stats_repos_dir = os.path.join(output_root, "stats", "repos")
    if os.path.isdir(stats_repos_dir):
        for dirpath, _, files in os.walk(stats_repos_dir):
            if "blame.json" in files:
                blame_path = os.path.join(dirpath, "blame.json")
                try:
                    with open(blame_path, "r", encoding="utf-8") as f:
                        blame_data = json.load(f)
                    repo_full_name = blame_data.get("repo")
                    if repo_full_name:
                        repo_names.add(repo_full_name)
                except Exception:
                    continue
    return sorted(repo_names)


def generate_cloc_cache(repos_root: str, output_root: str, services_file: str, max_parallel_workers: Optional[int] = None) -> None:
    logger.info("\n===========================================")
    logger.info("Generating repo/service LOC cache via tokei")
    logger.info("===========================================")
    services_config = load_services_config_file(services_file)
    stats_dir = os.path.join(output_root, "stats")
    os.makedirs(stats_dir, exist_ok=True)
    cache_path = os.path.join(stats_dir, "cloc_cache.json")

    repo_candidates = discover_repo_candidates(repos_root, output_root, services_config)
    if not repo_candidates:
        logger.info("No repositories discovered for LOC cache generation")
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({}, f)
        return

    if max_parallel_workers is not None and max_parallel_workers > 0:
        worker_count = max_parallel_workers
    else:
        worker_count = min(multiprocessing.cpu_count(), max(1, len(repo_candidates)))
    if worker_count > 1:
        logger.info(f"Processing {len(repo_candidates)} repositories in parallel (CPU workers={worker_count})")
    else:
        logger.info("Processing repositories sequentially")

    def process_repo(repo_name: str):
        repo_path = os.path.join(repos_root, repo_name)
        if not os.path.isdir(repo_path):
            logger.info(f"Skipping repo '{repo_name}' (path not found)")
            return repo_name, None
        repo_lang = run_cloc([repo_path]) or {}
        service_langs = {}
        allocated = defaultdict(int)
        for service_name, rel_paths in (services_config.get(repo_name) or {}).items():
            abs_paths = []
            for rel_path in rel_paths or []:
                if not rel_path:
                    continue
                abs_path = os.path.join(repo_path, rel_path)
                if os.path.exists(abs_path):
                    abs_paths.append(abs_path)
            if not abs_paths:
                continue
            lang_map = run_cloc(abs_paths)
            if not lang_map:
                continue
            service_langs[service_name] = lang_map
            for lang, lines in lang_map.items():
                allocated[lang] += lines
        remainder = {}
        if repo_lang:
            for lang, total in repo_lang.items():
                remainder_val = total - allocated.get(lang, 0)
                if remainder_val > 0:
                    remainder[lang] = remainder_val
        return repo_name, {
            "repo": repo_lang,
            "services": service_langs,
            "remainder": remainder,
        }

    cache = {}
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(process_repo, repo): repo for repo in repo_candidates}
            for future in as_completed(future_map):
                repo_name = future_map[future]
                try:
                    name, data = future.result()
                    if data is not None:
                        cache[name] = data
                except Exception as exc:
                    logger.info(f"Warning: Failed to process repo '{repo_name}': {exc}")
    else:
        for repo_name in repo_candidates:
            _, data = process_repo(repo_name)
            if data is not None:
                cache[repo_name] = data

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True)
    logger.info(f"LOC cache written to {cache_path} ({len(cache)} repos)")


def refresh_badge_cache_via_server() -> None:
    """Refresh badge cache by reusing dashboard_server logic."""
    try:
        dashboard_server = importlib.import_module("dashboard_server")
    except Exception as exc:
        logger.info(f"Warning: Unable to import dashboard_server for badge cache refresh: {exc}")
        return

    refresh_func = getattr(dashboard_server, "refresh_badge_cache", None)
    if not callable(refresh_func):
        logger.info("Warning: dashboard_server.refresh_badge_cache is unavailable; skipping badge cache refresh")
        return

    logger.info("\n===========================================")
    logger.info("Refreshing developer badge cache")
    logger.info("===========================================")
    try:
        data = refresh_func()
        if data and data.get("summary"):
            summary = data["summary"]
            logger.info(
                "Badge cache updated: %s users with badges, %s total badges",
                summary.get("users_with_badges", 0),
                summary.get("total_badges", 0),
            )
        else:
            logger.info("Badge cache refresh completed but produced no data. Ensure blame data exists.")
    except Exception as exc:
        logger.info(f"Warning: Exception while refreshing badge cache: {exc}")


def process_month_worker(month_data: dict) -> tuple[int, bool]:
    """Worker function to process a single month. Returns (month, success)."""
    year = month_data["year"]
    month = month_data["month"]
    date_from = month_data["date_from"]
    date_to = month_data["date_to"]
    python_exe = month_data["python_exe"]
    summery_script = month_data["summery_script"]
    service_script = month_data["service_script"]
    repos_root = month_data["repos_root"]
    output_root = month_data["output_root"]
    services_file = month_data["services_file"]
    alias_file = month_data["alias_file"]
    ignore_file = month_data["ignore_file"]
    use_parallel_repos = month_data.get("use_parallel_repos", True)  # Enable repo-level parallelization
    
    try:
        logger.info(f"\n--- Processing month: {year}-{month:02d} ({date_from} -> {date_to}) ---")
        
        # 1) Run summery.py (users) for this month
        summery_cmd = [
            python_exe,
            summery_script,
            "--from",
            date_from,
            "--to",
            date_to,
            "--repos-root",
            repos_root,
            "--output-root",
            output_root,
            "--alias-file",
            alias_file,
            "--ignore-file",
            ignore_file,
        ]
        
        if use_parallel_repos:
            summery_cmd.append("--parallel")
            
        result1 = subprocess.run(summery_cmd, check=False)
        
        if result1.returncode != 0:
            logger.info(f"ERROR: summery.py failed for {year}-{month:02d} with return code {result1.returncode}")
            return (month, False)

        # 2) Run service.py (unified subsystem analysis) for this month  
        service_cmd = [
            python_exe,
            service_script,
            "--from",
            date_from,
            "--to",
            date_to,
            "--repos-root",
            repos_root,
            "--output-root",
            output_root,
            "--services-file",
            services_file,
            "--alias-file",
            alias_file,
            "--ignore-file",
            ignore_file,
        ]
        
        if use_parallel_repos:
            service_cmd.append("--parallel")
            
        result2 = subprocess.run(service_cmd, check=False)

        if result2.returncode != 0:
            logger.info(f"ERROR: service.py failed for {year}-{month:02d} with return code {result2.returncode}")
            return (month, False)
        
        # 3) Generate team statistics for this month
        generate_team_monthly_stats(output_root, year, month)
            
        logger.info(f"✅ Completed month: {year}-{month:02d}")
        return (month, True)
        
    except Exception as e:
        logger.info(f"ERROR: Exception processing month {year}-{month:02d}: {e}")
        return (month, False)


def main() -> None:
    args = parse_args()

    year = args.year
    repos_root = args.repos_root
    output_root = args.output_root
    services_file = args.services_file
    ignore_file = args.ignore_file
    alias_file = args.alias_file
    skip_git_pull = args.skip_git_pull
    skip_blame = args.skip_blame
    parallel = args.parallel
    cpu_count = args.cpu_count
    pagerduty_only = getattr(args, "pagerduty_only", False)
    pagerduty_lookback_days = max(1, getattr(args, "pagerduty_lookback_days", 365) or 365)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    if pagerduty_only:
        if not sync_pagerduty_data:
            logger.info("PagerDuty integration is unavailable in this build.")
            sys.exit(1)
        logger.info("\n===========================================")
        logger.info("PagerDuty-only mode: syncing alerts")
        logger.info("Lookback days: %s", pagerduty_lookback_days)
        logger.info("===========================================")
        try:
            sync_pagerduty_data(
                script_dir,
                os.path.abspath(output_root),
                lookback_days=pagerduty_lookback_days,
                logger=logger,
            )
        except Exception as exc:
            logger.error("PagerDuty sync failed: %s", exc)
            sys.exit(1)
        logger.info("PagerDuty-only sync completed successfully")
        return

    if not _get_tokei_version():
        logger.info("ERROR: tokei binary is required but was not found in PATH. Set TOKEI_BIN or install tokei.")
        sys.exit(1)

    if year < 1:
        logger.info("ERROR: year must be a positive integer")
        sys.exit(1)

    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Decide how many months to run
    if year < current_year:
        first_month = 1
        last_month = 12
    elif year == current_year:
        first_month = 1
        last_month = current_month
    else:
        first_month = 1
        last_month = 12
        print(
            f"WARNING: Year {year} is in the future (relative to {current_year}). "
            "Running all 12 months; results may be empty.",
            file=sys.stderr,
        )

    # Determine number of workers
    if cpu_count is None or cpu_count <= 0:
        available_cores = multiprocessing.cpu_count()
        months_to_process = last_month - first_month + 1
        worker_cap = 8 if available_cores >= 8 else max(2, available_cores)
        max_workers = min(available_cores, months_to_process, worker_cap)
    else:
        max_workers = min(cpu_count, last_month - first_month + 1)

    logger.info("Master yearly analysis")
    logger.info("----------------------")
    logger.info(f"Year        : {year}")
    logger.info(f"Months      : {first_month:02d}..{last_month:02d}")
    logger.info(f"Repos root  : {repos_root}")
    logger.info(f"Output root : {output_root}")
    logger.info(f"Services    : {services_file}")
    logger.info(f"Alias       : {alias_file}")
    logger.info(f"Ignore      : {ignore_file}")
    if parallel:
        logger.info(f"Parallel    : Enabled (CPU workers: {max_workers})")
    else:
        logger.info(f"Parallel    : Disabled (sequential processing)")

    if skip_git_pull:
        logger.info("Git pull    : Skipped (--skip-git-pull)")
    else:
        git_pull_ok = git_pull_repositories(repos_root)
        if not git_pull_ok:
            logger.info("Git pull    : Completed with warnings or no repositories; continuing with existing clones")

    summery_script = os.path.join(script_dir, "summery.py")
    service_script = os.path.join(script_dir, "service.py")
    blame_script = os.path.join(script_dir, "blame.py")

    # Check scripts exist
    for path, name in [
        (summery_script, "summery.py"),
        (service_script, "service.py"),
        (blame_script, "blame.py"),
    ]:
        if not os.path.isfile(path):
            logger.info(f"ERROR: Required script '{name}' not found at {path}")
            sys.exit(1)

    python_exe = sys.executable or "python3"

    # Process months (either in parallel or sequentially)
    if parallel and (last_month - first_month + 1) > 1:
        logger.info(f"\n🚀 Processing {last_month - first_month + 1} months in parallel...")
        
        # Prepare month data for workers
        month_tasks = []
        for month in range(first_month, last_month + 1):
            try:
                date_from, date_to = compute_month_range(year, month)
            except ValueError as e:
                logger.info(f"ERROR: {e}")
                sys.exit(1)

            # Sanity check
            try:
                datetime.fromisoformat(date_from)
                datetime.fromisoformat(date_to)
            except ValueError as e:
                logger.info(f"ERROR: Invalid computed dates for {year}-{month:02d}: {e}")
                sys.exit(1)

            month_tasks.append({
                "year": year,
                "month": month,
                "date_from": date_from,
                "date_to": date_to,
                "python_exe": python_exe,
                "summery_script": summery_script,
                "service_script": service_script,
                "repos_root": repos_root,
                "output_root": output_root,
                "services_file": services_file,
                "alias_file": alias_file,
                "ignore_file": ignore_file,
                "use_parallel_repos": True,  # Enable repo-level parallelization when doing month-level parallelization
            })

        # Execute monthly processing in parallel
        failed_months = []
        completed_months = []
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_month = {executor.submit(process_month_worker, task): task["month"] for task in month_tasks}
            
            for future in as_completed(future_to_month):
                month, success = future.result()
                if success:
                    completed_months.append(month)
                    logger.info(f"✅ Month {year}-{month:02d} completed successfully")
                else:
                    failed_months.append(month)
                    logger.info(f"❌ Month {year}-{month:02d} failed")

        if failed_months:
            logger.info(f"\nERROR: Monthly processing failed for months: {sorted(failed_months)}")
            sys.exit(1)
            
        logger.info(f"\n✅ All {len(completed_months)} months processed successfully in parallel!")
        
    else:
        # Sequential processing (original behavior)
        logger.info(f"\n📊 Processing {last_month - first_month + 1} months sequentially...")
        for month in range(first_month, last_month + 1):
            try:
                date_from, date_to = compute_month_range(year, month)
            except ValueError as e:
                logger.info(f"ERROR: {e}")
                sys.exit(1)

            # Sanity check
            try:
                datetime.fromisoformat(date_from)
                datetime.fromisoformat(date_to)
            except ValueError as e:
                logger.info(f"ERROR: Invalid computed dates for {year}-{month:02d}: {e}")
                sys.exit(1)

            logger.info("\n-------------------------------------------")
            logger.info(f"Processing month: {year}-{month:02d}")
            logger.info(f"Date range      : {date_from} -> {date_to}")
            logger.info("-------------------------------------------")

            # 1) Run summery.py (users) for this month
            summery_cmd = [
                python_exe,
                summery_script,
                "--from",
                date_from,
                "--to",
                date_to,
                "--repos-root",
                repos_root,
                "--output-root",
                output_root,
                "--alias-file",
                alias_file,
                "--ignore-file",
                ignore_file,
            ]
            if parallel:  # Use repo-level parallelization when month-level parallelization is disabled
                summery_cmd.append("--parallel")
                
            run_cmd(
                summery_cmd,
                desc=f"summery.py for {year}-{month:02d} ({date_from}..{date_to})",
            )

            # 2) Run service.py (unified subsystem analysis) for this month
            service_cmd = [
                python_exe,
                service_script,
                "--from",
                date_from,
                "--to",
                date_to,
                "--repos-root",
                repos_root,
                "--output-root",
                output_root,
                "--services-file",
                services_file,
                "--alias-file",
                alias_file,
                "--ignore-file",
                ignore_file,
            ]
            if parallel:  # Use repo-level parallelization when month-level parallelization is disabled
                service_cmd.append("--parallel")
                
            run_cmd(
                service_cmd,
                desc=f"service.py for {year}-{month:02d} ({date_from}..{date_to})",
            )
            
            # 3) Generate team statistics for this month
            generate_team_monthly_stats(output_root, year, month)

    # After all months: create yearly summaries
    logger.info("\n===========================================")
    logger.info("Generating yearly summaries")
    logger.info("===========================================")
    
    # Create yearly summaries
    create_yearly_summaries(year, output_root, first_month, last_month)
    
    # Generate language statistics for subsystems
    logger.info("\n===========================================")
    logger.info("Generating language statistics for subsystems")
    logger.info("===========================================")
    generate_subsystem_language_stats(repos_root, output_root, services_file, max_parallel_workers=max_workers)

    # Precompute LOC evolution for all subsystems (monthly code lines per year)
    try:
        precompute_loc_evolution(year, repos_root, services_file, output_root, max_parallel_workers=max_workers)
        logger.info("Precomputed LOC evolution for subsystems")
    except Exception as e:
        logger.info(f"Warning: LOC evolution precompute failed: {e}")

    # After all months: run blame.py once (full history) - optional
    if not skip_blame:
        logger.info("\n===========================================")
        logger.info("Running blame.py (full-history ownership)")
        logger.info("===========================================")

        blame_cmd = [
            python_exe,
            blame_script,
            "--repos-root",
            repos_root,
            "--output-root",
            output_root,
            "--services-file",
            services_file,
            "--alias-file",
            alias_file,
            "--ignore-file",
            ignore_file,
        ]
        if parallel:
            blame_cmd.append("--parallel")
            
        run_cmd(
            blame_cmd,
            desc="blame.py (full history)",
        )
        refresh_badge_cache_via_server()
    else:
        logger.info("\n===========================================")
        logger.info("Skipping blame.py (--skip-blame specified)")
        logger.info("===========================================")

    logger.info("\n=== All yearly analyses completed successfully ===")
    if skip_blame:
        logger.info("Note: Ownership/blame analysis was skipped. Run without --skip-blame for complete analysis.")

    try:
        generate_cloc_cache(repos_root, output_root, services_file, max_parallel_workers=max_workers)
    except Exception as exc:
        logger.info(f"Warning: Failed to generate LOC cache: {exc}")

    try:
        precompute_subsystem_dashboard_caches(
            output_root,
            lookback_days=90,
            max_parallel_workers=max_workers,
        )
    except Exception as exc:
        logger.info(f"Warning: Failed to precompute subsystem dashboard caches: {exc}")

    if sync_pagerduty_data:
        if year == current_year:
            try:
                logger.info("\n===========================================")
                logger.info("Syncing PagerDuty alerts (last 12 months)")
                logger.info("===========================================")
                sync_pagerduty_data(
                    script_dir,
                    os.path.abspath(output_root),
                    lookback_days=365,
                    logger=logger,
                )
            except Exception as exc:
                logger.info(f"Warning: PagerDuty sync failed: {exc}")
        else:
            logger.info(
                "Skipping PagerDuty sync for year %s (will run with current year %s)",
                year,
                current_year,
            )
    
    # Note about repos directory: It's kept for blame analysis only (for badges)
    # The actual service/subsystem statistics are now in stats/subsystems/
    repos_stats_dir = os.path.join(output_root, "stats", "repos")
    if os.path.exists(repos_stats_dir):
        if skip_blame:
            logger.info("INFO: stats/repos directory exists but blame analysis was skipped.")
            logger.info("INFO: This directory is only used for blame analysis and badges.")
        else:
            logger.info("INFO: stats/repos directory contains blame analysis for badges.")
            logger.info("INFO: Main subsystem statistics are in stats/subsystems/")
    else:
        logger.info("INFO: No stats/repos directory found. Will be created by blame.py if needed.")


def generate_team_monthly_stats(output_root: str, year: int, month: int) -> None:
    """Generate team statistics for a specific month by aggregating member contributions."""
    import json
    from collections import defaultdict
    
    stats_root = os.path.join(output_root, "stats")
    teams_file = os.path.join(output_root, "configuration", "teams.json")
    
    # Load teams configuration
    if not os.path.exists(teams_file):
        logger.info(f"No teams.json found, skipping team statistics for {year}-{month:02d}")
        return
        
    with open(teams_file, "r", encoding="utf-8") as f:
        teams_config = json.load(f)
    
    if not teams_config:
        logger.info(f"No teams defined, skipping team statistics for {year}-{month:02d}")
        return
    
    date_from, date_to = compute_month_range(year, month)
    # Use YYYY-MM folder format to match user monthly summaries
    month_folder = f"{year}-{month:02d}"
    
    # Process each team (teams_config is a dict with team_id as key)
    for team_id, team_data in teams_config.items():
        team_name = team_data.get("name")
        members = team_data.get("members", [])
        
        if not team_name or not members:
            continue
        
        # Aggregate stats from all team members who have data for this month
        team_stats = {
            "team": team_name,
            "month": month_folder,
            "members": members,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "lines_changed": 0,
            "files_changed": 0,
            "subsystems": defaultdict(lambda: {"commits": 0, "lines_added": 0, "lines_deleted": 0, "lines_changed": 0}),
            "languages": defaultdict(int)
        }
        
        members_with_data = 0
        
        for member in members:
            # Load member's monthly stats (users/<member>/<YYYY-MM>/summary.json)
            member_file = os.path.join(stats_root, "users", member, month_folder, "summary.json")
            if not os.path.exists(member_file):
                continue
            
            members_with_data += 1
            
            with open(member_file, "r", encoding="utf-8") as f:
                member_data = json.load(f)
            
            # Aggregate top-level stats
            team_stats["commits"] += member_data.get("total_commits", 0)
            team_stats["lines_added"] += member_data.get("total_lines_added", 0)
            team_stats["lines_deleted"] += member_data.get("total_lines_deleted", 0)
            team_stats["lines_changed"] += abs(member_data.get("total_lines_added", 0)) + abs(member_data.get("total_lines_deleted", 0))
            
            # Aggregate per-subsystem stats (use per_repo as subsystem proxy)
            for subsystem, stats in member_data.get("per_repo", {}).items():
                team_stats["subsystems"][subsystem]["commits"] += stats.get("commits", 0)
                team_stats["subsystems"][subsystem]["lines_added"] += stats.get("additions", 0)
                team_stats["subsystems"][subsystem]["lines_deleted"] += stats.get("deletions", 0)
                team_stats["subsystems"][subsystem]["lines_changed"] += stats.get("additions", 0) + stats.get("deletions", 0)
            
            # Aggregate language stats
            for lang, lang_data in member_data.get("languages", {}).items():
                if isinstance(lang_data, dict):
                    lines = lang_data.get("additions", 0) + lang_data.get("deletions", 0)
                else:
                    lines = lang_data
                team_stats["languages"][lang] += lines
        
        # Only create team file if at least one member had data
        if members_with_data > 0:
            # Convert defaultdict to regular dict
            team_stats["subsystems"] = dict(team_stats["subsystems"])
            team_stats["languages"] = dict(team_stats["languages"])
            
            # Save team monthly stats
            team_dir = os.path.join(stats_root, "teams", team_name)
            os.makedirs(team_dir, exist_ok=True)
            
            # Save YYYY-MM.json (for API)
            team_file_short = os.path.join(team_dir, f"{month_folder}.json")
            with open(team_file_short, "w", encoding="utf-8") as f:
                json.dump(team_stats, f, indent=2, ensure_ascii=False)
            
            logger.info(f"  ✓ Generated team stats for '{team_name}' ({members_with_data}/{len(members)} members active)")


def create_yearly_summaries(year: int, output_root: str, first_month: int, last_month: int) -> None:
    """Create yearly summaries by aggregating all monthly data."""
    import json
    from collections import defaultdict
    
    stats_root = os.path.join(output_root, "stats")
    
    # Process user yearly summaries
    create_user_yearly_summaries(stats_root, year, first_month, last_month)
    
    # Process service yearly summaries  
    create_service_yearly_summaries(stats_root, year, first_month, last_month)
    
    # Process team yearly summaries
    create_team_yearly_summaries(stats_root, year, first_month, last_month)


def create_user_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly user summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    users_root = os.path.join(stats_root, "users")
    if not os.path.isdir(users_root):
        return
    
    logger.info("Creating user yearly summaries...")
    
    for user_slug in os.listdir(users_root):
        user_path = os.path.join(users_root, user_slug)
        if not os.path.isdir(user_path):
            continue
        
        # Collect all monthly data for this year
        yearly_data = aggregate_user_monthly_data(user_path, year, first_month, last_month)
        
        if yearly_data:
            # Create year folder
            year_dir = os.path.join(user_path, "year")
            os.makedirs(year_dir, exist_ok=True)
            
            # Write yearly summary
            output_path = os.path.join(year_dir, f"{year}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(yearly_data, f, indent=2)
            
            logger.info(f"  Created yearly summary for user: {user_slug} ({year})")
            
            # Generate daily stats files for this year
            generate_user_daily_stats(user_path, year, first_month, last_month)


def generate_user_daily_stats(user_path: str, year: int, first_month: int, last_month: int) -> None:
    """Generate daily stats files for a user by extracting per_date data from monthly files."""
    import json
    from collections import defaultdict
    
    # Create daily directory
    daily_dir = os.path.join(user_path, "daily")
    os.makedirs(daily_dir, exist_ok=True)
    
    # Process each month
    for month in range(first_month, last_month + 1):
        month_str = f"{year:04d}-{month:02d}"
        
        # Initialize daily data for this month
        daily_data = {}
        
        # Look for monthly data
        for entry in os.listdir(user_path):
            if not os.path.isdir(os.path.join(user_path, entry)):
                continue
            if "_" not in entry:
                continue
                
            date_from, date_to = entry.split("_", 1)
            if not date_from.startswith(month_str):
                continue
                
            monthly_file = os.path.join(user_path, entry, "summary.json")
            if not os.path.isfile(monthly_file):
                continue
                
            try:
                with open(monthly_file, "r", encoding="utf-8") as f:
                    monthly_data = json.load(f)
                    
                # Extract per_date data
                if "per_date" in monthly_data:
                    for date_str, date_data in monthly_data["per_date"].items():
                        if date_str not in daily_data:
                            daily_data[date_str] = {
                                "total_commits": 0,
                                "additions": 0,
                                "deletions": 0,
                                "net_lines": 0
                            }
                        
                        daily_data[date_str]["total_commits"] += date_data.get("commits", 0)
                        daily_data[date_str]["additions"] += date_data.get("additions", 0)
                        daily_data[date_str]["deletions"] += date_data.get("deletions", 0)
                        daily_data[date_str]["net_lines"] += date_data.get("net_lines", 0)
                        
            except (json.JSONDecodeError, IOError) as e:
                continue
        
        # Write daily stats file for this month
        if daily_data:
            output_file = os.path.join(daily_dir, f"{month_str}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(daily_data, f, indent=2)


def create_repo_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly repo summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    repos_root = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_root):
        return
    
    logger.info("Creating repo yearly summaries...")
    
    # Find all repos that have monthly data
    for root, dirs, files in os.walk(repos_root):
        if "summary.json" in files:
            rel_path = os.path.relpath(root, repos_root)
            parts = rel_path.split(os.sep)
            if len(parts) < 2:
                continue
            
            repo_rel = os.path.join(*parts[:-1]).replace(os.sep, "/")
            folder = parts[-1]
            
            # Skip if this is already a yearly folder
            if folder.startswith(f"{year:04d}-01-01"):
                continue
            
            # Process this repo if we haven't already
            repo_path = os.path.join(repos_root, *parts[:-1])
            if not hasattr(create_repo_yearly_summaries, '_processed_repos'):
                create_repo_yearly_summaries._processed_repos = set()
            
            if repo_rel not in create_repo_yearly_summaries._processed_repos:
                create_repo_yearly_summaries._processed_repos.add(repo_rel)
                
                # Aggregate monthly data for this repo
                yearly_data = aggregate_repo_monthly_data(repo_path, year, first_month, last_month)
                
                if yearly_data:
                    # Create yearly folder
                    yearly_folder = f"{year:04d}-01-01_{year:04d}-12-31"
                    yearly_dir = os.path.join(repo_path, yearly_folder)
                    os.makedirs(yearly_dir, exist_ok=True)
                    
                    # Write yearly summary
                    output_path = os.path.join(yearly_dir, "summary.json")
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(yearly_data, f, indent=2)
                    
                    logger.info(f"  Created yearly summary for repo: {repo_rel}")


def aggregate_user_monthly_data(user_path: str, year: int, first_month: int, last_month: int) -> dict:
    """Aggregate monthly user data into yearly summary."""
    import json
    from collections import defaultdict
    
    yearly_data = {
        "author_name": "",
        "author_email": "",
        "author_slug": "",
        "from": f"{year:04d}-01-01",
        "to": f"{year:04d}-12-31",
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "net_lines": 0,
        "per_repo": defaultdict(lambda: {
            "commits": 0,
            "additions": 0,
            "deletions": 0,
            "net_lines": 0,
            "languages": defaultdict(lambda: {"additions": 0, "deletions": 0, "net_lines": 0}),
            "code_type": defaultdict(lambda: {"additions": 0, "deletions": 0, "net_lines": 0}),
            "documentation": {"additions": 0, "deletions": 0, "net_lines": 0}
        }),
        "languages": defaultdict(lambda: {"additions": 0, "deletions": 0, "net_lines": 0}),
        "code_type": defaultdict(lambda: {"additions": 0, "deletions": 0, "net_lines": 0}),
        "documentation": {"additions": 0, "deletions": 0, "net_lines": 0},
        "per_weekday": defaultdict(lambda: {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}),
        "per_hour": defaultdict(lambda: {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}),
        "per_date": defaultdict(lambda: {"commits": 0, "additions": 0, "deletions": 0, "net_lines": 0}),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "repos_root": ""
    }
    
    monthly_files_found = 0
    
    for month in range(first_month, last_month + 1):
        # Look for monthly data folder named YYYY-MM
        month_folder = f"{year:04d}-{month:02d}"
        monthly_file = os.path.join(user_path, month_folder, "summary.json")
        
        if not os.path.isfile(monthly_file):
            continue
        
        try:
            with open(monthly_file, "r", encoding="utf-8") as f:
                monthly_data = json.load(f)
                
                monthly_files_found += 1
                
                # Copy basic info from first monthly file
                if not yearly_data["author_name"]:
                    yearly_data["author_name"] = monthly_data.get("author_name", "")
                    yearly_data["author_email"] = monthly_data.get("author_email", "")
                    yearly_data["author_slug"] = monthly_data.get("author_slug", "")
                    yearly_data["repos_root"] = monthly_data.get("repos_root", "")
                
                # Aggregate totals
                yearly_data["total_commits"] += monthly_data.get("total_commits", 0)
                yearly_data["total_lines_added"] += monthly_data.get("total_lines_added", 0)
                yearly_data["total_lines_deleted"] += monthly_data.get("total_lines_deleted", 0)
                yearly_data["net_lines"] += monthly_data.get("net_lines", 0)
                
                # Aggregate per-repo data
                for repo_name, repo_data in monthly_data.get("per_repo", {}).items():
                    repo_yearly = yearly_data["per_repo"][repo_name]
                    repo_yearly["commits"] += repo_data.get("commits", 0)
                    repo_yearly["additions"] += repo_data.get("additions", 0)
                    repo_yearly["deletions"] += repo_data.get("deletions", 0)
                    repo_yearly["net_lines"] += repo_data.get("net_lines", 0)
                    
                    # Languages
                    for lang, lang_data in repo_data.get("languages", {}).items():
                        lang_yearly = repo_yearly["languages"][lang]
                        lang_yearly["additions"] += lang_data.get("additions", 0)
                        lang_yearly["deletions"] += lang_data.get("deletions", 0)
                        lang_yearly["net_lines"] += lang_data.get("net_lines", 0)
                    
                    # Code types
                    for code_type, type_data in repo_data.get("code_type", {}).items():
                        type_yearly = repo_yearly["code_type"][code_type]
                        type_yearly["additions"] += type_data.get("additions", 0)
                        type_yearly["deletions"] += type_data.get("deletions", 0)
                        type_yearly["net_lines"] += type_data.get("net_lines", 0)
                    
                    # Documentation
                    doc_data = repo_data.get("documentation", {})
                    repo_yearly["documentation"]["additions"] += doc_data.get("additions", 0)
                    repo_yearly["documentation"]["deletions"] += doc_data.get("deletions", 0)
                    repo_yearly["documentation"]["net_lines"] += doc_data.get("net_lines", 0)
                
                # Aggregate global languages
                for lang, lang_data in monthly_data.get("languages", {}).items():
                    lang_yearly = yearly_data["languages"][lang]
                    lang_yearly["additions"] += lang_data.get("additions", 0)
                    lang_yearly["deletions"] += lang_data.get("deletions", 0)
                    lang_yearly["net_lines"] += lang_data.get("net_lines", 0)
                
                # Aggregate global code types
                for code_type, type_data in monthly_data.get("code_type", {}).items():
                    type_yearly = yearly_data["code_type"][code_type]
                    type_yearly["additions"] += type_data.get("additions", 0)
                    type_yearly["deletions"] += type_data.get("deletions", 0)
                    type_yearly["net_lines"] += type_data.get("net_lines", 0)
                
                # Aggregate global documentation
                doc_data = monthly_data.get("documentation", {})
                yearly_data["documentation"]["additions"] += doc_data.get("additions", 0)
                yearly_data["documentation"]["deletions"] += doc_data.get("deletions", 0)
                yearly_data["documentation"]["net_lines"] += doc_data.get("net_lines", 0)
                
                # Aggregate weekday stats
                for day, day_data in monthly_data.get("per_weekday", {}).items():
                    day_yearly = yearly_data["per_weekday"][day]
                    day_yearly["commits"] += day_data.get("commits", 0)
                    day_yearly["additions"] += day_data.get("additions", 0)
                    day_yearly["deletions"] += day_data.get("deletions", 0)
                    day_yearly["net_lines"] += day_data.get("net_lines", 0)
                
                # Aggregate hour stats
                for hour, hour_data in monthly_data.get("per_hour", {}).items():
                    hour_yearly = yearly_data["per_hour"][hour]
                    hour_yearly["commits"] += hour_data.get("commits", 0)
                    hour_yearly["additions"] += hour_data.get("additions", 0)
                    hour_yearly["deletions"] += hour_data.get("deletions", 0)
                    hour_yearly["net_lines"] += hour_data.get("net_lines", 0)
                
                # Aggregate daily stats
                for date, date_data in monthly_data.get("per_date", {}).items():
                    date_yearly = yearly_data["per_date"][date]
                    date_yearly["commits"] += date_data.get("commits", 0)
                    date_yearly["additions"] += date_data.get("additions", 0)
                    date_yearly["deletions"] += date_data.get("deletions", 0)
                    date_yearly["net_lines"] += date_data.get("net_lines", 0)
            
        except (json.JSONDecodeError, IOError) as e:
            logger.info(f"  Warning: Failed to read {monthly_file}: {e}")
            continue
    
    if monthly_files_found == 0:
        return None
    
    # Convert defaultdicts to regular dicts
    yearly_data["per_repo"] = {k: dict(v) for k, v in yearly_data["per_repo"].items()}
    for repo_data in yearly_data["per_repo"].values():
        repo_data["languages"] = dict(repo_data["languages"])
        repo_data["code_type"] = dict(repo_data["code_type"])
    
    yearly_data["languages"] = dict(yearly_data["languages"])
    yearly_data["code_type"] = dict(yearly_data["code_type"])
    yearly_data["per_weekday"] = dict(yearly_data["per_weekday"])
    yearly_data["per_hour"] = dict(yearly_data["per_hour"])
    yearly_data["per_date"] = dict(yearly_data["per_date"])
    
    return yearly_data


def aggregate_repo_monthly_data(repo_path: str, year: int, first_month: int, last_month: int) -> dict:
    """Aggregate monthly repo data into yearly summary."""
    import json
    from collections import defaultdict
    
    yearly_data = {
        "repo": "",
        "from": f"{year:04d}-01-01",
        "to": f"{year:04d}-12-31",
        "services": defaultdict(lambda: {
            "developers": defaultdict(lambda: {
                "slug": "",
                "display_name": "",
                "emails": [],
                "commits": 0,
                "lines_added": 0,
                "lines_deleted": 0,
                "net_lines": 0,
                "changed_lines": 0
            })
        }),
        "developers": defaultdict(lambda: {
            "slug": "",
            "display_name": "",
            "emails": [],
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0
        }),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "repos_root": ""
    }
    
    monthly_files_found = 0
    
    for month in range(first_month, last_month + 1):
        # Look for monthly data
        for entry in os.listdir(repo_path):
            if not os.path.isdir(os.path.join(repo_path, entry)):
                continue
            if "_" not in entry:
                continue
                
            date_from, date_to = entry.split("_", 1)
            if not date_from.startswith(f"{year:04d}-{month:02d}"):
                continue
                
            monthly_file = os.path.join(repo_path, entry, "summary.json")
            if not os.path.isfile(monthly_file):
                continue
                
            try:
                with open(monthly_file, "r", encoding="utf-8") as f:
                    monthly_data = json.load(f)
                
                monthly_files_found += 1
                
                # Copy basic info from first monthly file
                if not yearly_data["repo"]:
                    yearly_data["repo"] = monthly_data.get("repo", "")
                    yearly_data["repos_root"] = monthly_data.get("repos_root", "")
                
                # Aggregate service developers
                for service_name, service_data in monthly_data.get("services", {}).items():
                    service_yearly = yearly_data["services"][service_name]
                    
                    for dev_slug, dev_data in service_data.get("developers", {}).items():
                        dev_yearly = service_yearly["developers"][dev_slug]
                        
                        if not dev_yearly["slug"]:
                            dev_yearly["slug"] = dev_data.get("slug", "")
                            dev_yearly["display_name"] = dev_data.get("display_name", "")
                            dev_yearly["emails"] = list(set(dev_yearly["emails"] + dev_data.get("emails", [])))
                        
                        dev_yearly["commits"] += dev_data.get("commits", 0)
                        dev_yearly["lines_added"] += dev_data.get("lines_added", 0)
                        dev_yearly["lines_deleted"] += dev_data.get("lines_deleted", 0)
                        dev_yearly["net_lines"] += dev_data.get("net_lines", 0)
                        dev_yearly["changed_lines"] += dev_data.get("changed_lines", 0)
                
                # Aggregate global developers
                for dev_slug, dev_data in monthly_data.get("developers", {}).items():
                    dev_yearly = yearly_data["developers"][dev_slug]
                    
                    if not dev_yearly["slug"]:
                        dev_yearly["slug"] = dev_data.get("slug", "")
                        dev_yearly["display_name"] = dev_data.get("display_name", "")
                        dev_yearly["emails"] = list(set(dev_yearly["emails"] + dev_data.get("emails", [])))
                    
                    dev_yearly["commits"] += dev_data.get("commits", 0)
                    dev_yearly["lines_added"] += dev_data.get("lines_added", 0)
                    dev_yearly["lines_deleted"] += dev_data.get("lines_deleted", 0)
                    dev_yearly["net_lines"] += dev_data.get("net_lines", 0)
                    dev_yearly["changed_lines"] += dev_data.get("changed_lines", 0)
                
            except (json.JSONDecodeError, IOError) as e:
                logger.info(f"  Warning: Failed to read {monthly_file}: {e}")
                continue
    
    if monthly_files_found == 0:
        return None
    
    # Calculate top developers for services
    for service_name, service_data in yearly_data["services"].items():
        if service_data["developers"]:
            top_dev = max(service_data["developers"].values(), 
                         key=lambda d: d["changed_lines"])
            service_data["top_developer"] = {
                "slug": top_dev["slug"],
                "display_name": top_dev["display_name"],
                "changed_lines": top_dev["changed_lines"],
                "commits": top_dev["commits"]
            }
    
    # Calculate top developer for repo
    if yearly_data["developers"]:
        top_dev = max(yearly_data["developers"].values(), 
                     key=lambda d: d["changed_lines"])
        yearly_data["top_developer"] = {
            "slug": top_dev["slug"],
            "display_name": top_dev["display_name"],
            "changed_lines": top_dev["changed_lines"],
            "commits": top_dev["commits"]
        }
    
    # Convert defaultdicts to regular dicts
    yearly_data["services"] = {k: dict(v) for k, v in yearly_data["services"].items()}
    for service_data in yearly_data["services"].values():
        service_data["developers"] = dict(service_data["developers"])
    
    yearly_data["developers"] = dict(yearly_data["developers"])
    
    return yearly_data


def create_service_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly service summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    subsystems_root = os.path.join(stats_root, "subsystems")
    if not os.path.isdir(subsystems_root):
        return
    
    logger.info("Creating service yearly summaries...")
    
    # Find all services that have monthly data
    for service_name in os.listdir(subsystems_root):
        service_path = os.path.join(subsystems_root, service_name)
        if not os.path.isdir(service_path):
            continue
        
        # Aggregate monthly data for this service
        yearly_data = aggregate_service_monthly_data(service_path, year, first_month, last_month)
        
        if yearly_data:
            # Create yearly folder
            yearly_folder = f"{year:04d}-01-01_{year:04d}-12-31"
            yearly_dir = os.path.join(service_path, yearly_folder)
            os.makedirs(yearly_dir, exist_ok=True)
            
            # Write yearly summary
            output_path = os.path.join(yearly_dir, "summary.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(yearly_data, f, indent=2)
            
            logger.info(f"  Created yearly summary for service: {service_name}")


def aggregate_service_monthly_data(service_path: str, year: int, first_month: int, last_month: int) -> dict:
    """Aggregate monthly service data into yearly summary."""
    import json
    from collections import defaultdict
    
    yearly_data = {
        "service": "",
        "from": f"{year:04d}-01-01",
        "to": f"{year:04d}-12-31",
        "repositories": defaultdict(lambda: {
            "repo": "",
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0,
            "developers": defaultdict(lambda: {
                "slug": "",
                "display_name": "",
                "commits": 0,
                "lines_added": 0,
                "lines_deleted": 0,
                "net_lines": 0,
                "changed_lines": 0
            })
        }),
        "developers": defaultdict(lambda: {
            "slug": "",
            "display_name": "",
            "emails": [],
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "net_lines": 0,
            "changed_lines": 0,
            "repositories": defaultdict(lambda: {
                "commits": 0,
                "lines_added": 0,
                "lines_deleted": 0,
                "net_lines": 0,
                "changed_lines": 0
            })
        }),
        "top_developer": {},
        "total_commits": 0,
        "total_lines_added": 0,
        "total_lines_deleted": 0,
        "total_changed_lines": 0,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    
    monthly_files_found = 0
    
    for month in range(first_month, last_month + 1):
        # Look for monthly data
        for entry in os.listdir(service_path):
            if not os.path.isdir(os.path.join(service_path, entry)):
                continue
            if "_" not in entry:
                continue
                
            date_from, date_to = entry.split("_", 1)
            if not date_from.startswith(f"{year:04d}-{month:02d}"):
                continue
                
            monthly_file = os.path.join(service_path, entry, "summary.json")
            if not os.path.isfile(monthly_file):
                continue
                
            try:
                with open(monthly_file, "r", encoding="utf-8") as f:
                    monthly_data = json.load(f)
                
                monthly_files_found += 1
                
                # Copy basic info from first monthly file
                if not yearly_data["service"]:
                    yearly_data["service"] = monthly_data.get("service", "")
                
                # Aggregate totals
                yearly_data["total_commits"] += monthly_data.get("total_commits", 0)
                yearly_data["total_lines_added"] += monthly_data.get("total_lines_added", 0)
                yearly_data["total_lines_deleted"] += monthly_data.get("total_lines_deleted", 0)
                yearly_data["total_changed_lines"] += monthly_data.get("total_changed_lines", 0)
                
                # Aggregate repository data
                for repo_name, repo_data in monthly_data.get("repositories", {}).items():
                    repo_yearly = yearly_data["repositories"][repo_name]
                    if not repo_yearly["repo"]:
                        repo_yearly["repo"] = repo_data.get("repo", repo_name)
                    
                    repo_yearly["commits"] += repo_data.get("commits", 0)
                    repo_yearly["lines_added"] += repo_data.get("lines_added", 0)
                    repo_yearly["lines_deleted"] += repo_data.get("lines_deleted", 0)
                    repo_yearly["net_lines"] += repo_data.get("net_lines", 0)
                    repo_yearly["changed_lines"] += repo_data.get("changed_lines", 0)
                    
                    # Aggregate repo developers
                    for dev_slug, dev_data in repo_data.get("developers", {}).items():
                        repo_dev_yearly = repo_yearly["developers"][dev_slug]
                        if not repo_dev_yearly["slug"]:
                            repo_dev_yearly["slug"] = dev_data.get("slug", dev_slug)
                            repo_dev_yearly["display_name"] = dev_data.get("display_name", "")
                        
                        repo_dev_yearly["commits"] += dev_data.get("commits", 0)
                        repo_dev_yearly["lines_added"] += dev_data.get("lines_added", 0)
                        repo_dev_yearly["lines_deleted"] += dev_data.get("lines_deleted", 0)
                        repo_dev_yearly["net_lines"] += dev_data.get("net_lines", 0)
                        repo_dev_yearly["changed_lines"] += dev_data.get("changed_lines", 0)
                
                # Aggregate global developers
                for dev_slug, dev_data in monthly_data.get("developers", {}).items():
                    dev_yearly = yearly_data["developers"][dev_slug]
                    
                    if not dev_yearly["slug"]:
                        dev_yearly["slug"] = dev_data.get("slug", dev_slug)
                        dev_yearly["display_name"] = dev_data.get("display_name", "")
                        dev_yearly["emails"] = list(set(dev_yearly["emails"] + dev_data.get("emails", [])))
                    else:
                        # Merge emails
                        new_emails = dev_data.get("emails", [])
                        dev_yearly["emails"] = list(set(dev_yearly["emails"] + new_emails))
                    
                    dev_yearly["commits"] += dev_data.get("commits", 0)
                    dev_yearly["lines_added"] += dev_data.get("lines_added", 0)
                    dev_yearly["lines_deleted"] += dev_data.get("lines_deleted", 0)
                    dev_yearly["net_lines"] += dev_data.get("net_lines", 0)
                    dev_yearly["changed_lines"] += dev_data.get("changed_lines", 0)
                    
                    # Aggregate developer repositories
                    for repo_name, repo_data in dev_data.get("repositories", {}).items():
                        dev_repo_yearly = dev_yearly["repositories"][repo_name]
                        dev_repo_yearly["commits"] += repo_data.get("commits", 0)
                        dev_repo_yearly["lines_added"] += repo_data.get("lines_added", 0)
                        dev_repo_yearly["lines_deleted"] += repo_data.get("lines_deleted", 0)
                        dev_repo_yearly["net_lines"] += repo_data.get("net_lines", 0)
                        dev_repo_yearly["changed_lines"] += repo_data.get("changed_lines", 0)
                
            except (json.JSONDecodeError, IOError) as e:
                logger.info(f"  Warning: Failed to read {monthly_file}: {e}")
                continue
    
    if monthly_files_found == 0:
        return None
    
    # Calculate top developer
    if yearly_data["developers"]:
        top_dev = max(yearly_data["developers"].values(), 
                     key=lambda d: d["changed_lines"])
        yearly_data["top_developer"] = {
            "slug": top_dev["slug"],
            "display_name": top_dev["display_name"],
            "changed_lines": top_dev["changed_lines"],
            "commits": top_dev["commits"]
        }
    
    # Convert defaultdicts to regular dicts
    yearly_data["repositories"] = {k: dict(v) for k, v in yearly_data["repositories"].items()}
    for repo_data in yearly_data["repositories"].values():
        repo_data["developers"] = dict(repo_data["developers"])
    
    yearly_data["developers"] = {k: dict(v) for k, v in yearly_data["developers"].items()}
    for dev_data in yearly_data["developers"].values():
        dev_data["repositories"] = dict(dev_data["repositories"])
    
    return yearly_data


def create_team_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly team summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    teams_root = os.path.join(stats_root, "teams")
    if not os.path.isdir(teams_root):
        logger.info("No teams directory found, skipping team yearly summaries")
        return
    
    # Load team responsibilities
    responsibilities = {}
    responsibilities_file = os.path.join("configuration", "team_subsystem_responsibilities.json")
    if os.path.isfile(responsibilities_file):
        try:
            with open(responsibilities_file, "r", encoding="utf-8") as f:
                responsibilities = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    # Load teams config to map team names to IDs
    teams_config = {}
    teams_file = os.path.join("configuration", "teams.json")
    if os.path.isfile(teams_file):
        try:
            with open(teams_file, "r", encoding="utf-8") as f:
                teams_config = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    # Create reverse mapping: team_name -> team_id
    team_name_to_id = {}
    for team_id, team_info in teams_config.items():
        team_name_to_id[team_info.get("name", team_id)] = team_id
    
    logger.info("Creating team yearly summaries...")
    
    # Find all teams that have monthly data
    for team_name in os.listdir(teams_root):
        team_path = os.path.join(teams_root, team_name)
        if not os.path.isdir(team_path):
            continue
        
        # Aggregate monthly data for this team
        yearly_data = {
            "team": team_name,
            "year": year,
            "commits": 0,
            "lines_added": 0,
            "lines_deleted": 0,
            "lines_changed": 0,
            "files_changed": 0,
            "subsystems": defaultdict(lambda: {"commits": 0, "lines_added": 0, "lines_deleted": 0, "lines_changed": 0}),
            "members": set(),
            "languages": defaultdict(int)
        }
        
        monthly_files_found = 0
        
        for month in range(first_month, last_month + 1):
            month_str = f"{year}-{month:02d}"
            monthly_file = os.path.join(team_path, f"{month_str}.json")
            
            if not os.path.isfile(monthly_file):
                continue
            
            try:
                with open(monthly_file, "r", encoding="utf-8") as f:
                    monthly_data = json.load(f)
                
                monthly_files_found += 1
                
                # Aggregate totals
                yearly_data["commits"] += monthly_data.get("commits", 0)
                yearly_data["lines_added"] += monthly_data.get("lines_added", 0)
                yearly_data["lines_deleted"] += monthly_data.get("lines_deleted", 0)
                yearly_data["lines_changed"] += monthly_data.get("lines_changed", 0)
                yearly_data["files_changed"] += monthly_data.get("files_changed", 0)
                
                # Collect all members
                yearly_data["members"].update(monthly_data.get("members", []))
                
                # Aggregate subsystem data
                for subsystem, stats in monthly_data.get("subsystems", {}).items():
                    yearly_data["subsystems"][subsystem]["commits"] += stats.get("commits", 0)
                    yearly_data["subsystems"][subsystem]["lines_added"] += stats.get("lines_added", 0)
                    yearly_data["subsystems"][subsystem]["lines_deleted"] += stats.get("lines_deleted", 0)
                    yearly_data["subsystems"][subsystem]["lines_changed"] += stats.get("lines_changed", 0)
                
                # Aggregate language data
                for lang, lang_data in monthly_data.get("languages", {}).items():
                    # Handle both dict format (with additions/deletions) and simple int format
                    if isinstance(lang_data, dict):
                        lines = lang_data.get("additions", 0) + lang_data.get("deletions", 0)
                    else:
                        lines = lang_data
                    
                    # Ensure yearly_data["languages"][lang] is always an int
                    current_value = yearly_data["languages"].get(lang, 0)
                    if isinstance(current_value, dict):
                        current_value = current_value.get("additions", 0) + current_value.get("deletions", 0)
                    yearly_data["languages"][lang] = current_value + lines
            
            except (json.JSONDecodeError, IOError) as e:
                logger.info(f"Warning: Could not read {monthly_file}: {e}")
                continue
        
        if monthly_files_found > 0:
            # Convert sets and defaultdicts to regular types
            yearly_data["members"] = sorted(list(yearly_data["members"]))
            yearly_data["subsystems"] = dict(yearly_data["subsystems"])
            yearly_data["languages"] = dict(yearly_data["languages"])
            
            # Add responsible subsystems
            team_id = team_name_to_id.get(team_name, team_name.lower().replace(" ", "-"))
            yearly_data["responsible_subsystems"] = responsibilities.get(team_id, [])
            
            # Calculate responsible subsystem details
            responsible_subsystem_details = {}
            total_responsible_lines = 0
            
            for subsystem_name in yearly_data["responsible_subsystems"]:
                subsystem_lang_path = os.path.join(stats_root, "subsystems", subsystem_name, "languages.json")
                if os.path.isfile(subsystem_lang_path):
                    try:
                        with open(subsystem_lang_path, "r", encoding="utf-8") as f:
                            lang_data = json.load(f)
                            subsystem_lines = 0
                            subsystem_languages = {}
                            for lang_name, lang_info in lang_data.get("languages", {}).items():
                                if isinstance(lang_info, dict):
                                    code_lines = lang_info.get("code_lines", 0)
                                    subsystem_lines += code_lines
                                    subsystem_languages[lang_name] = code_lines
                            
                            responsible_subsystem_details[subsystem_name] = {
                                "name": subsystem_name,
                                "lines": subsystem_lines,
                                "languages": subsystem_languages
                            }
                            total_responsible_lines += subsystem_lines
                    except (json.JSONDecodeError, IOError):
                        pass
            
            yearly_data["responsible_subsystem_details"] = responsible_subsystem_details
            yearly_data["total_responsible_lines"] = total_responsible_lines
            
            # Save yearly summary
            yearly_file = os.path.join(team_path, f"{year}.json")
            with open(yearly_file, "w", encoding="utf-8") as f:
                json.dump(yearly_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"  Created yearly summary for team: {team_name} ({monthly_files_found} months)")


def generate_subsystem_language_stats(repos_root: str, output_root: str, services_file: str, max_parallel_workers: Optional[int] = None) -> None:
    """Generate language statistics for each subsystem using tokei."""

    services_config: dict = {}
    if os.path.isfile(services_file):
        try:
            with open(services_file, "r", encoding="utf-8") as f:
                services_config = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.info(f"Warning: Error loading services file {services_file}: {e}")
    else:
        logger.info(f"Services file {services_file} not found, will only process standalone repositories")

    stats_root = os.path.join(output_root, "stats")
    subsystems_stats_root = os.path.join(stats_root, "subsystems")

    if not _get_tokei_version():
        logger.info("tokei not found. Please install tokei to generate language statistics.")
        logger.info("See https://github.com/XAMPPRocky/tokei for installation instructions or set TOKEI_BIN to the binary path.")
        return

    logger.info("Generating language statistics for subsystems...")

    repo_service_paths: dict[str, set[str]] = defaultdict(set)
    subsystem_repos: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for repo_name, services in services_config.items():
        for service_name, raw_paths in services.items():
            clean_paths = [p.rstrip("/") for p in (raw_paths or []) if p]
            subsystem_repos[service_name].append(
                {
                    "repo": repo_name,
                    "paths": clean_paths or [""],
                    "exclude_paths": [],
                }
            )
            for path in clean_paths:
                repo_service_paths[repo_name].add(path)

    repos_root_abs = os.path.abspath(repos_root)
    if os.path.exists(repos_root_abs):
        logger.info("  Discovering repository-backed subsystems...")
        for org_dir in os.listdir(repos_root_abs):
            org_path = os.path.join(repos_root_abs, org_dir)
            if not os.path.isdir(org_path):
                continue

            for repo_dir in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_dir)
                git_dir = os.path.join(repo_path, ".git")
                if not os.path.isdir(git_dir):
                    continue

                repo_name = f"{org_dir}/{repo_dir}"
                subsystem_name = repo_dir
                exclude_paths = sorted(repo_service_paths.get(repo_name, set()))

                # Ensure each repository is also treated as its own subsystem (the remainder)
                existing_specs = subsystem_repos[subsystem_name]
                already_present = any(spec.get("repo") == repo_name and spec.get("paths") == [""] for spec in existing_specs)
                if not already_present:
                    subsystem_repos[subsystem_name].append(
                        {
                            "repo": repo_name,
                            "paths": [""],
                            "exclude_paths": exclude_paths,
                        }
                    )

    subsystem_items = sorted(subsystem_repos.items())
    if not subsystem_items:
        logger.info("  No subsystems discovered for language statistics.")
        return

    if max_parallel_workers is not None and max_parallel_workers > 0:
        max_workers = max_parallel_workers
    else:
        max_workers = max(1, multiprocessing.cpu_count())
    use_parallel = len(subsystem_items) > 1 and max_workers > 1
    if use_parallel:
        logger.info(f"  Processing {len(subsystem_items)} subsystems in parallel (CPU workers: {max_workers})")
    else:
        logger.info("  Processing subsystems sequentially")

    def process_subsystem(subsystem_name: str, repo_specs: list[dict[str, Any]]) -> bool:
        try:
            logger.info(f"  Processing subsystem: {subsystem_name}")
            subsystem_dir = os.path.join(subsystems_stats_root, subsystem_name)
            os.makedirs(subsystem_dir, exist_ok=True)

            aggregate = _init_language_stats()
            produced = False

            for spec in repo_specs:
                repo_name = spec.get("repo")
                include_paths = spec.get("paths") or [""]
                exclude_paths = spec.get("exclude_paths") or []
                stats = _compute_repo_language_stats(repos_root, repo_name, include_paths, exclude_paths)
                if not stats:
                    continue
                _merge_language_stats(aggregate, stats)
                produced = True

            if not produced:
                logger.info(f"    No valid paths found for subsystem {subsystem_name}, skipping...")
                return False

            aggregate["generated_at"] = datetime.utcnow().isoformat() + "Z"
            languages_file = os.path.join(subsystem_dir, "languages.json")
            with open(languages_file, "w", encoding="utf-8") as f:
                json.dump(aggregate, f, indent=2)
            logger.info(f"    Generated language stats: {languages_file}")
            return True
        except Exception as e:
            logger.info(f"    Error generating language stats for {subsystem_name}: {e}")
            return False

    if use_parallel:
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for subsystem_name, repo_specs in subsystem_items:
                futures[executor.submit(process_subsystem, subsystem_name, repo_specs)] = subsystem_name
            for future in as_completed(futures):
                subsystem_name = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.info(f"    Unexpected error for subsystem {subsystem_name}: {exc}")
    else:
        for subsystem_name, repo_specs in subsystem_items:
            process_subsystem(subsystem_name, repo_specs)


def _init_language_stats() -> dict:
    return {
        "generated_at": "",
        "tokei_version": _get_tokei_version() or "unknown",
        "elapsed_seconds": 0.0,
        "languages": {},
        "totals": {
            "files": 0,
            "blank_lines": 0,
            "comment_lines": 0,
            "code_lines": 0,
        },
    }


def _merge_language_stats(target: dict, addition: dict) -> None:
    if not addition:
        return
    target["elapsed_seconds"] += addition.get("elapsed_seconds", 0.0)
    if addition.get("tokei_version"):
        target["tokei_version"] = addition["tokei_version"]

    lang_keys = ("files", "blank_lines", "comment_lines", "code_lines")
    for lang, stats in (addition.get("languages") or {}).items():
        entry = target["languages"].setdefault(
            lang,
            {key: 0 for key in lang_keys},
        )
        for key in lang_keys:
            entry[key] += stats.get(key, 0)

    for key in target["totals"]:
        target["totals"][key] += (addition.get("totals", {}) or {}).get(key, 0)


def _subtract_language_stats(base: dict, subtraction: dict) -> dict:
    if not subtraction:
        return base

    lang_keys = ("files", "blank_lines", "comment_lines", "code_lines")
    for lang, stats in (subtraction.get("languages") or {}).items():
        entry = base.get("languages", {}).get(lang)
        if not entry:
            continue
        for key in lang_keys:
            entry[key] = max(0, entry.get(key, 0) - stats.get(key, 0))
        if all(entry.get(key, 0) == 0 for key in lang_keys):
            base["languages"].pop(lang, None)

    for key in ("files", "blank_lines", "comment_lines", "code_lines"):
        base["totals"][key] = max(0, base["totals"].get(key, 0) - (subtraction.get("totals", {}) or {}).get(key, 0))

    base["elapsed_seconds"] += subtraction.get("elapsed_seconds", 0.0)
    return base


def _compute_repo_language_stats(
    repos_root: str,
    repo_name: Optional[str],
    include_paths: List[str],
    exclude_paths: List[str],
) -> Optional[dict]:
    if not repo_name:
        return None
    repo_path = os.path.join(repos_root, repo_name)
    if not os.path.isdir(repo_path):
        logger.info(f"    Repository not found: {repo_path}, skipping...")
        return None

    include_abs: List[str] = []
    for rel_path in include_paths or [""]:
        rel = (rel_path or "").rstrip("/")
        candidate = repo_path if not rel else os.path.join(repo_path, rel)
        if os.path.exists(candidate):
            include_abs.append(candidate)
    if not include_abs:
        return None

    stats = run_cloc_for_paths(include_abs)
    if not stats:
        return None

    exclude_abs: List[str] = []
    for rel_path in exclude_paths or []:
        rel = (rel_path or "").rstrip("/")
        if not rel:
            continue
        candidate = os.path.join(repo_path, rel)
        if os.path.exists(candidate):
            exclude_abs.append(candidate)
    if exclude_abs:
        excluded = run_cloc_for_paths(exclude_abs)
        if excluded:
            _subtract_language_stats(stats, excluded)

    return stats


def run_cloc_for_paths(paths: list) -> dict:
    """Run LOC tool on the given paths and return language statistics."""
    existing = [p for p in paths if p and os.path.exists(p)]
    if not existing:
        return {}

    languages: dict[str, dict] = {}
    totals = {
        "files": 0,
        "blank_lines": 0,
        "comment_lines": 0,
        "code_lines": 0,
    }
    elapsed_seconds = 0.0
    exclude_patterns = _tokei_exclude_patterns()

    for path in existing:
        data, elapsed = _run_tokei_json(path, exclude_patterns)
        elapsed_seconds += elapsed
        if not data:
            continue

        for lang_name, lang_data in data.items():
            if lang_name == "Total" or not isinstance(lang_data, dict):
                continue
            entry = languages.setdefault(
                lang_name,
                {
                    "files": 0,
                    "blank_lines": 0,
                    "comment_lines": 0,
                    "code_lines": 0,
                },
            )
            entry["files"] += len(lang_data.get("reports") or [])
            entry["blank_lines"] += int(lang_data.get("blanks") or 0)
            entry["comment_lines"] += int(lang_data.get("comments") or 0)
            entry["code_lines"] += int(lang_data.get("code") or 0)

        total_data = data.get("Total") or {}
        totals["files"] += _count_files_from_tokei_total(total_data)
        totals["blank_lines"] += int(total_data.get("blanks") or 0)
        totals["comment_lines"] += int(total_data.get("comments") or 0)
        totals["code_lines"] += int(total_data.get("code") or 0)

    if not languages:
        return {}

    result_data = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "tokei_version": _get_tokei_version() or "unknown",
        "elapsed_seconds": elapsed_seconds,
        "languages": languages,
        "totals": totals,
    }

    return result_data

def precompute_loc_evolution(year: int, repos_root: str, services_file: str, output_root: str, max_parallel_workers: Optional[int] = None) -> None:
    """
    Precompute monthly LOC evolution for all subsystems and persist to
    stats/subsystems/<name>/monthly/<year>.json.

    It mirrors the logic used in dashboard_server's /loc-evolution endpoint,
    but runs offline for *all* subsystems and never touches the repos on disk
    (it uses git archive snapshots + tokei).
    """
    import subprocess
    import json
    import tempfile
    import tarfile
    import shutil

    logger.info("")
    logger.info("========================================")
    logger.info("Precomputing LOC evolution for subsystems")
    logger.info("Target year: %s", year)
    logger.info("========================================")

    # Load services configuration (mapping repo_key -> {service_name: [paths...]})
    services_config: dict = {}
    if os.path.isfile(services_file):
        try:
            with open(services_file, "r", encoding="utf-8") as sf:
                services_config = json.load(sf)
        except Exception as e:
            logger.warning("Could not read services config %s: %s", services_file, e)
    else:
        logger.warning("Services config file not found: %s", services_file)

    stats_root = os.path.join(output_root, "stats")
    subsystems_root = os.path.join(stats_root, "subsystems")
    repos_root_abs = os.path.abspath(repos_root)

    # Build subsystem -> {repo, paths, exclude_paths} mapping
    subsystem_repos: dict[str, dict] = {}
    repo_service_paths: dict[str, set[str]] = defaultdict(set)

    # First, services defined in services.json
    for repo_key, services in services_config.items():
        for service_name, paths in services.items():
            cleaned_paths = [p.rstrip("/") for p in paths if p]
            subsystem_repos.setdefault(
                service_name,
                {"repo": repo_key, "paths": cleaned_paths or [""], "exclude_paths": []},
            )
            for path in cleaned_paths:
                repo_service_paths[repo_key].add(path)

    # Then, ensure each repo (or the remainder of it) is represented
    if os.path.isdir(repos_root_abs):
        for org_dir in os.listdir(repos_root_abs):
            org_path = os.path.join(repos_root_abs, org_dir)
            if not os.path.isdir(org_path):
                continue

            for repo_dir in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_dir)
                git_dir = os.path.join(repo_path, ".git")
                if not os.path.isdir(git_dir):
                    continue

                repo_key = f"{org_dir}/{repo_dir}"
                subsystem_name = repo_dir
                exclude_paths = sorted(repo_service_paths.get(repo_key, set()))

                subsystem_repos.setdefault(
                    subsystem_name,
                    {"repo": repo_key, "paths": [""], "exclude_paths": exclude_paths},
                )

    subsystem_items = sorted(subsystem_repos.items())
    if not subsystem_items:
        logger.info("[loc-precompute] No subsystems discovered. Skipping LOC evolution precompute.")
        return

    prepared_subsystems = []
    for subsystem_name, info in subsystem_items:
        repo_key = info.get("repo")
        repo_path = os.path.join(repos_root_abs, repo_key) if repo_key else None
        if not repo_path or not os.path.isdir(repo_path):
            logger.info("[loc-precompute] Skipping %s: repo %s not found (path=%s)",
                        subsystem_name, repo_key, repo_path)
            continue
        paths = info.get("paths") or [""]
        filtered_paths = tuple(p for p in paths if p)
        exclude_paths = tuple(info.get("exclude_paths") or [])
        prepared_subsystems.append((subsystem_name, repo_key, repo_path, filtered_paths, exclude_paths))

    if not prepared_subsystems:
        logger.info("[loc-precompute] No valid subsystems to process after repository checks.")
        return

    month_labels = [f"{year:04d}-{month:02d}" for month in range(1, 13)]
    series_map: dict[str, dict[str, dict]] = {
        subsystem_name: {
            label: {"month": label, "code_lines": 0, "files": 0, "recorded": False}
            for label in month_labels
        }
        for subsystem_name, _, _, _, _ in prepared_subsystems
    }

    exclude_patterns = _tokei_exclude_patterns()

    tasks = []
    for subsystem_name, repo_key, repo_path, filtered_paths, exclude_paths in prepared_subsystems:
        logger.info("[loc-precompute] %s (%s)", subsystem_name, repo_key)
        for month in range(1, 13):
            tasks.append((subsystem_name, repo_key, repo_path, filtered_paths, exclude_paths, month))

    if max_parallel_workers is not None and max_parallel_workers > 0:
        max_workers = max_parallel_workers
    else:
        max_workers = max(1, multiprocessing.cpu_count())

    logger.info(
        "[loc-precompute] Processing %s subsystem-month combinations (CPU workers=%s)",
        len(tasks),
        max_workers,
    )

    def process_month_task(subsystem_name, repo_key, repo_path, filtered_paths, exclude_paths, month):
        filtered_list = list(filtered_paths)
        exclude_list = [p for p in (exclude_paths or []) if p]
        since = f"{year:04d}-{month:02d}-01"
        until = f"{year + 1:04d}-01-01" if month == 12 else f"{year:04d}-{month + 1:02d}-01"
        month_label = f"{year:04d}-{month:02d}"

    
        try:
            rev_list_cmd = [
                "git", "-C", repo_path, "rev-list",
                f"--since={since}", f"--until={until}", "--reverse", "HEAD",
            ]
            if filtered_list:
                rev_list_cmd.append("--")
                rev_list_cmd.extend(filtered_list)

            rl = subprocess.run(
                rev_list_cmd,
                capture_output=True,
                text=True,
                check=False,
            )

            if rl.returncode != 0:
                logger.warning(
                    "[loc-precompute] rev-list failed for %s (%s) %s..%s: rc=%s stderr=%s",
                    subsystem_name,
                    repo_key,
                    since,
                    until,
                    rl.returncode,
                    (rl.stderr or "").strip(),
                )
                return subsystem_name, month_label, 0, 0, False

            revs = [line.strip() for line in rl.stdout.splitlines() if line.strip()]
            if not revs:
                logger.info(
                    "[loc-precompute] No commits for %s in %s..%s",
                    subsystem_name,
                    since,
                    until,
                )
                return subsystem_name, month_label, 0, 0, False

            rev = revs[0]

            if filtered_list:
                ls_cmd = ["git", "-C", repo_path, "ls-tree", "--name-only", rev, "--"]
                ls_cmd.extend(filtered_list)
                ls = subprocess.run(
                    ls_cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if ls.returncode != 0:
                    logger.warning(
                        "[loc-precompute] ls-tree failed for %s at %s: rc=%s stderr=%s",
                        repo_key,
                        rev,
                        ls.returncode,
                        (ls.stderr or "").strip(),
                    )
                    return subsystem_name, month_label, 0, 0, False

                present_files = [line.strip() for line in ls.stdout.splitlines() if line.strip()]
                if not present_files:
                    logger.info(
                        "[loc-precompute] No files for %s at %s in paths %s",
                        subsystem_name,
                        rev,
                        filtered_list,
                    )
                    return subsystem_name, month_label, 0, 0, False

            tmpdir = tempfile.mkdtemp(prefix="loc-precompute-")
            try:
                tar_path = os.path.join(tmpdir, "snapshot.tar")
                archive_cmd = ["git", "-C", repo_path, "archive", "--format=tar", rev]
                if filtered_list:
                    archive_cmd.extend(filtered_list)

                ar = subprocess.run(
                    archive_cmd,
                    capture_output=True,
                    text=False,
                    check=False,
                )

                if ar.returncode != 0 or not ar.stdout:
                    logger.warning(
                        "[loc-precompute] archive failed for %s %s: rc=%s",
                        repo_key,
                        rev,
                        ar.returncode,
                    )
                    return subsystem_name, month_label, 0, 0, False

                with open(tar_path, "wb") as tf:
                    tf.write(ar.stdout)

                with tarfile.open(tar_path, "r") as tar:
                    tar.extractall(path=tmpdir)

                if exclude_list:
                    for rel_path in exclude_list:
                        rel = rel_path.strip().strip("/\\")
                        if not rel:
                            continue
                        candidate = os.path.join(tmpdir, rel)
                        if os.path.isdir(candidate):
                            shutil.rmtree(candidate, ignore_errors=True)
                        elif os.path.isfile(candidate):
                            os.remove(candidate)

                data, _ = _run_tokei_json(tmpdir, exclude_patterns)

                if not data:
                    logger.warning(
                        "[loc-precompute] tokei failed for %s %s",
                        subsystem_name,
                        rev,
                    )
                    return subsystem_name, month_label, 0, 0, False

                total_data = data.get("Total") or {}
                total_code = int(total_data.get("code") or 0)
                total_files = _count_files_from_tokei_total(total_data)
                if total_code == 0 and total_files == 0:
                    for lang_info in data.values():
                        if not isinstance(lang_info, dict):
                            continue
                        total_code += int(lang_info.get("code") or 0)
                        total_files += len(lang_info.get("reports") or [])

                logger.info(
                    "[loc-precompute] %s %s: code_lines=%s files=%s",
                    subsystem_name,
                    month_label,
                    total_code,
                    total_files,
                )
                return subsystem_name, month_label, total_code, total_files, True

            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

        except Exception as e:
            logger.warning(
                "[loc-precompute] Error computing LOC for %s %s: %s",
                subsystem_name,
                month_label,
                e,
            )
            return subsystem_name, month_label, 0, 0, False

    def _carry_forward_series(entries: dict[str, dict]):
        last_lines = None
        last_files = None
        for label in month_labels:
            entry = entries[label]
            if entry.get("recorded"):
                last_lines = entry.get("code_lines")
                last_files = entry.get("files")
            elif last_lines is not None:
                entry["code_lines"] = last_lines
                entry["files"] = last_files

    if max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(process_month_task, *task): task[:2] for task in tasks
            }
            for future in as_completed(future_map):
                try:
                    subsystem_name, month_label, code_lines, total_files, recorded = future.result()
                    entry = series_map[subsystem_name][month_label]
                    entry["code_lines"] = code_lines
                    entry["files"] = total_files
                    entry["recorded"] = recorded
                except Exception as exc:
                    subsystem_name, _ = future_map[future]
                    logger.warning("[loc-precompute] Unexpected failure for %s: %s", subsystem_name, exc)
    else:
        for task in tasks:
            subsystem_name, month_label, code_lines, total_files, recorded = process_month_task(*task)
            entry = series_map[subsystem_name][month_label]
            entry["code_lines"] = code_lines
            entry["files"] = total_files
            entry["recorded"] = recorded

    for subsystem_name, _, _, _, _ in prepared_subsystems:
        out_dir = os.path.join(subsystems_root, subsystem_name, "monthly")
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"{year:04d}.json")
        _carry_forward_series(series_map[subsystem_name])
        ordered_series = []
        for label in month_labels:
            entry = series_map[subsystem_name][label]
            entry.pop("recorded", None)
            ordered_series.append(entry)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(
                {"generated_at": datetime.utcnow().isoformat() + "Z", "series": ordered_series},
                f,
                indent=2,
            )
        logger.info("[loc-precompute] Wrote %s", out_file)


def precompute_subsystem_dashboard_caches(
    output_root: str,
    lookback_days: int = 90,
    max_parallel_workers: Optional[int] = None,
) -> None:
    from subsystem_metrics import (
        compute_dead_subsystems,
        compute_subsystem_top_maintainers,
        compute_subsystem_maintainer_timeline,
        compute_subsystem_significant_ownership,
        compute_subsystem_size_rankings,
    )

    stats_root = os.path.join(output_root, "stats")
    subsystems_root = os.path.join(stats_root, "subsystems")
    if not os.path.isdir(subsystems_root):
        logger.info("[subsystem-cache] No subsystems directory at %s; skipping cache generation.", subsystems_root)
        return

    logger.info("\n===========================================")
    logger.info("Precomputing subsystem dashboard caches")
    logger.info("===========================================")

    timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    try:
        dead_status = compute_dead_subsystems(stats_root)
        dead_payload = {
            "generated_at": timestamp,
            "threshold_months": 3,
            "subsystem_status": dead_status,
        }
        dead_path = os.path.join(subsystems_root, "dead_status.json")
        _write_json_atomic(dead_path, dead_payload)
        logger.info("[subsystem-cache] Dead status cached for %s subsystems", len(dead_status))
    except Exception as exc:
        logger.info("[subsystem-cache] Warning: dead status computation failed: %s", exc)

    subsystem_names = sorted(
        name for name in os.listdir(subsystems_root) if os.path.isdir(os.path.join(subsystems_root, name))
    )
    if not subsystem_names:
        logger.info("[subsystem-cache] No subsystem directories found. Skipping dashboard cache generation.")
        return

    if max_parallel_workers and max_parallel_workers > 0:
        worker_count = min(max_parallel_workers, len(subsystem_names))
    else:
        worker_count = min(max(1, multiprocessing.cpu_count()), len(subsystem_names))

    def process_subsystem(subsystem_name: str) -> tuple[str, int]:
        subsystem_dir = os.path.join(subsystems_root, subsystem_name)
        produced = 0

        try:
            top_payload = compute_subsystem_top_maintainers(stats_root, subsystem_name, lookback_days)
            top_payload["generated_at"] = timestamp
            _write_json_atomic(os.path.join(subsystem_dir, "top_maintainers.json"), top_payload)
            produced += 1
        except Exception as exc:
            logger.info("[subsystem-cache] %s: failed to compute top maintainers (%s)", subsystem_name, exc)

        try:
            timeline_payload = compute_subsystem_maintainer_timeline(stats_root, subsystem_name)
            timeline_payload["generated_at"] = timestamp
            _write_json_atomic(os.path.join(subsystem_dir, "maintainer_timeline.json"), timeline_payload)
            produced += 1
        except Exception as exc:
            logger.info("[subsystem-cache] %s: failed to compute maintainer timeline (%s)", subsystem_name, exc)

        try:
            ownership_payload = compute_subsystem_significant_ownership(stats_root, subsystem_name)
            ownership_payload["generated_at"] = timestamp
            _write_json_atomic(os.path.join(subsystem_dir, "significant_ownership.json"), ownership_payload)
            produced += 1
        except Exception as exc:
            logger.info("[subsystem-cache] %s: failed to compute significant ownership (%s)", subsystem_name, exc)

        return subsystem_name, produced

    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(process_subsystem, name): name for name in subsystem_names}
            for future in as_completed(future_map):
                try:
                    name, produced = future.result()
                    logger.info("[subsystem-cache] %s: refreshed %s artifacts", name, produced)
                except Exception as exc:
                    name = future_map.get(future, "unknown")
                    logger.info("[subsystem-cache] %s: unexpected failure (%s)", name, exc)
    else:
        for name in subsystem_names:
            name, produced = process_subsystem(name)
            logger.info("[subsystem-cache] %s: refreshed %s artifacts", name, produced)

    try:
        size_payload = compute_subsystem_size_rankings(stats_root)
        size_payload["generated_at"] = timestamp
        _write_json_atomic(os.path.join(subsystems_root, "size_rankings.json"), size_payload)
        logger.info(
            "[subsystem-cache] Size rankings cached (%s subsystems)",
            size_payload.get("total_subsystems", 0),
        )
    except Exception as exc:
        logger.info("[subsystem-cache] Warning: size rankings computation failed: %s", exc)

    logger.info("[subsystem-cache] Completed cache refresh for %s subsystems", len(subsystem_names))


if __name__ == "__main__":
    main()

