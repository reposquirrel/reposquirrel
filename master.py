#!/usr/bin/env python3
import argparse
import calendar
import os
import sys
import subprocess
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing


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
        "--max-workers",
        dest="max_workers",
        type=int,
        default=None,
        help="Maximum number of parallel workers (default: auto-detect based on CPU cores)",
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
    print(f"\n=== Running: {desc} ===")
    print("Command:", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=False)
    except FileNotFoundError as e:
        print(f"ERROR: Failed to run '{desc}': {e}", file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        print(f"ERROR: '{desc}' exited with code {result.returncode}", file=sys.stderr)
        sys.exit(result.returncode)
    else:
        print(f"=== Done: {desc} ===")

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
    ignore_file = month_data["ignore_file"]
    use_parallel_repos = month_data.get("use_parallel_repos", True)  # Enable repo-level parallelization
    
    try:
        print(f"\n--- Processing month: {year}-{month:02d} ({date_from} -> {date_to}) ---")
        
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
        ]
        
        if use_parallel_repos:
            summery_cmd.append("--parallel")
            
        result1 = subprocess.run(summery_cmd, check=False)
        
        if result1.returncode != 0:
            print(f"ERROR: summery.py failed for {year}-{month:02d} with return code {result1.returncode}", file=sys.stderr)
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
            "--ignore-file",
            ignore_file,
        ]
        
        if use_parallel_repos:
            service_cmd.append("--parallel")
            
        result2 = subprocess.run(service_cmd, check=False)

        if result2.returncode != 0:
            print(f"ERROR: service.py failed for {year}-{month:02d} with return code {result2.returncode}", file=sys.stderr)
            return (month, False)
            
        print(f"✅ Completed month: {year}-{month:02d}")
        return (month, True)
        
    except Exception as e:
        print(f"ERROR: Exception processing month {year}-{month:02d}: {e}", file=sys.stderr)
        return (month, False)


def main() -> None:
    args = parse_args()

    year = args.year
    repos_root = args.repos_root
    output_root = args.output_root
    services_file = args.services_file
    ignore_file = args.ignore_file
    skip_blame = args.skip_blame
    parallel = args.parallel
    max_workers = args.max_workers

    if year < 1:
        print("ERROR: year must be a positive integer", file=sys.stderr)
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
    if max_workers is None:
        # Use a reasonable default: min of available CPU cores and number of months to process
        available_cores = multiprocessing.cpu_count()
        months_to_process = last_month - first_month + 1
        # Increase cap to 6 for systems with more cores, as blame analysis is often I/O bound
        worker_cap = 6 if available_cores >= 8 else 4
        max_workers = min(available_cores, months_to_process, worker_cap)

    print("Master yearly analysis")
    print("----------------------")
    print(f"Year        : {year}")
    print(f"Months      : {first_month:02d}..{last_month:02d}")
    print(f"Repos root  : {repos_root}")
    print(f"Output root : {output_root}")
    print(f"Services    : {services_file}")
    print(f"Ignore      : {ignore_file}")
    if parallel:
        print(f"Parallel    : Enabled (max workers: {max_workers})")
    else:
        print(f"Parallel    : Disabled (sequential processing)")

    script_dir = os.path.dirname(os.path.abspath(__file__))
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
            print(f"ERROR: Required script '{name}' not found at {path}", file=sys.stderr)
            sys.exit(1)

    python_exe = sys.executable or "python3"

    # Process months (either in parallel or sequentially)
    if parallel and (last_month - first_month + 1) > 1:
        print(f"\n🚀 Processing {last_month - first_month + 1} months in parallel...")
        
        # Prepare month data for workers
        month_tasks = []
        for month in range(first_month, last_month + 1):
            try:
                date_from, date_to = compute_month_range(year, month)
            except ValueError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                sys.exit(1)

            # Sanity check
            try:
                datetime.fromisoformat(date_from)
                datetime.fromisoformat(date_to)
            except ValueError as e:
                print(f"ERROR: Invalid computed dates for {year}-{month:02d}: {e}", file=sys.stderr)
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
                    print(f"✅ Month {year}-{month:02d} completed successfully")
                else:
                    failed_months.append(month)
                    print(f"❌ Month {year}-{month:02d} failed")

        if failed_months:
            print(f"\nERROR: Monthly processing failed for months: {sorted(failed_months)}", file=sys.stderr)
            sys.exit(1)
            
        print(f"\n✅ All {len(completed_months)} months processed successfully in parallel!")
        
    else:
        # Sequential processing (original behavior)
        print(f"\n📊 Processing {last_month - first_month + 1} months sequentially...")
        for month in range(first_month, last_month + 1):
            try:
                date_from, date_to = compute_month_range(year, month)
            except ValueError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                sys.exit(1)

            # Sanity check
            try:
                datetime.fromisoformat(date_from)
                datetime.fromisoformat(date_to)
            except ValueError as e:
                print(f"ERROR: Invalid computed dates for {year}-{month:02d}: {e}", file=sys.stderr)
                sys.exit(1)

            print("\n-------------------------------------------")
            print(f"Processing month: {year}-{month:02d}")
            print(f"Date range      : {date_from} -> {date_to}")
            print("-------------------------------------------")

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
                "--ignore-file",
                ignore_file,
            ]
            if parallel:  # Use repo-level parallelization when month-level parallelization is disabled
                service_cmd.append("--parallel")
                
            run_cmd(
                service_cmd,
                desc=f"service.py for {year}-{month:02d} ({date_from}..{date_to})",
            )

    # After all months: create yearly summaries
    print("\n===========================================")
    print("Generating yearly summaries")
    print("===========================================")
    
    # Create yearly summaries
    create_yearly_summaries(year, output_root, first_month, last_month)
    
    # Generate language statistics for subsystems
    print("\n===========================================")
    print("Generating language statistics for subsystems")
    print("===========================================")
    generate_subsystem_language_stats(repos_root, output_root, services_file)

    # After all months: run blame.py once (full history) - optional
    if not skip_blame:
        print("\n===========================================")
        print("Running blame.py (full-history ownership)")
        print("===========================================")

        blame_cmd = [
            python_exe,
            blame_script,
            "--repos-root",
            repos_root,
            "--output-root",
            output_root,
            "--services-file",
            services_file,
            "--ignore-file",
            ignore_file,
        ]
        if parallel:
            blame_cmd.append("--parallel")
            
        run_cmd(
            blame_cmd,
            desc="blame.py (full history)",
        )
    else:
        print("\n===========================================")
        print("Skipping blame.py (--skip-blame specified)")
        print("===========================================")

    print("\n=== All yearly analyses completed successfully ===")
    if skip_blame:
        print("Note: Ownership/blame analysis was skipped. Run without --skip-blame for complete analysis.")
    
    # Note about repos directory: It's kept for blame analysis only (for badges)
    # The actual service/subsystem statistics are now in stats/subsystems/
    repos_stats_dir = os.path.join(output_root, "stats", "repos")
    if os.path.exists(repos_stats_dir):
        if skip_blame:
            print("INFO: stats/repos directory exists but blame analysis was skipped.")
            print("INFO: This directory is only used for blame analysis and badges.")
        else:
            print("INFO: stats/repos directory contains blame analysis for badges.")
            print("INFO: Main subsystem statistics are in stats/subsystems/")
    else:
        print("INFO: No stats/repos directory found. Will be created by blame.py if needed.")


def create_yearly_summaries(year: int, output_root: str, first_month: int, last_month: int) -> None:
    """Create yearly summaries by aggregating all monthly data."""
    import json
    from collections import defaultdict
    
    stats_root = os.path.join(output_root, "stats")
    
    # Process user yearly summaries
    create_user_yearly_summaries(stats_root, year, first_month, last_month)
    
    # Process service yearly summaries  
    create_service_yearly_summaries(stats_root, year, first_month, last_month)


def create_user_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly user summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    users_root = os.path.join(stats_root, "users")
    if not os.path.isdir(users_root):
        return
    
    print("Creating user yearly summaries...")
    
    for user_slug in os.listdir(users_root):
        user_path = os.path.join(users_root, user_slug)
        if not os.path.isdir(user_path):
            continue
        
        # Collect all monthly data for this year
        yearly_data = aggregate_user_monthly_data(user_path, year, first_month, last_month)
        
        if yearly_data:
            # Create yearly folder
            yearly_folder = f"{year:04d}-01-01_{year:04d}-12-31"
            yearly_dir = os.path.join(user_path, yearly_folder)
            os.makedirs(yearly_dir, exist_ok=True)
            
            # Write yearly summary
            output_path = os.path.join(yearly_dir, "summary.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(yearly_data, f, indent=2)
            
            print(f"  Created yearly summary for user: {user_slug}")


def create_repo_yearly_summaries(stats_root: str, year: int, first_month: int, last_month: int) -> None:
    """Create yearly repo summaries by aggregating monthly data."""
    import json
    from collections import defaultdict
    
    repos_root = os.path.join(stats_root, "repos")
    if not os.path.isdir(repos_root):
        return
    
    print("Creating repo yearly summaries...")
    
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
                    
                    print(f"  Created yearly summary for repo: {repo_rel}")


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
        # Look for monthly data
        for entry in os.listdir(user_path):
            if not os.path.isdir(os.path.join(user_path, entry)):
                continue
            if "_" not in entry:
                continue
                
            date_from, date_to = entry.split("_", 1)
            if not date_from.startswith(f"{year:04d}-{month:02d}"):
                continue
                
            monthly_file = os.path.join(user_path, entry, "summary.json")
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
                print(f"  Warning: Failed to read {monthly_file}: {e}")
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
                print(f"  Warning: Failed to read {monthly_file}: {e}")
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
    
    print("Creating service yearly summaries...")
    
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
            
            print(f"  Created yearly summary for service: {service_name}")


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
                print(f"  Warning: Failed to read {monthly_file}: {e}")
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


def generate_subsystem_language_stats(repos_root: str, output_root: str, services_file: str) -> None:
    """Generate language statistics for each subsystem using cloc."""
    import json
    import subprocess
    import tempfile
    
    # Load services configuration
    services_config = {}
    if os.path.isfile(services_file):
        try:
            with open(services_file, "r", encoding="utf-8") as f:
                services_config = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Error loading services file {services_file}: {e}")
    else:
        print(f"Services file {services_file} not found, will only process standalone repositories")
    
    stats_root = os.path.join(output_root, "stats")
    subsystems_stats_root = os.path.join(stats_root, "subsystems")
    
    # Check if cloc is available
    try:
        subprocess.run(["cloc", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("cloc not found. Please install cloc to generate language statistics.")
        print("On Ubuntu/Debian: sudo apt-get install cloc")
        print("On macOS: brew install cloc")
        return
    
    print("Generating language statistics for subsystems...")
    
    # Create a mapping of subsystem -> list of (repo, paths)
    subsystem_repos = {}
    
    # First, add services defined in configuration/services.json
    for repo_name, services in services_config.items():
        for service_name, paths in services.items():
            if service_name not in subsystem_repos:
                subsystem_repos[service_name] = []
            subsystem_repos[service_name].append((repo_name, paths))
    
    # Next, discover standalone repositories (those that exist on disk but not in configuration/services.json)
    repos_root_abs = os.path.abspath(repos_root)
    if os.path.exists(repos_root_abs):
        print("  Looking for standalone repositories...")
        for org_dir in os.listdir(repos_root_abs):
            org_path = os.path.join(repos_root_abs, org_dir)
            if not os.path.isdir(org_path):
                continue
                
            for repo_dir in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_dir)
                git_dir = os.path.join(repo_path, ".git")
                
                if os.path.exists(git_dir):
                    repo_name = f"{org_dir}/{repo_dir}"
                    
                    # Check if this repository is NOT already handled by configuration/services.json
                    if repo_name not in services_config:
                        print(f"  Found standalone repository: {repo_name}")
                        # Use the repo directory name as the subsystem name
                        subsystem_name = repo_dir
                        if subsystem_name not in subsystem_repos:
                            subsystem_repos[subsystem_name] = []
                        subsystem_repos[subsystem_name].append((repo_name, [""]))  # Empty path = entire repo
    
    for subsystem_name, repo_paths in subsystem_repos.items():
        print(f"  Processing subsystem: {subsystem_name}")
        
        # Create subsystem stats directory if it doesn't exist
        subsystem_dir = os.path.join(subsystems_stats_root, subsystem_name)
        if not os.path.exists(subsystem_dir):
            print(f"    Subsystem stats directory not found: {subsystem_dir}, skipping...")
            continue
        
        # Collect all paths for this subsystem
        all_paths = []
        for repo_name, service_paths in repo_paths:
            repo_path = os.path.join(repos_root, repo_name)
            if not os.path.exists(repo_path):
                print(f"    Repository not found: {repo_path}, skipping...")
                continue
            
            for service_path in service_paths:
                if service_path == "":
                    # Empty path means entire repo
                    all_paths.append(repo_path)
                else:
                    # Specific path within repo
                    full_path = os.path.join(repo_path, service_path.rstrip("/"))
                    if os.path.exists(full_path):
                        all_paths.append(full_path)
        
        if not all_paths:
            print(f"    No valid paths found for subsystem {subsystem_name}, skipping...")
            continue
        
        # Run cloc on all paths for this subsystem
        try:
            cloc_result = run_cloc_for_paths(all_paths)
            if cloc_result:
                # Save language stats file
                languages_file = os.path.join(subsystem_dir, "languages.json")
                with open(languages_file, "w", encoding="utf-8") as f:
                    json.dump(cloc_result, f, indent=2)
                print(f"    Generated language stats: {languages_file}")
            else:
                print(f"    No language statistics generated for {subsystem_name}")
        except Exception as e:
            print(f"    Error generating language stats for {subsystem_name}: {e}")
            continue


def run_cloc_for_paths(paths: list) -> dict:
    """Run cloc on the given paths and return language statistics."""
    import subprocess
    import json
    import tempfile
    
    # Create a temporary file to hold the list of paths
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp_file:
        for path in paths:
            tmp_file.write(path + '\n')
        tmp_file_path = tmp_file.name
    
    try:
        # Run cloc with JSON output on all paths
        cmd = [
            "cloc",
            "--json",
            "--list-file=" + tmp_file_path,
            "--exclude-dir=.git,node_modules,.venv,__pycache__,vendor,target,build,dist",
            "--skip-uniqueness"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
        
        if result.returncode != 0:
            print(f"    cloc command failed with return code {result.returncode}")
            if result.stderr:
                print(f"    stderr: {result.stderr}")
            return {}
        
        if not result.stdout.strip():
            print("    cloc produced no output")
            return {}
        
        # Parse JSON output
        try:
            cloc_data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            print(f"    Failed to parse cloc JSON output: {e}")
            return {}
        
        # Convert cloc output to our format
        languages = {}
        header = cloc_data.get("header", {})
        
        for lang_name, lang_data in cloc_data.items():
            if lang_name in ["header", "SUM"]:
                continue
            
            if isinstance(lang_data, dict) and "nFiles" in lang_data:
                languages[lang_name] = {
                    "files": lang_data.get("nFiles", 0),
                    "blank_lines": lang_data.get("blank", 0),
                    "comment_lines": lang_data.get("comment", 0),
                    "code_lines": lang_data.get("code", 0)
                }
        
        # Add summary information
        result_data = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "cloc_version": header.get("cloc_version", "unknown"),
            "elapsed_seconds": header.get("elapsed_seconds", 0),
            "languages": languages
        }
        
        # Add totals
        sum_data = cloc_data.get("SUM", {})
        if sum_data:
            result_data["totals"] = {
                "files": sum_data.get("nFiles", 0),
                "blank_lines": sum_data.get("blank", 0),
                "comment_lines": sum_data.get("comment", 0),
                "code_lines": sum_data.get("code", 0)
            }
        
        return result_data
        
    finally:
        # Clean up temporary file
        try:
            os.unlink(tmp_file_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()

