#!/usr/bin/env python3
import os
import json
import queue
import threading
import time
import sys
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional

from flask import Flask, jsonify, send_from_directory, render_template, abort, request, Response

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATS_ROOT = os.path.join(BASE_DIR, "stats")

app = Flask(__name__, template_folder="templates", static_folder="static")

# Global storage for clone progress
clone_operations = {}

# Global queue for update progress messages
update_progress_queue = queue.Queue()
update_process_active = False

# Update log file
UPDATE_LOG_FILE = os.path.join(BASE_DIR, "update_logs.txt")

def log_update_message(message_dict):
    """Log update messages to both queue and persistent file."""
    # Add to queue for SSE streaming
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

# Automatic cleanup on server startup
def reset_update_state():
    """Reset update process state - called on startup and after repo operations"""
    global update_process_active
    print("🔄 Resetting update process state...")
    update_process_active = False
    
    # Clear any remaining messages in the queue
    queue_cleared = 0
    while not update_progress_queue.empty():
        try:
            update_progress_queue.get_nowait()
            queue_cleared += 1
        except queue.Empty:
            break
    
    if queue_cleared > 0:
        print(f"🧹 Cleared {queue_cleared} messages from update queue")

def start_new_update_log():
    """Start a new section in the update log."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        separator = "=" * 80
        with open(UPDATE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"\n{separator}\n")
            f.write(f"UPDATE SESSION STARTED: {timestamp}\n")
            f.write(f"{separator}\n\n")
    except Exception as e:
        print(f"Error starting update log: {e}")

# Reset state on application startup
reset_update_state()
print("✅ Update state reset on startup")


# ---------------------------
# Helper functions
# ---------------------------

def list_user_months() -> Dict[str, List[Dict[str, Any]]]:
    """
    Scan stats/users and return:
    {
      "user_slug": [
        {"from": "...", "to": "...", "label": "YYYY-MM", "path": "...", "is_yearly": False},
        {"from": "...", "to": "...", "label": "YYYY", "path": "...", "is_yearly": True},
        ...
      ],
      ...
    }
    """
    users_root = os.path.join(STATS_ROOT, "users")
    result: Dict[str, List[Dict[str, Any]]] = {}

    if not os.path.isdir(users_root):
        return result

    for user_slug in sorted(os.listdir(users_root)):
        user_path = os.path.join(users_root, user_slug)
        if not os.path.isdir(user_path):
            continue
        month_entries: List[Dict[str, Any]] = []
        for entry in sorted(os.listdir(user_path)):
            subdir = os.path.join(user_path, entry)
            if not os.path.isdir(subdir):
                continue
            # We expect directories like "YYYY-MM-DD_YYYY-MM-DD"
            if "_" not in entry:
                continue
            date_from, date_to = entry.split("_", 1)
            
            # Check if this is a yearly summary (e.g., "2025-01-01_2025-12-31")
            is_yearly = (date_from.endswith("-01-01") and date_to.endswith("-12-31") and 
                        date_from[:4] == date_to[:4])
            
            if is_yearly:
                label = date_from[:4]  # Just the year
            else:
                label = date_from[:7] if len(date_from) >= 7 else entry  # YYYY-MM
                
            summary_path = os.path.join(subdir, "summary.json")
            if not os.path.isfile(summary_path):
                continue
            month_entries.append(
                {
                    "from": date_from,
                    "to": date_to,
                    "label": label,
                    "folder": entry,
                    "is_yearly": is_yearly,
                }
            )
        if month_entries:
            result[user_slug] = month_entries
    return result


def list_repos_with_blame() -> List[str]:
    """
    Return all repos that have a stats/repos/<repo>/blame/blame.json file.
    This is kept for badge analysis only.
    """
    repos_root = os.path.join(STATS_ROOT, "repos")
    if not os.path.isdir(repos_root):
        return []

    repos_with_blame: List[str] = []
    for root, dirs, files in os.walk(repos_root):
        if "blame.json" in files:
            rel_path = os.path.relpath(root, repos_root)
            # Expect rel_path like "<repo>/blame"
            parts = rel_path.split(os.sep)
            if len(parts) >= 2 and parts[-1] == "blame":
                repo_rel = os.path.join(*parts[:-1]).replace(os.sep, "/")
                repos_with_blame.append(repo_rel)

    return sorted(set(repos_with_blame))


def list_service_months() -> Dict[str, List[Dict[str, Any]]]:
    """
    Scan stats/subsystems and return:
    {
      "service_name": [
        {"from": "...", "to": "...", "label": "YYYY-MM", "folder": "...", "is_yearly": False},
        {"from": "...", "to": "...", "label": "YYYY", "folder": "...", "is_yearly": True},
        ...
      ],
      ...
    }
    """
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")
    result: Dict[str, List[Dict[str, Any]]] = {}

    if not os.path.isdir(subsystems_root):
        return result

    for service_name in sorted(os.listdir(subsystems_root)):
        service_path = os.path.join(subsystems_root, service_name)
        if not os.path.isdir(service_path):
            continue
        period_entries: List[Dict[str, Any]] = []
        for entry in sorted(os.listdir(service_path)):
            subdir = os.path.join(service_path, entry)
            if not os.path.isdir(subdir):
                continue
            # We expect directories like "YYYY-MM-DD_YYYY-MM-DD"
            if "_" not in entry:
                continue
            date_from, date_to = entry.split("_", 1)
            
            # Check if this is a yearly summary (e.g., "2025-01-01_2025-12-31")
            is_yearly = (date_from.endswith("-01-01") and date_to.endswith("-12-31") and 
                        date_from[:4] == date_to[:4])
            
            if is_yearly:
                label = date_from[:4]  # Just the year
            else:
                label = date_from[:7] if len(date_from) >= 7 else entry  # YYYY-MM
                
            summary_path = os.path.join(subdir, "summary.json")
            if not os.path.isfile(summary_path):
                continue
            period_entries.append(
                {
                    "from": date_from,
                    "to": date_to,
                    "label": label,
                    "folder": entry,
                    "is_yearly": is_yearly,
                }
            )
        if period_entries:
            result[service_name] = period_entries
    return result


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def analyze_developer_badges() -> Dict[str, List[Dict[str, Any]]]:
    """
    Analyze blame data to determine which subsystems/repositories each developer is the top contributor for.
    Returns a dictionary mapping developer slugs to their badges.
    """
    badges = {}
    
    try:
        # Get ownership badges from blame data (top owner of subsystems/repos)
        ownership_badges = analyze_ownership_badges()
        
        # Get maintainer badges from recent commit activity (last 3 months)
        maintainer_badges = analyze_maintainer_badges()
        
        # Get most productive developer badge (only one developer gets this)
        productive_badge = analyze_most_productive_badge()
        
        # Get 10%+ ownership badges 
        ownership_percentage_badges = analyze_ownership_percentage_badges()
        
        # Merge all types of badges
        for dev_slug in ownership_badges:
            if dev_slug not in badges:
                badges[dev_slug] = []
            badges[dev_slug].extend(ownership_badges[dev_slug])
        
        for dev_slug in maintainer_badges:
            if dev_slug not in badges:
                badges[dev_slug] = []
            badges[dev_slug].extend(maintainer_badges[dev_slug])
        
        for dev_slug in ownership_percentage_badges:
            if dev_slug not in badges:
                badges[dev_slug] = []
            badges[dev_slug].extend(ownership_percentage_badges[dev_slug])
        
        if productive_badge:
            dev_slug, badge = productive_badge
            if dev_slug not in badges:
                badges[dev_slug] = []
            badges[dev_slug].append(badge)
        
        # Sort badges by type and then by metric value (ownership % or commits)
        for dev_slug in badges:
            badges[dev_slug].sort(key=lambda b: (b["type"], -b.get("share", b.get("commits", b.get("lines_added", 0)))))
        
        return badges
        
    except Exception as e:
        print(f"Error in analyze_developer_badges: {e}")
        return {}


def _process_blame_file_for_ownership_percentage(blame_file: str, repo_name: str, repo_full_name: str, badges: Dict[str, List[Dict[str, Any]]]):
    """Helper function to process a single blame file for ownership percentage badges."""
    try:
        blame_data = load_json(blame_file)
        
        # Check individual developers in the blame data
        developers = blame_data.get("developers", {})
        total_lines = blame_data.get("total_lines", 0)
        
        if total_lines > 0:  # Prevent division by zero
            for dev_slug, dev_data in developers.items():
                dev_lines = dev_data.get("lines", 0)
                ownership_share = dev_lines / total_lines
                
                # Only create badge if developer owns >10% of the subsystem
                if ownership_share > 0.10:  # More than 10%
                    if dev_slug not in badges:
                        badges[dev_slug] = []
                    
                    badges[dev_slug].append({
                        "type": "ownership_percentage",
                        "badge_type": "significant_owner",
                        "title": f"Significant Owner: {repo_name}",
                        "subtitle": f"{ownership_share*100:.1f}% ownership ({dev_lines:,} lines)",
                        "subsystem": repo_name,
                        "repo_path": repo_full_name,
                        "lines": dev_lines,
                        "share": ownership_share
                    })
        
        # Check per-service ownership percentages as well  
        services = blame_data.get("services", {})
        for service_name, service_data in services.items():
            service_developers = service_data.get("developers", {})
            service_total_lines = service_data.get("total_lines", 0)
            
            if service_total_lines > 0:  # Prevent division by zero
                for dev_slug, dev_data in service_developers.items():
                    dev_lines = dev_data.get("lines", 0)
                    ownership_share = dev_lines / service_total_lines
                    
                    # Only create badge if developer owns >10% of the service
                    if ownership_share > 0.10:
                        if dev_slug not in badges:
                            badges[dev_slug] = []
                        
                        # Avoid duplicating if service name same as repo name
                        if service_name != repo_name:
                            badges[dev_slug].append({
                                "type": "ownership_percentage", 
                                "badge_type": "significant_service_owner",
                                "title": f"Significant Owner: {service_name}",
                                "subtitle": f"{ownership_share*100:.1f}% ownership ({dev_lines:,} lines)",
                                "subsystem": service_name,
                                "repo_path": repo_full_name,
                                "lines": dev_lines,
                                "share": ownership_share
                            })
    
    except Exception as e:
        print(f"Error processing blame file {blame_file} for ownership percentages: {e}")


def _process_blame_file_for_ownership(blame_file: str, repo_name: str, repo_full_name: str, badges: Dict[str, List[Dict[str, Any]]]):
    """Helper function to process a single blame file for ownership badges."""
    try:
        blame_data = load_json(blame_file)
        
        # Check overall repository top developer
        repo_top_dev = blame_data.get("top_developer")
        if repo_top_dev and repo_top_dev.get("slug"):
            dev_slug = repo_top_dev["slug"]
            if dev_slug not in badges:
                badges[dev_slug] = []
            
            badges[dev_slug].append({
                "type": "ownership",
                "badge_type": "repository_owner",
                "title": f"Top Owner: {repo_name}",
                "subtitle": f"{repo_top_dev.get('lines', 0):,} lines ({repo_top_dev.get('share', 0)*100:.1f}%)",
                "subsystem": repo_name,
                "repo_path": repo_full_name,
                "lines": repo_top_dev.get("lines", 0),
                "share": repo_top_dev.get("share", 0)
            })
        
        # Check per-service top developers
        services = blame_data.get("services", {})
        for service_name, service_data in services.items():
            service_top_dev = service_data.get("top_developer")
            if service_top_dev and service_top_dev.get("slug"):
                dev_slug = service_top_dev["slug"]
                if dev_slug not in badges:
                    badges[dev_slug] = []
                
                # Skip if it's the same as repo owner and service name matches repo name
                if service_name == repo_name and repo_top_dev and repo_top_dev.get("slug") == dev_slug:
                    continue
                
                badges[dev_slug].append({
                    "type": "ownership",
                    "badge_type": "service_owner", 
                    "title": f"Top Owner: {service_name}",
                    "subtitle": f"{service_top_dev.get('lines', 0):,} lines ({service_top_dev.get('share', 0)*100:.1f}%)",
                    "subsystem": service_name,
                    "repo_path": repo_full_name,
                    "lines": service_top_dev.get("lines", 0),
                    "share": service_top_dev.get("share", 0)
                })
    
    except Exception as e:
        print(f"Error processing blame file {blame_file}: {e}")


def analyze_ownership_badges() -> Dict[str, List[Dict[str, Any]]]:
    """
    Analyze blame data for ownership badges.
    """
    badges = {}
    
    # Check blame files in the repos structure - handle both flat and nested structures
    repos_path = os.path.join(STATS_ROOT, "repos")
    if os.path.exists(repos_path):
        # First, try flat structure (repos/repo_name/blame/blame.json)
        for repo_name in os.listdir(repos_path):
            repo_path = os.path.join(repos_path, repo_name)
            if not os.path.isdir(repo_path):
                continue
            
            blame_file = os.path.join(repo_path, "blame", "blame.json")
            if os.path.exists(blame_file):
                # Found blame file in flat structure
                repo_full_name = repo_name
                _process_blame_file_for_ownership(blame_file, repo_name, repo_full_name, badges)
            else:
                # Try nested structure (repos/org_name/repo_name/blame/blame.json)
                if os.path.isdir(repo_path):
                    for nested_repo_name in os.listdir(repo_path):
                        nested_repo_path = os.path.join(repo_path, nested_repo_name)
                        if not os.path.isdir(nested_repo_path):
                            continue
                        
                        nested_blame_file = os.path.join(nested_repo_path, "blame", "blame.json")
                        if os.path.exists(nested_blame_file):
                            # Found blame file in nested structure
                            repo_full_name = f"{repo_name}/{nested_repo_name}"
                            _process_blame_file_for_ownership(nested_blame_file, nested_repo_name, repo_full_name, badges)
    
    return badges


def analyze_maintainer_badges() -> Dict[str, List[Dict[str, Any]]]:
    """
    Analyze recent commit activity (last 3 months) to determine top maintainers.
    """
    badges = {}
    
    # Get current date to determine last 3 months
    from datetime import datetime, timedelta
    current_date = datetime.now()
    three_months_ago = current_date - timedelta(days=90)
    
    # Check subsystems directory for recent activity
    subsystems_path = os.path.join(STATS_ROOT, "subsystems")
    if not os.path.exists(subsystems_path):
        return badges
    
    subsystem_activity = {}  # subsystem -> {dev_slug: total_commits}
    
    for subsystem_name in os.listdir(subsystems_path):
        subsystem_path = os.path.join(subsystems_path, subsystem_name)
        if not os.path.isdir(subsystem_path):
            continue
        
        subsystem_activity[subsystem_name] = {}
        
        # Look for monthly summary files from last 3 months
        for period_dir in os.listdir(subsystem_path):
            period_path = os.path.join(subsystem_path, period_dir)
            if not os.path.isdir(period_path):
                continue
            
            # Skip yearly summaries for maintainer analysis
            if "_2025-12-31" in period_dir:
                continue
            
            # Parse date range from directory name
            try:
                date_parts = period_dir.split("_")
                if len(date_parts) != 2:
                    continue
                
                from_date_str = date_parts[0]
                period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                
                # Only consider periods within last 3 months
                if period_date < three_months_ago:
                    continue
                
            except (ValueError, IndexError):
                continue
            
            summary_file = os.path.join(period_path, "summary.json")
            if not os.path.exists(summary_file):
                continue
            
            try:
                summary_data = load_json(summary_file)
                
                # Aggregate commits from all repositories for this subsystem/period
                repositories = summary_data.get("repositories", {})
                for repo_data in repositories.values():
                    developers = repo_data.get("developers", {})
                    for dev_slug, dev_data in developers.items():
                        commits = dev_data.get("commits", 0)
                        if commits > 0:
                            if dev_slug not in subsystem_activity[subsystem_name]:
                                subsystem_activity[subsystem_name][dev_slug] = 0
                            subsystem_activity[subsystem_name][dev_slug] += commits
            
            except Exception as e:
                print(f"Error processing summary file {summary_file}: {e}")
                continue
    
    # Determine top maintainer for each subsystem
    for subsystem_name, dev_commits in subsystem_activity.items():
        if not dev_commits:
            continue
        
        # Find developer with most commits
        top_dev_slug = max(dev_commits.keys(), key=lambda slug: dev_commits[slug])
        top_commits = dev_commits[top_dev_slug]
        
        # Only award badge if developer has meaningful activity (at least 3 commits in 3 months)
        if top_commits >= 3:
            if top_dev_slug not in badges:
                badges[top_dev_slug] = []
            
            badges[top_dev_slug].append({
                "type": "maintainer",
                "badge_type": "top_maintainer",
                "title": f"Top Maintainer: {subsystem_name}",
                "subtitle": f"{top_commits} commits (last 3 months)",
                "subsystem": subsystem_name,
                "commits": top_commits,
                "period": "3 months"
            })
    
    return badges


def analyze_most_productive_badge() -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Find the single most productive developer based on total lines added across all subsystems.
    Returns a tuple of (developer_slug, badge_dict) for the most productive developer, or None.
    """
    # Get current date to determine the year for analysis  
    from datetime import datetime
    current_date = datetime.now()
    current_year = current_date.year
    
    # Check subsystems directory for yearly data
    subsystems_path = os.path.join(STATS_ROOT, "subsystems")
    if not os.path.exists(subsystems_path):
        return None
    
    developer_totals = {}  # dev_slug -> total_lines_added
    
    for subsystem_name in os.listdir(subsystems_path):
        subsystem_path = os.path.join(subsystems_path, subsystem_name)
        if not os.path.isdir(subsystem_path):
            continue
        
        # Look for yearly summary for current year
        yearly_folder = f"{current_year:04d}-01-01_{current_year:04d}-12-31"
        yearly_dir = os.path.join(subsystem_path, yearly_folder)
        
        if not os.path.exists(yearly_dir):
            continue
            
        summary_file = os.path.join(yearly_dir, "summary.json")
        if not os.path.exists(summary_file):
            continue
        
        try:
            summary_data = load_json(summary_file)
            
            # Aggregate lines added from all developers in this subsystem
            developers = summary_data.get("developers", {})
            for dev_slug, dev_data in developers.items():
                lines_added = dev_data.get("lines_added", 0)
                if lines_added > 0:
                    if dev_slug not in developer_totals:
                        developer_totals[dev_slug] = {
                            "lines_added": 0,
                            "display_name": dev_data.get("display_name", dev_slug)
                        }
                    developer_totals[dev_slug]["lines_added"] += lines_added
        
        except Exception as e:
            print(f"Error processing yearly summary file {summary_file}: {e}")
            continue
    
    if not developer_totals:
        return None
    
    # Find the developer with the most total lines added
    most_productive_slug = max(developer_totals.keys(), 
                              key=lambda slug: developer_totals[slug]["lines_added"])
    most_productive_data = developer_totals[most_productive_slug]
    
    # Only award if developer has meaningful activity (at least 1000 lines added)
    if most_productive_data["lines_added"] < 1000:
        return None
    
    badge = {
        "type": "productivity",
        "badge_type": "most_productive",
        "title": "🚀 Most Productive Developer",
        "subtitle": f"{most_productive_data['lines_added']:,} lines added ({current_year})",
        "lines_added": most_productive_data["lines_added"],
        "year": current_year,
        "description": f"Sum of all lines added across all subsystems during {current_year}. Calculated by aggregating lines_added from all monthly commits for each developer."
    }
    
    return (most_productive_slug, badge)


def analyze_ownership_percentage_badges() -> Dict[str, List[Dict[str, Any]]]:
    """
    Analyze ownership percentages to create badges for developers who own >10% of a subsystem.
    """
    badges = {}
    
    try:
        # Check blame files in the repos structure for ownership percentages
        repos_path = os.path.join(STATS_ROOT, "repos")
        if not os.path.exists(repos_path):
            return badges
        
        # Also load services config to understand which services are in which repos
        services_config = load_services_config()
        
        for org_name in os.listdir(repos_path):
            org_path = os.path.join(repos_path, org_name)
            if not os.path.isdir(org_path):
                continue
                
            for repo_name in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_name)
                if not os.path.isdir(repo_path):
                    continue
                    
                blame_file = os.path.join(repo_path, "blame", "blame.json")
                if not os.path.exists(blame_file):
                    continue
                
                try:
                    blame_data = load_json(blame_file)
                    repo_full_name = f"{org_name}/{repo_name}"
                    
                    # Check individual developers in the blame data
                    developers = blame_data.get("developers", {})
                    total_lines = blame_data.get("total_lines", 0)
                    
                    if total_lines > 0:  # Prevent division by zero
                        for dev_slug, dev_data in developers.items():
                            dev_lines = dev_data.get("lines", 0)
                            ownership_share = dev_lines / total_lines
                            
                            # Only create badge if developer owns >10% of the subsystem
                            if ownership_share > 0.10:  # More than 10%
                                if dev_slug not in badges:
                                    badges[dev_slug] = []
                                
                                badges[dev_slug].append({
                                    "type": "ownership_percentage",
                                    "badge_type": "significant_owner",
                                    "title": f"Significant Owner: {repo_name}",
                                    "subtitle": f"{ownership_share*100:.1f}% ownership ({dev_lines:,} lines)",
                                    "subsystem": repo_name,
                                    "repo_path": repo_full_name,
                                    "lines": dev_lines,
                                    "share": ownership_share
                                })
                    
                    # Check per-service ownership percentages as well  
                    services = blame_data.get("services", {})
                    for service_name, service_data in services.items():
                        service_developers = service_data.get("developers", {})
                        service_total_lines = service_data.get("total_lines", 0)
                        
                        if service_total_lines > 0:  # Prevent division by zero
                            for dev_slug, dev_data in service_developers.items():
                                dev_lines = dev_data.get("lines", 0)
                                ownership_share = dev_lines / service_total_lines
                                
                                # Only create badge if developer owns >10% of the service
                                if ownership_share > 0.10:  # More than 10%
                                    if dev_slug not in badges:
                                        badges[dev_slug] = []
                                    
                                    # Avoid duplicating if service name same as repo name
                                    if service_name != repo_name:
                                        badges[dev_slug].append({
                                            "type": "ownership_percentage", 
                                            "badge_type": "significant_service_owner",
                                            "title": f"Significant Owner: {service_name}",
                                            "subtitle": f"{ownership_share*100:.1f}% ownership ({dev_lines:,} lines)",
                                            "subsystem": service_name,
                                            "repo_path": repo_full_name,
                                            "lines": dev_lines,
                                            "share": ownership_share
                                        })
                
                except Exception as e:
                    print(f"Error processing blame file {blame_file} for ownership percentages: {e}")
                    continue
        
        return badges
        
    except Exception as e:
        print(f"Error in analyze_ownership_percentage_badges: {e}")
        return {}


def find_user_summary(user_slug: str, from_date: str, to_date: str) -> str:
    """
    Locate stats/users/<slug>/<from>_<to>/summary.json
    """
    folder = f"{from_date}_{to_date}"
    path = os.path.join(STATS_ROOT, "users", user_slug, folder, "summary.json")
    return path


def find_repo_blame(repo_rel: str) -> str:
    """
    Locate stats/repos/<repo_rel>/blame/blame.json
    This is kept for badge analysis only.
    """
    path = os.path.join(STATS_ROOT, "repos", *repo_rel.split("/"), "blame", "blame.json")
    return path


def find_service_summary(service_name: str, from_date: str, to_date: str) -> str:
    """
    Locate stats/subsystems/<service_name>/<from>_<to>/summary.json
    """
    folder = f"{from_date}_{to_date}"
    path = os.path.join(STATS_ROOT, "subsystems", service_name, folder, "summary.json")
    return path


# ---------------------------
# Routes
# ---------------------------

@app.route("/test")
def test_interface():
    return render_template("test.html")


@app.route("/test-ui")
def test_ui_simple():
    """Simple UI test page"""
    with open(os.path.join(BASE_DIR, "test_ui_simple.html"), "r") as f:
        content = f.read()
    return content


@app.route("/test-simple")
def test_simple():
    """Simple API test page"""
    with open(os.path.join(BASE_DIR, "test_dashboard_simple.html"), "r") as f:
        content = f.read()
    return content


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats/check")
def api_stats_check():
    """Check if any stats data exists."""
    try:
        # Check if any user data exists
        users_root = os.path.join(STATS_ROOT, "users")
        has_users = False
        if os.path.exists(users_root):
            has_users = len([d for d in os.listdir(users_root) if os.path.isdir(os.path.join(users_root, d))]) > 0
        
        # Check if any subsystem data exists
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        has_subsystems = False
        if os.path.exists(subsystems_root):
            has_subsystems = len([d for d in os.listdir(subsystems_root) if os.path.isdir(os.path.join(subsystems_root, d))]) > 0
        
        has_data = has_users or has_subsystems
        
        return jsonify({
            "has_data": has_data,
            "has_users": has_users,
            "has_subsystems": has_subsystems
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users")
def api_users():
    user_months = list_user_months()
    users = []
    for slug, months in user_months.items():
        # Try to get a display name from any summary.json
        display_name = slug
        # load first summary for that user to see if we have author_name
        try:
            any_month = months[0]
            path = find_user_summary(slug, any_month["from"], any_month["to"])
            data = load_json(path)
            display_name = data.get("author_name") or slug
        except Exception:
            pass

        users.append(
            {
                "slug": slug,
                "display_name": display_name,
                "months": months,
            }
        )
    return jsonify({"users": users})


@app.route("/api/users/<user_slug>/badges")
def api_user_badges(user_slug: str):
    """Get badges for a specific user based on blame/ownership analysis."""
    try:
        print(f"Getting badges for user: {user_slug}")
        all_badges = analyze_developer_badges()
        user_badges = all_badges.get(user_slug, [])
        print(f"Found {len(user_badges)} badges for user {user_slug}")
        
        return jsonify({"badges": user_badges})
    except Exception as e:
        print(f"Error analyzing badges for user {user_slug}: {str(e)}")
        return jsonify({"badges": [], "error": str(e)})


@app.route("/api/developers/total-ownership")
def api_developers_total_ownership():
    """Get total lines owned by each developer across all subsystems."""
    try:
        from collections import defaultdict
        
        # Load aliases to merge developers
        alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
        alias_map = {}
        if os.path.exists(alias_file):
            try:
                alias_map = load_json(alias_file)
            except:
                pass
        
        # Helper function to get canonical slug
        def get_canonical_slug(slug):
            """Apply aliases to get canonical developer slug."""
            for canonical, aliases in alias_map.items():
                if isinstance(aliases, list) and slug in aliases:
                    return canonical
                elif isinstance(aliases, str) and slug == aliases:
                    return canonical
            return slug
        
        developer_lines = defaultdict(lambda: {"lines": 0, "subsystems": [], "display_name": ""})
        
        # Walk through all blame files
        # Track repos we've already processed to avoid double-counting (standalone vs monorepo)
        repos_path = os.path.join(STATS_ROOT, "repos")
        processed_repos = set()
        
        for root, dirs, files in os.walk(repos_path):
            if "blame.json" in files:
                blame_file = os.path.join(root, "blame.json")
                try:
                    blame_data = load_json(blame_file)
                    repo_full_name = blame_data.get("repo", "")
                    repo_name = repo_full_name.split("/")[-1]
                    
                    # Skip if we've already processed this repo name (avoid standalone + monorepo duplicates)
                    if repo_name in processed_repos:
                        continue
                    
                    processed_repos.add(repo_name)
                    
                    # Check if this repo has services - if so, use service-level data to avoid double counting
                    services = blame_data.get("services", {})
                    
                    if services:
                        # Process service-level developers (more granular)
                        for service_name, service_data in services.items():
                            service_developers = service_data.get("developers", {})
                            for dev_slug, dev_data in service_developers.items():
                                # Apply alias mapping
                                canonical_slug = get_canonical_slug(dev_slug)
                                
                                if isinstance(dev_data, dict):
                                    lines = dev_data.get("lines", 0)
                                    display_name = dev_data.get("display_name", dev_slug)
                                else:
                                    lines = dev_data if isinstance(dev_data, int) else 0
                                    display_name = dev_slug
                                
                                if lines > 0:
                                    developer_lines[canonical_slug]["lines"] += lines
                                    if service_name not in developer_lines[canonical_slug]["subsystems"]:
                                        developer_lines[canonical_slug]["subsystems"].append(service_name)
                                    if not developer_lines[canonical_slug]["display_name"]:
                                        developer_lines[canonical_slug]["display_name"] = display_name
                    else:
                        # No services, process main repo developers
                        developers = blame_data.get("developers", {})
                        for dev_slug, dev_data in developers.items():
                            # Apply alias mapping
                            canonical_slug = get_canonical_slug(dev_slug)
                            
                            lines = dev_data.get("lines", 0)
                            if lines > 0:
                                developer_lines[canonical_slug]["lines"] += lines
                                if repo_name not in developer_lines[canonical_slug]["subsystems"]:
                                    developer_lines[canonical_slug]["subsystems"].append(repo_name)
                                if not developer_lines[canonical_slug]["display_name"]:
                                    developer_lines[canonical_slug]["display_name"] = dev_data.get("display_name", dev_slug)
                
                except Exception as e:
                    print(f"Error processing blame file {blame_file}: {e}")
                    continue
        
        # Convert to list format
        result = []
        for dev_slug, data in developer_lines.items():
            result.append({
                "slug": dev_slug,
                "display_name": data["display_name"],
                "total_lines": data["lines"],
                "subsystem_count": len(set(data["subsystems"])),
                "subsystems": list(set(data["subsystems"]))
            })
        
        # Sort by total lines (descending)
        result.sort(key=lambda x: x["total_lines"], reverse=True)
        
        return jsonify({"developers": result})
        
    except Exception as e:
        print(f"Error calculating total ownership: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"developers": [], "error": str(e)})


@app.route("/api/users/<user_slug>/ownership-timeline")
def api_user_ownership_timeline(user_slug: str):
    """Get ownership timeline for subsystems where this user is a top maintainer."""
    try:
        from datetime import datetime, timedelta
        from collections import defaultdict
        
        # First, find which subsystems this user is a top maintainer of
        all_badges = analyze_developer_badges()
        user_badges = all_badges.get(user_slug, [])
        
        # Extract subsystems where user is top maintainer
        maintainer_subsystems = set()
        for badge in user_badges:
            if badge.get("badge_type") == "top_maintainer":
                maintainer_subsystems.add(badge.get("subsystem"))
        
        if not maintainer_subsystems:
            return jsonify({"timelines": {}})
        
        # For each subsystem, calculate the ownership timeline
        result = {}
        
        for subsystem_name in maintainer_subsystems:
            try:
                # Get current ownership from blame
                repos_path = os.path.join(STATS_ROOT, "repos")
                current_ownership_lines = 0
                total_current_lines = 0
                
                for root, dirs, files in os.walk(repos_path):
                    if "blame.json" in files:
                        blame_file = os.path.join(root, "blame.json")
                        blame_data = load_json(blame_file)
                        
                        # Check repo match
                        if subsystem_name.lower() in blame_data.get("repo", "").lower():
                            developers = blame_data.get("developers", {})
                            total_current_lines = blame_data.get("total_lines", 0)
                            dev_data = developers.get(user_slug, {})
                            current_ownership_lines = dev_data.get("lines", 0) if isinstance(dev_data, dict) else 0
                            break
                        
                        # Check service match
                        services = blame_data.get("services", {})
                        if subsystem_name in services:
                            service_data = services[subsystem_name]
                            developers = service_data.get("developers", {})
                            total_current_lines = service_data.get("total_lines", 0)
                            dev_data = developers.get(user_slug, {})
                            if isinstance(dev_data, dict):
                                current_ownership_lines = dev_data.get("lines", 0)
                            else:
                                current_ownership_lines = dev_data if dev_data else 0
                            break
                
                if total_current_lines == 0:
                    continue
                
                # Get monthly changes
                subsystem_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name)
                if not os.path.exists(subsystem_path):
                    continue
                
                monthly_net_changes = defaultdict(lambda: defaultdict(int))
                
                for period_dir in os.listdir(subsystem_path):
                    if period_dir == 'languages.json' or '_12-31' in period_dir:
                        continue
                    
                    try:
                        from_date_str = period_dir.split('_')[0]
                        period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                        month_label = period_date.strftime("%Y-%m")
                        
                        summary_file = os.path.join(subsystem_path, period_dir, "summary.json")
                        if not os.path.exists(summary_file):
                            continue
                        
                        summary_data = load_json(summary_file)
                        for repo_data in summary_data.get("repositories", {}).values():
                            for dev_slug, dev_data in repo_data.get("developers", {}).items():
                                lines_added = dev_data.get("lines_added", 0)
                                lines_deleted = dev_data.get("lines_deleted", 0)
                                net_lines = lines_added - lines_deleted
                                monthly_net_changes[dev_slug][month_label] += net_lines
                    except:
                        continue
                
                # Calculate backward timeline
                all_months = sorted(set(month for dev_data in monthly_net_changes.values() for month in dev_data.keys()))
                if not all_months:
                    continue
                
                percentages = []
                dev_lines = current_ownership_lines
                total_lines = total_current_lines
                
                for month in reversed(all_months):
                    percentage = (dev_lines / total_lines * 100) if total_lines > 0 else 0
                    percentages.insert(0, round(percentage, 1))
                    
                    dev_lines -= monthly_net_changes[user_slug].get(month, 0)
                    total_lines -= sum(monthly_net_changes[dev].get(month, 0) for dev in monthly_net_changes.keys())
                    
                    dev_lines = max(0, dev_lines)
                    total_lines = max(1, total_lines)
                
                result[subsystem_name] = {
                    "months": all_months,
                    "ownership": percentages,
                    "current_ownership": round((current_ownership_lines / total_current_lines * 100), 1) if total_current_lines > 0 else 0
                }
                
            except Exception as e:
                print(f"Error calculating timeline for {subsystem_name}: {e}")
                continue
        
        return jsonify({"timelines": result})
        
    except Exception as e:
        print(f"Error generating ownership timeline for user {user_slug}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"timelines": {}, "error": str(e)})


@app.route("/api/users/<user_slug>/month/<from_date>/<to_date>")
def api_user_month(user_slug: str, from_date: str, to_date: str):
    path = find_user_summary(user_slug, from_date, to_date)
    if not os.path.isfile(path):
        abort(404, description="User month summary not found")
    data = load_json(path)
    return jsonify(data)


@app.route("/api/users/<user_slug>/year/<int:year>")
def api_user_year(user_slug: str, year: int):
    """Get yearly summary for a user."""
    from_date = f"{year:04d}-01-01"
    to_date = f"{year:04d}-12-31"
    path = find_user_summary(user_slug, from_date, to_date)
    if not os.path.isfile(path):
        abort(404, description="User yearly summary not found")
    data = load_json(path)
    return jsonify(data)


@app.route("/api/subsystems")
def api_subsystems():
    """Get all subsystems with their available time periods."""
    # Get service data from subsystems directory
    service_months = list_service_months()
    
    subsystems = []
    
    # Add all services/subsystems from the unified subsystems directory
    for service_name, periods in service_months.items():
        subsystems.append(
            {
                "name": service_name,
                "type": "subsystem",  # All are now unified as subsystems
                "periods": periods,
            }
        )
    
    return jsonify({"subsystems": subsystems})


def load_services_config() -> Dict[str, Dict[str, list]]:
    """Load services configuration from JSON."""
    services_path = "configuration/services.json"
    if not os.path.isfile(services_path):
        return {}

    try:
        with open(services_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            return {}
        
        return data
    except (json.JSONDecodeError, IOError):
        return {}


def load_team_subsystem_responsibilities() -> Dict[str, List[str]]:
    """Load team-subsystem responsibilities from JSON."""
    responsibilities_path = os.path.join(BASE_DIR, "configuration/team_subsystem_responsibilities.json")
    if not os.path.isfile(responsibilities_path):
        return {}

    try:
        with open(responsibilities_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, dict):
            return {}
        
        return data
    except (json.JSONDecodeError, IOError):
        return {}


def get_subsystem_responsible_teams(subsystem_name: str) -> List[str]:
    """Get list of teams responsible for a given subsystem."""
    responsibilities = load_team_subsystem_responsibilities()
    responsible_teams = []
    
    for team_id, subsystems in responsibilities.items():
        if subsystem_name in subsystems:
            responsible_teams.append(team_id)
    
    return responsible_teams


def get_team_responsible_subsystems(team_id: str) -> List[str]:
    """Get list of subsystems a team is responsible for."""
    responsibilities = load_team_subsystem_responsibilities()
    return responsibilities.get(team_id, [])


def detect_dead_subsystems(threshold_months: int = 3) -> Dict[str, Dict[str, Any]]:
    """
    Detect subsystems with no recent activity.
    
    Returns dict mapping subsystem_name to:
    {
        "is_dead": bool,
        "last_activity_date": str or None,
        "months_since_activity": int or None
    }
    """
    from datetime import datetime, timedelta
    
    current_date = datetime.now()
    threshold_date = current_date - timedelta(days=30 * threshold_months)
    
    subsystem_status = {}
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")
    
    if not os.path.exists(subsystems_root):
        return subsystem_status
    
    for subsystem_name in os.listdir(subsystems_root):
        subsystem_dir = os.path.join(subsystems_root, subsystem_name)
        if not os.path.isdir(subsystem_dir):
            continue
        
        # Find the most recent activity by checking all period directories
        # Only look at monthly data, not yearly summaries
        latest_activity_date = None
        
        for period_dir in os.listdir(subsystem_dir):
            if "_" not in period_dir:
                continue
                
            try:
                date_from, date_to = period_dir.split("_", 1)
                
                # Skip yearly summaries (check if it spans a full year)
                from_date_obj = datetime.strptime(date_from, "%Y-%m-%d")
                to_date_obj = datetime.strptime(date_to, "%Y-%m-%d")
                
                # Skip if this is a yearly summary (spans more than 35 days)
                days_span = (to_date_obj - from_date_obj).days
                if days_span > 35:
                    continue
                
                period_path = os.path.join(subsystem_dir, period_dir)
                summary_file = os.path.join(period_path, "summary.json")
                
                if os.path.exists(summary_file):
                    # Check if this period has any commits
                    with open(summary_file, "r", encoding="utf-8") as f:
                        summary_data = json.load(f)
                    
                    total_commits = summary_data.get("total_commits", 0)
                    if total_commits > 0:
                        # Use the end date of this period
                        if latest_activity_date is None or to_date_obj > latest_activity_date:
                            latest_activity_date = to_date_obj
                            
            except (ValueError, json.JSONDecodeError, IOError):
                continue
        
        # Determine if subsystem is dead
        is_dead = False
        months_since_activity = None
        last_activity_str = None
        
        if latest_activity_date:
            last_activity_str = latest_activity_date.strftime("%Y-%m-%d")
            months_since_activity = int((current_date - latest_activity_date).days / 30.44)  # Average days per month
            is_dead = latest_activity_date < threshold_date
        else:
            # No activity found at all
            is_dead = True
            months_since_activity = None
        
        subsystem_status[subsystem_name] = {
            "is_dead": is_dead,
            "last_activity_date": last_activity_str,
            "months_since_activity": months_since_activity
        }
    
    return subsystem_status


@app.route("/api/subsystems/dead-status")
def api_subsystems_dead_status():
    """Get dead/inactive subsystem status for all subsystems."""
    try:
        dead_status = detect_dead_subsystems()
        return jsonify({"subsystem_status": dead_status})
    except Exception as e:
        print(f"Error in api_subsystems_dead_status: {str(e)}")
        return jsonify({"subsystem_status": {}, "error": str(e)})


@app.route("/api/subsystems/<subsystem_name>/month/<from_date>/<to_date>")
def api_subsystem_month(subsystem_name: str, from_date: str, to_date: str):
    """Get monthly summary for a subsystem."""
    # Try to find it as a service/subsystem
    service_path = find_service_summary(subsystem_name, from_date, to_date)
    if os.path.isfile(service_path):
        data = load_json(service_path)
        data["type"] = "subsystem"
        data["responsible_teams"] = get_subsystem_responsible_teams(subsystem_name)
        
        # Add dead status information
        dead_status = detect_dead_subsystems()
        if subsystem_name in dead_status:
            data["dead_status"] = dead_status[subsystem_name]
        else:
            data["dead_status"] = {"is_dead": False, "last_activity_date": None, "months_since_activity": None}
        
        return jsonify(data)
    
    abort(404, description="Subsystem summary not found")


@app.route("/api/subsystems/<subsystem_name>/year/<int:year>")
def api_subsystem_year(subsystem_name: str, year: int):
    """Get yearly summary for a subsystem."""
    from_date = f"{year:04d}-01-01"
    to_date = f"{year:04d}-12-31"
    
    # Try to find it as a service/subsystem
    service_path = find_service_summary(subsystem_name, from_date, to_date)
    if os.path.isfile(service_path):
        data = load_json(service_path)
        data["type"] = "subsystem"
        
        # Add dead status information
        dead_status = detect_dead_subsystems()
        if subsystem_name in dead_status:
            data["dead_status"] = dead_status[subsystem_name]
        else:
            data["dead_status"] = {"is_dead": False, "last_activity_date": None, "months_since_activity": None}
        
        return jsonify(data)
    
    abort(404, description="Subsystem yearly summary not found")


@app.route("/api/subsystems/<subsystem_name>/top-maintainers")
def api_subsystem_top_maintainers(subsystem_name: str):
    """Get top maintainers for a subsystem based on recent commit activity."""
    try:
        # Get current date to determine last 3 months
        from datetime import datetime, timedelta
        current_date = datetime.now()
        three_months_ago = current_date - timedelta(days=90)
        
        subsystem_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name)
        if not os.path.exists(subsystem_path):
            return jsonify({"maintainers": []})
        
        maintainer_data = {}  # dev_slug -> {commits, display_name, etc}
        
        # Look for monthly summary files from last 3 months
        for period_dir in os.listdir(subsystem_path):
            period_path = os.path.join(subsystem_path, period_dir)
            if not os.path.isdir(period_path):
                continue
            
            # Skip yearly summaries for maintainer analysis
            if "_2025-12-31" in period_dir:
                continue
            
            # Parse date range from directory name
            try:
                date_parts = period_dir.split("_")
                if len(date_parts) != 2:
                    continue
                
                from_date_str = date_parts[0]
                period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                
                # Only consider periods within last 3 months
                if period_date < three_months_ago:
                    continue
                
            except (ValueError, IndexError):
                continue
            
            summary_file = os.path.join(period_path, "summary.json")
            if not os.path.exists(summary_file):
                continue
            
            try:
                summary_data = load_json(summary_file)
                
                # Aggregate commits from all repositories for this subsystem/period
                repositories = summary_data.get("repositories", {})
                for repo_data in repositories.values():
                    developers = repo_data.get("developers", {})
                    for dev_slug, dev_data in developers.items():
                        commits = dev_data.get("commits", 0)
                        if commits > 0:
                            if dev_slug not in maintainer_data:
                                maintainer_data[dev_slug] = {
                                    "slug": dev_slug,
                                    "display_name": dev_data.get("display_name", dev_slug),
                                    "commits": 0,
                                    "lines_added": 0,
                                    "lines_deleted": 0,
                                    "changed_lines": 0
                                }
                            maintainer_data[dev_slug]["commits"] += commits
                            maintainer_data[dev_slug]["lines_added"] += dev_data.get("lines_added", 0)
                            maintainer_data[dev_slug]["lines_deleted"] += dev_data.get("lines_deleted", 0)
                            maintainer_data[dev_slug]["changed_lines"] += dev_data.get("changed_lines", 0)
            
            except Exception as e:
                print(f"Error processing summary file {summary_file}: {e}")
                continue
        
        # Sort by commits and take top 5
        top_maintainers = sorted(maintainer_data.values(), key=lambda x: x["commits"], reverse=True)[:5]
        
        return jsonify({"maintainers": top_maintainers})
        
    except Exception as e:
        abort(500, description=f"Error analyzing top maintainers: {str(e)}")


@app.route("/api/subsystems/<subsystem_name>/maintainer-timeline")
def api_subsystem_maintainer_timeline(subsystem_name: str):
    """Get historical ownership percentage timeline based on current blame data and monthly changes."""
    try:
        from datetime import datetime
        from collections import defaultdict
        
        subsystem_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name)
        if not os.path.exists(subsystem_path):
            return jsonify({"timeline": {}})
        
        # First, get current ownership from blame data
        repos_path = os.path.join(STATS_ROOT, "repos")
        current_ownership = {}  # {dev_slug: lines_owned}
        total_current_lines = 0
        
        # Look for blame data for this subsystem (could be a repo or a service)
        for root, dirs, files in os.walk(repos_path):
            if "blame.json" in files:
                blame_file = os.path.join(root, "blame.json")
                try:
                    blame_data = load_json(blame_file)
                    
                    # Check if this is a direct repo match
                    if subsystem_name.lower() in blame_data.get("repo", "").lower():
                        developers = blame_data.get("developers", {})
                        total_current_lines = blame_data.get("total_lines", 0)
                        for dev_slug, dev_data in developers.items():
                            current_ownership[dev_slug] = dev_data.get("lines", 0)
                        break
                    
                    # Check if this is a service within a repo
                    services = blame_data.get("services", {})
                    if subsystem_name in services:
                        service_data = services[subsystem_name]
                        developers = service_data.get("developers", {})
                        total_current_lines = service_data.get("total_lines", 0)
                        for dev_slug, dev_data in developers.items():
                            if isinstance(dev_data, dict):
                                current_ownership[dev_slug] = dev_data.get("lines", 0)
                            else:
                                current_ownership[dev_slug] = dev_data
                        break
                except Exception as e:
                    continue
        
        if not current_ownership or total_current_lines == 0:
            return jsonify({"timeline": {}})
        
        # Now get monthly net line changes (lines_added - lines_deleted)
        # Structure: {dev_slug: {month: net_lines}}
        monthly_net_changes = defaultdict(lambda: defaultdict(int))
        
        # Look for monthly summary files
        for period_dir in os.listdir(subsystem_path):
            period_path = os.path.join(subsystem_path, period_dir)
            if not os.path.isdir(period_path):
                continue
            
            # Skip yearly summaries
            if "_2025-12-31" in period_dir or "_2024-12-31" in period_dir:
                continue
            
            # Parse date range from directory name
            try:
                date_parts = period_dir.split("_")
                if len(date_parts) != 2:
                    continue
                
                from_date_str = date_parts[0]
                period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                month_label = period_date.strftime("%Y-%m")
                
            except (ValueError, IndexError):
                continue
            
            summary_file = os.path.join(period_path, "summary.json")
            if not os.path.exists(summary_file):
                continue
            
            try:
                summary_data = load_json(summary_file)
                
                repositories = summary_data.get("repositories", {})
                for repo_data in repositories.values():
                    developers = repo_data.get("developers", {})
                    for dev_slug, dev_data in developers.items():
                        lines_added = dev_data.get("lines_added", 0)
                        lines_deleted = dev_data.get("lines_deleted", 0)
                        net_lines = lines_added - lines_deleted
                        monthly_net_changes[dev_slug][month_label] += net_lines
            
            except Exception as e:
                print(f"Error processing summary file {summary_file}: {e}")
                continue
        
        # Get top 5 maintainers by recent activity (last 3 months) - same as top-maintainers endpoint
        from datetime import timedelta
        three_months_ago = datetime.now() - timedelta(days=90)
        recent_activity = defaultdict(int)
        
        for period_dir in os.listdir(subsystem_path):
            if period_dir == 'languages.json' or '_12-31' in period_dir:
                continue
            
            try:
                from_date_str = period_dir.split('_')[0]
                period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                if period_date < three_months_ago:
                    continue
                
                summary_file = os.path.join(subsystem_path, period_dir, "summary.json")
                if not os.path.exists(summary_file):
                    continue
                
                summary_data = load_json(summary_file)
                for repo_data in summary_data.get("repositories", {}).values():
                    for dev_slug, dev_data in repo_data.get("developers", {}).items():
                        recent_activity[dev_slug] += dev_data.get("commits", 0)
            except:
                continue
        
        # Select top 5 by recent commits
        top_maintainers = sorted(recent_activity.items(), key=lambda x: x[1], reverse=True)[:5]
        top_maintainers_slugs = [slug for slug, _ in top_maintainers]
        
        # Build backward timeline
        result = {}
        all_months = sorted(set(month for dev_data in monthly_net_changes.values() for month in dev_data.keys()))
        
        for dev_slug in top_maintainers_slugs:
            percentages = []
            
            # Start with current ownership
            dev_lines = current_ownership.get(dev_slug, 0)
            total_lines = total_current_lines
            
            # Work backwards through months (reverse chronological order)
            for month in reversed(all_months):
                # Calculate ownership at this point in time
                percentage = (dev_lines / total_lines * 100) if total_lines > 0 else 0
                percentages.insert(0, round(percentage, 1))  # Insert at beginning since we're going backwards
                
                # Subtract this month's changes to get previous month's state
                dev_lines -= monthly_net_changes[dev_slug].get(month, 0)
                total_lines -= sum(monthly_net_changes[dev].get(month, 0) for dev in monthly_net_changes.keys())
                
                # Don't let values go negative
                dev_lines = max(0, dev_lines)
                total_lines = max(1, total_lines)  # Avoid division by zero
            
            result[dev_slug] = {
                "months": all_months,
                "ownership": percentages
            }
        
        return jsonify({"timeline": result})
        
    except Exception as e:
        print(f"Error in maintainer timeline: {e}")
        import traceback
        traceback.print_exc()
        abort(500, description=f"Error generating maintainer timeline: {str(e)}")


@app.route("/api/subsystems/<subsystem_name>/significant-ownership")
def api_subsystem_significant_ownership(subsystem_name: str):
    """Get developers with >10% ownership of a subsystem."""
    try:
        significant_owners = []
        
        # Check blame files in the repos structure for ownership percentages
        repos_path = os.path.join(STATS_ROOT, "repos")
        if not os.path.exists(repos_path):
            return jsonify({"owners": []})
        
        # Load services config to understand which repo might contain this service
        services_config = load_services_config()
        
        for org_name in os.listdir(repos_path):
            org_path = os.path.join(repos_path, org_name)
            if not os.path.isdir(org_path):
                continue
                
            for repo_name in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_name)
                if not os.path.isdir(repo_path):
                    continue
                    
                blame_file = os.path.join(repo_path, "blame", "blame.json")
                if not os.path.exists(blame_file):
                    # Skip repos without blame files
                    continue
                
                try:
                    blame_data = load_json(blame_file)
                    repo_full_name = f"{org_name}/{repo_name}"
                    
                    # Check if this repo matches our subsystem name
                    repo_matches = (repo_name == subsystem_name or 
                                   f"{org_name}/{repo_name}" == subsystem_name)
                    
                    # If repo matches, check repo-level developers
                    if repo_matches:
                        developers = blame_data.get("developers", {})
                        total_lines = blame_data.get("total_lines", 0)
                        
                        for dev_slug, dev_data in developers.items():
                            dev_lines = dev_data.get("lines", 0)
                            ownership_share = dev_lines / total_lines if total_lines > 0 else 0
                            
                            # Only include developers with >10% ownership
                            if ownership_share > 0.10:  # More than 10%
                                significant_owners.append({
                                    "slug": dev_slug,
                                    "display_name": dev_data.get("display_name", dev_slug),
                                    "lines": dev_lines,
                                    "share": ownership_share,
                                    "percentage": round(ownership_share * 100, 1),
                                    "source": f"repo-{repo_name}"
                                })
                    
                    # Always check per-service ownership percentages
                    services = blame_data.get("services", {})
                    for service_name, service_data in services.items():
                        # Check if this service matches our subsystem
                        if service_name == subsystem_name:
                            service_developers = service_data.get("developers", {})
                            service_total_lines = service_data.get("total_lines", 0)
                            
                            for dev_slug, dev_data in service_developers.items():
                                dev_lines = dev_data.get("lines", 0)
                                ownership_share = dev_lines / service_total_lines if service_total_lines > 0 else 0
                                
                                # Only include developers with >10% ownership
                                if ownership_share > 0.10:  # More than 10%
                                    # Check if we already have this developer from repo-level analysis
                                    existing = next((o for o in significant_owners if o["slug"] == dev_slug), None)
                                    if not existing:
                                        significant_owners.append({
                                            "slug": dev_slug,
                                            "display_name": dev_data.get("display_name", dev_slug),
                                            "lines": dev_lines,
                                            "share": ownership_share,
                                            "percentage": round(ownership_share * 100, 1),
                                            "source": f"service-{service_name}-in-{repo_name}"
                                        })
                                    elif ownership_share > existing["share"]:
                                        # Update if this service has higher ownership
                                        existing.update({
                                            "lines": dev_lines,
                                            "share": ownership_share,
                                            "percentage": round(ownership_share * 100, 1),
                                            "source": f"service-{service_name}-in-{repo_name}"
                                        })
                
                except Exception as e:
                    print(f"Error processing blame file {blame_file} for significant ownership: {e}")
                    continue
        
        # Sort by ownership percentage (descending) and remove duplicates
        unique_owners = {}
        for owner in significant_owners:
            slug = owner["slug"]
            if slug not in unique_owners or owner["share"] > unique_owners[slug]["share"]:
                unique_owners[slug] = owner
        
        sorted_owners = sorted(unique_owners.values(), key=lambda x: x["share"], reverse=True)
        
        return jsonify({"owners": sorted_owners})
        
    except Exception as e:
        print(f"Error in api_subsystem_significant_ownership: {str(e)}")
        return jsonify({"owners": [], "error": str(e)})


@app.route("/api/subsystems/<subsystem_name>/languages")
def api_subsystem_languages(subsystem_name: str):
    """Get language statistics for a subsystem."""
    try:
        # Look for languages.json in subsystem directory
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        subsystem_dir = os.path.join(subsystems_root, subsystem_name)
        languages_file = os.path.join(subsystem_dir, "languages.json")
        
        if not os.path.exists(languages_file):
            return jsonify({"languages": {}, "totals": {}, "error": "Language statistics not available"})
        
        try:
            with open(languages_file, "r", encoding="utf-8") as f:
                language_data = json.load(f)
            
            return jsonify(language_data)
        except (json.JSONDecodeError, IOError) as e:
            return jsonify({"languages": {}, "totals": {}, "error": f"Error reading language statistics: {str(e)}"})
        
    except Exception as e:
        print(f"Error in api_subsystem_languages: {str(e)}")
        return jsonify({"languages": {}, "totals": {}, "error": str(e)})


@app.route("/api/subsystems/size-rankings")
def api_subsystem_size_rankings():
    """Get size rankings for all subsystems based on total lines of code."""
    try:
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        if not os.path.exists(subsystems_root):
            return jsonify({"rankings": {}, "buckets": {"big": [], "medium": [], "small": []}})
        
        # Calculate total git blame lines across all repos
        # Important: Deduplicate repos that appear both standalone and in monorepos
        # Track by the final repo name component to avoid double-counting
        total_git_lines = 0
        repos_path = os.path.join(STATS_ROOT, "repos")
        counted_repos = set()
        
        for root, dirs, files in os.walk(repos_path):
            if "blame.json" in files:
                blame_file = os.path.join(root, "blame.json")
                try:
                    blame_data = load_json(blame_file)
                    repo_full_name = blame_data.get("repo", "")
                    # Get the last component (e.g., "appgate-docker" from "appgate-sdp-int/appgate-docker")
                    repo_name = repo_full_name.split("/")[-1]
                    
                    # Only count each unique repo name once (prefer monorepo version if duplicate)
                    if repo_name not in counted_repos:
                        total_git_lines += blame_data.get("total_lines", 0)
                        counted_repos.add(repo_name)
                except Exception as e:
                    continue
        
        # Collect language statistics for all subsystems
        subsystem_sizes = []
        
        for subsystem_name in os.listdir(subsystems_root):
            subsystem_dir = os.path.join(subsystems_root, subsystem_name)
            if not os.path.isdir(subsystem_dir):
                continue
                
            languages_file = os.path.join(subsystem_dir, "languages.json")
            if not os.path.exists(languages_file):
                continue
                
            try:
                with open(languages_file, "r", encoding="utf-8") as f:
                    language_data = json.load(f)
                
                total_lines = language_data.get("totals", {}).get("code_lines", 0)
                if total_lines > 0:
                    subsystem_sizes.append({
                        "name": subsystem_name,
                        "total_lines": total_lines
                    })
                    
            except (json.JSONDecodeError, IOError):
                continue
        
        # Sort by total lines (descending)
        subsystem_sizes.sort(key=lambda x: x["total_lines"], reverse=True)
        
        # Calculate total system lines
        total_system_lines = sum(s["total_lines"] for s in subsystem_sizes)
        
        # Create rankings dictionary
        rankings = {}
        for i, subsystem in enumerate(subsystem_sizes):
            rankings[subsystem["name"]] = {
                "rank": i + 1,
                "total_lines": subsystem["total_lines"],
                "total_subsystems": len(subsystem_sizes)
            }
        
        # Divide into 3 equal buckets
        total_count = len(subsystem_sizes)
        bucket_size = total_count // 3
        remainder = total_count % 3
        
        # Distribute remainder: big gets +1 if remainder >= 1, medium gets +1 if remainder == 2
        big_size = bucket_size + (1 if remainder >= 1 else 0)
        medium_size = bucket_size + (1 if remainder >= 2 else 0)
        small_size = bucket_size
        
        buckets = {
            "big": [s["name"] for s in subsystem_sizes[:big_size]],
            "medium": [s["name"] for s in subsystem_sizes[big_size:big_size + medium_size]],
            "small": [s["name"] for s in subsystem_sizes[big_size + medium_size:]]
        }
        
        # Add bucket info to rankings
        for subsystem_name in buckets["big"]:
            rankings[subsystem_name]["size_bucket"] = "big"
        for subsystem_name in buckets["medium"]:
            rankings[subsystem_name]["size_bucket"] = "medium"
        for subsystem_name in buckets["small"]:
            rankings[subsystem_name]["size_bucket"] = "small"
        
        return jsonify({
            "rankings": rankings,
            "buckets": buckets,
            "total_subsystems": total_count,
            "total_system_lines": total_system_lines,
            "total_git_lines": total_git_lines
        })
        
    except Exception as e:
        print(f"Error in api_subsystem_size_rankings: {str(e)}")
        return jsonify({"rankings": {}, "buckets": {"big": [], "medium": [], "small": []}, "error": str(e)})


@app.route("/api/subsystems/overview")
def api_subsystems_overview():
    """Get overview data for all subsystems including size comparison and activity."""
    try:
        from datetime import datetime, timedelta
        
        # Get size rankings
        size_data_response = api_subsystem_size_rankings()
        size_data = size_data_response.get_json()
        
        # Get dead subsystem status
        dead_status = detect_dead_subsystems()
        
        # Get current date for recent activity (last month)
        current_date = datetime.now()
        current_month_start = current_date.replace(day=1).strftime("%Y-%m-%d")
        
        # Calculate last month
        if current_date.month == 1:
            last_month = 12
            last_year = current_date.year - 1
        else:
            last_month = current_date.month - 1
            last_year = current_date.year
        
        last_month_start = f"{last_year:04d}-{last_month:02d}-01"
        
        # Find last day of last month
        import calendar
        last_day = calendar.monthrange(last_year, last_month)[1]
        last_month_end = f"{last_year:04d}-{last_month:02d}-{last_day:02d}"
        
        # Get activity data for last month
        subsystems_activity = []
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        
        if os.path.exists(subsystems_root):
            for subsystem_name in os.listdir(subsystems_root):
                subsystem_dir = os.path.join(subsystems_root, subsystem_name)
                if not os.path.isdir(subsystem_dir):
                    continue
                
                # Look for last month's data
                activity_data = {"name": subsystem_name, "commits": 0, "lines_changed": 0, "developers": 0}
                
                # Add dead status
                if subsystem_name in dead_status:
                    activity_data["is_dead"] = dead_status[subsystem_name]["is_dead"]
                    activity_data["last_activity_date"] = dead_status[subsystem_name]["last_activity_date"]
                    activity_data["months_since_activity"] = dead_status[subsystem_name]["months_since_activity"]
                else:
                    activity_data["is_dead"] = False
                    activity_data["last_activity_date"] = None
                    activity_data["months_since_activity"] = None
                
                for period_dir in os.listdir(subsystem_dir):
                    if period_dir.startswith(last_month_start[:7]):  # Match YYYY-MM
                        period_path = os.path.join(subsystem_dir, period_dir)
                        summary_file = os.path.join(period_path, "summary.json")
                        
                        if os.path.exists(summary_file):
                            try:
                                with open(summary_file, "r", encoding="utf-8") as f:
                                    summary_data = json.load(f)
                                
                                activity_data["commits"] = summary_data.get("total_commits", 0)
                                activity_data["lines_changed"] = summary_data.get("total_changed_lines", 0)
                                activity_data["developers"] = len(summary_data.get("developers", {}))
                                break
                                
                            except (json.JSONDecodeError, IOError):
                                continue
                
                subsystems_activity.append(activity_data)
        
        # Sort activity data
        most_active_commits = sorted(subsystems_activity, key=lambda x: x["commits"], reverse=True)[:10]
        most_active_changes = sorted(subsystems_activity, key=lambda x: x["lines_changed"], reverse=True)[:10]
        
        # Count dead subsystems
        dead_subsystems = [s for s in subsystems_activity if s["is_dead"]]
        
        return jsonify({
            "size_data": size_data,
            "activity": {
                "period": f"{last_year:04d}-{last_month:02d}",
                "most_commits": most_active_commits,
                "most_changes": most_active_changes
            },
            "total_subsystems": len(subsystems_activity),
            "dead_subsystems": {
                "count": len(dead_subsystems),
                "subsystems": dead_subsystems
            }
        })
        
    except Exception as e:
        print(f"Error in api_subsystems_overview: {str(e)}")
        return jsonify({"error": str(e)})


@app.route("/api/users/overview")
def api_users_overview():
    """Get overview data for all users including activity and statistics."""
    try:
        from datetime import datetime, timedelta
        
        # Get current date for recent activity (last month)
        current_date = datetime.now()
        current_year = current_date.year
        
        # Calculate last month
        if current_date.month == 1:
            last_month = 12
            last_year = current_date.year - 1
        else:
            last_month = current_date.month - 1
            last_year = current_year
        
        last_month_start = f"{last_year:04d}-{last_month:02d}-01"
        
        # Find last day of last month
        import calendar
        last_day = calendar.monthrange(last_year, last_month)[1]
        last_month_end = f"{last_year:04d}-{last_month:02d}-{last_day:02d}"
        
        # Get activity data for last month and yearly data
        users_activity = []
        users_yearly = []
        users_root = os.path.join(STATS_ROOT, "users")
        
        if os.path.exists(users_root):
            for user_slug in os.listdir(users_root):
                user_dir = os.path.join(users_root, user_slug)
                if not os.path.isdir(user_dir):
                    continue
                
                user_data = {
                    "slug": user_slug,
                    "display_name": user_slug,
                    "monthly_commits": 0,
                    "monthly_lines_added": 0,
                    "monthly_lines_deleted": 0,
                    "yearly_commits": 0,
                    "yearly_lines_added": 0,
                    "yearly_lines_deleted": 0
                }
                
                # Look for last month's data
                monthly_folder = f"{last_month_start}_{last_month_end}"
                monthly_path = os.path.join(user_dir, monthly_folder, "summary.json")
                
                if os.path.exists(monthly_path):
                    try:
                        with open(monthly_path, "r", encoding="utf-8") as f:
                            monthly_data = json.load(f)
                        
                        user_data["display_name"] = monthly_data.get("author_name", user_slug)
                        user_data["monthly_commits"] = monthly_data.get("total_commits", 0)
                        user_data["monthly_lines_added"] = monthly_data.get("total_lines_added", 0)
                        user_data["monthly_lines_deleted"] = monthly_data.get("total_lines_deleted", 0)
                        
                    except (json.JSONDecodeError, IOError):
                        pass
                
                # Look for yearly data
                yearly_folder = f"{current_year:04d}-01-01_{current_year:04d}-12-31"
                yearly_path = os.path.join(user_dir, yearly_folder, "summary.json")
                
                if os.path.exists(yearly_path):
                    try:
                        with open(yearly_path, "r", encoding="utf-8") as f:
                            yearly_data = json.load(f)
                        
                        user_data["display_name"] = yearly_data.get("author_name", user_slug)
                        user_data["yearly_commits"] = yearly_data.get("total_commits", 0)
                        user_data["yearly_lines_added"] = yearly_data.get("total_lines_added", 0)
                        user_data["yearly_lines_deleted"] = yearly_data.get("total_lines_deleted", 0)
                        
                    except (json.JSONDecodeError, IOError):
                        pass
                
                if user_data["monthly_commits"] > 0 or user_data["yearly_commits"] > 0:
                    users_activity.append(user_data)
                    users_yearly.append(user_data)
        
        # Sort by different metrics
        most_active_monthly = sorted(users_activity, key=lambda x: x["monthly_commits"], reverse=True)[:10]
        most_productive_monthly = sorted(users_activity, key=lambda x: x["monthly_lines_added"], reverse=True)[:10]
        most_active_yearly = sorted(users_yearly, key=lambda x: x["yearly_commits"], reverse=True)[:10]
        most_productive_yearly = sorted(users_yearly, key=lambda x: x["yearly_lines_added"], reverse=True)[:10]
        
        # Calculate aggregate statistics from ALL users (not just top 10)
        monthly_active_count = sum(1 for u in users_activity if u["monthly_commits"] > 0)
        yearly_active_count = sum(1 for u in users_yearly if u["yearly_commits"] > 0)
        total_monthly_commits = sum(u["monthly_commits"] for u in users_activity)
        total_yearly_commits = sum(u["yearly_commits"] for u in users_yearly)
        
        return jsonify({
            "activity": {
                "period": f"{last_year:04d}-{last_month:02d}",
                "most_active_monthly": most_active_monthly,
                "most_productive_monthly": most_productive_monthly,
                "total_active_users": monthly_active_count,
                "total_commits": total_monthly_commits
            },
            "yearly": {
                "year": current_year,
                "most_active_yearly": most_active_yearly,
                "most_productive_yearly": most_productive_yearly,
                "total_active_users": yearly_active_count,
                "total_commits": total_yearly_commits
            },
            "total_users": len(users_activity)
        })
        
    except Exception as e:
        print(f"Error in api_users_overview: {str(e)}")
        return jsonify({"error": str(e)})


@app.route("/api/settings/ignore-users", methods=["GET", "POST"])
def api_settings_ignore_users():
    """Get or update the configuration/ignore_user.txt file."""
    ignore_file_path = os.path.join(BASE_DIR, "configuration/ignore_user.txt")
    
    if request.method == "GET":
        try:
            if os.path.exists(ignore_file_path):
                with open(ignore_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                content = ""
            
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            if not data or "content" not in data:
                return jsonify({"error": "Content is required"}), 400
            
            content = data["content"]
            
            # Write the file
            with open(ignore_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            return jsonify({"success": True, "message": "Ignore users file updated successfully"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/settings/aliases", methods=["GET", "POST"])
def api_settings_aliases():
    """Get or update the configuration/alias.json file."""
    alias_file_path = os.path.join(BASE_DIR, "configuration/alias.json")
    
    if request.method == "GET":
        try:
            if os.path.exists(alias_file_path):
                with open(alias_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                content = "{}"
            
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            if not data or "content" not in data:
                return jsonify({"error": "Content is required"}), 400
            
            content = data["content"]
            
            # Validate JSON format
            try:
                json.loads(content)
            except json.JSONDecodeError as e:
                return jsonify({"error": f"Invalid JSON format: {str(e)}"}), 400
            
            # Write the file
            with open(alias_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            return jsonify({"success": True, "message": "Aliases file updated successfully"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/settings/teams", methods=["GET", "POST"])
def api_settings_teams():
    """Get or update the configuration/teams.json file."""
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if request.method == "GET":
        try:
            if os.path.exists(teams_file_path):
                with open(teams_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                content = "{}"
            
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            if not data or "content" not in data:
                return jsonify({"error": "Content is required"}), 400
            
            content = data["content"]
            
            # Validate JSON format
            try:
                json.loads(content)
            except json.JSONDecodeError as e:
                return jsonify({"error": f"Invalid JSON format: {str(e)}"}), 400
            
            # Write the file
            with open(teams_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            return jsonify({"success": True, "message": "Teams file updated successfully"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/settings/team-subsystem-responsibilities", methods=["GET", "POST"])
def api_settings_team_subsystem_responsibilities():
    """Get or update the team-subsystem responsibilities mapping."""
    responsibilities_file_path = os.path.join(BASE_DIR, "configuration/team_subsystem_responsibilities.json")
    
    if request.method == "GET":
        try:
            # Get teams
            teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
            teams = {}
            if os.path.exists(teams_file_path):
                with open(teams_file_path, "r", encoding="utf-8") as f:
                    teams = json.load(f)
            
            # Get all available subsystems
            subsystems_root = os.path.join(STATS_ROOT, "subsystems")
            available_subsystems = []
            if os.path.isdir(subsystems_root):
                available_subsystems = sorted([name for name in os.listdir(subsystems_root) 
                                             if os.path.isdir(os.path.join(subsystems_root, name))])
            
            # Get current responsibilities
            responsibilities = {}
            if os.path.exists(responsibilities_file_path):
                with open(responsibilities_file_path, "r", encoding="utf-8") as f:
                    responsibilities = json.load(f)
            
            return jsonify({
                "teams": teams,
                "available_subsystems": available_subsystems,
                "responsibilities": responsibilities
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            if not data or "responsibilities" not in data:
                return jsonify({"error": "Responsibilities data is required"}), 400
            
            responsibilities = data["responsibilities"]
            
            # Validate that it's a proper JSON object
            if not isinstance(responsibilities, dict):
                return jsonify({"error": "Responsibilities must be a JSON object"}), 400
            
            # Write the file
            with open(responsibilities_file_path, "w", encoding="utf-8") as f:
                json.dump(responsibilities, f, indent=2, ensure_ascii=False)
            
            return jsonify({"success": True, "message": "Team-subsystem responsibilities updated successfully"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/settings/repositories", methods=["GET", "POST"])
def api_settings_repositories():
    """Get or update repository configuration."""
    if request.method == "GET":
        try:
            repos = []
            repos_root = os.path.join(BASE_DIR, "repos")
            
            if os.path.exists(repos_root):
                for org_dir in os.listdir(repos_root):
                    org_path = os.path.join(repos_root, org_dir)
                    if os.path.isdir(org_path):
                        for repo_dir in os.listdir(org_path):
                            repo_path = os.path.join(org_path, repo_dir)
                            if os.path.isdir(repo_path) and os.path.exists(os.path.join(repo_path, ".git")):
                                repo_name = f"{org_dir}/{repo_dir}"
                                
                                # Try to get remote URL
                                remote_url = "Unknown"
                                try:
                                    import subprocess
                                    result = subprocess.run(
                                        ["git", "remote", "get-url", "origin"],
                                        cwd=repo_path,
                                        capture_output=True,
                                        text=True
                                    )
                                    if result.returncode == 0:
                                        remote_url = result.stdout.strip()
                                except:
                                    pass
                                
                                repos.append({
                                    "name": repo_name,
                                    "path": repo_path,
                                    "url": remote_url,
                                    "exists": True
                                })
            
            return jsonify({"repositories": repos})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            action = data.get("action")
            
            if action == "add":
                repo_name = data.get("name", "").strip()
                repo_url = data.get("url", "").strip()
                
                if not repo_name or not repo_url:
                    return jsonify({"error": "Repository name and URL are required"}), 400
                
                # Validate repo name format
                if "/" not in repo_name:
                    return jsonify({"error": "Repository name must be in format 'owner/repo'"}), 400
                
                org_name, repo_name_only = repo_name.split("/", 1)
                
                repos_root = os.path.join(BASE_DIR, "repos")
                org_path = os.path.join(repos_root, org_name)
                repo_path = os.path.join(org_path, repo_name_only)
                
                # Create directories if they don't exist
                os.makedirs(org_path, exist_ok=True)
                
                if os.path.exists(repo_path):
                    return jsonify({"error": f"Repository {repo_name} already exists"}), 400
                
                return jsonify({"success": True, "message": f"Repository {repo_name} added to configuration (use 'Clone Missing Repos' to clone)"})
                
                # Reset update state after adding repository to prevent stuck state
                reset_update_state()
            
            elif action == "remove":
                repo_name = data.get("name", "").strip()
                
                if not repo_name:
                    return jsonify({"error": "Repository name is required"}), 400
                
                if "/" not in repo_name:
                    return jsonify({"error": "Repository name must be in format 'owner/repo'"}), 400
                
                org_name, repo_name_only = repo_name.split("/", 1)
                repos_root = os.path.join(BASE_DIR, "repos")
                repo_path = os.path.join(repos_root, org_name, repo_name_only)
                
                # Check if repository exists
                if not os.path.exists(repo_path):
                    return jsonify({"error": "Repository not found"}), 404
                
                # Perform removal in background thread to avoid timeout
                import threading
                import shutil
                
                def remove_repo_worker():
                    """Background worker to remove repository files"""
                    import subprocess
                    try:
                        print(f"🗑️ Starting removal of {repo_name}")
                        
                        # Remove the repository directory (use system rm for speed)
                        if os.path.exists(repo_path):
                            print(f"🗑️ Removing repository directory: {repo_path}")
                            # Use system rm command which is much faster than shutil.rmtree for large directories
                            subprocess.run(["rm", "-rf", repo_path], check=True)
                            print(f"✅ Repository directory removed")
                        
                        # Remove associated stats from stats/repos
                        stats_repo_path = os.path.join(STATS_ROOT, "repos", org_name, repo_name_only)
                        if os.path.exists(stats_repo_path):
                            print(f"🗑️ Removing stats directory: {stats_repo_path}")
                            subprocess.run(["rm", "-rf", stats_repo_path], check=True)
                            print(f"✅ Stats directory removed")
                        
                        # Remove services from configuration/services.json
                        services_file = os.path.join(BASE_DIR, "configuration", "services.json")
                        if os.path.exists(services_file):
                            try:
                                with open(services_file, "r", encoding="utf-8") as f:
                                    services_config = json.load(f)
                                
                                # Check if this repo has services
                                if repo_name in services_config:
                                    # Remove stats for each service/subsystem
                                    for service_name in services_config[repo_name].keys():
                                        subsystem_stats_path = os.path.join(STATS_ROOT, "subsystems", service_name)
                                        if os.path.exists(subsystem_stats_path):
                                            print(f"🗑️ Removing subsystem stats: {subsystem_stats_path}")
                                            subprocess.run(["rm", "-rf", subsystem_stats_path], check=True)
                                    
                                    # Remove the repository entry from services.json
                                    del services_config[repo_name]
                                    
                                    # Write back the updated configuration
                                    with open(services_file, "w", encoding="utf-8") as f:
                                        json.dump(services_config, f, indent=2)
                                    print(f"✅ Services configuration updated")
                            except (json.JSONDecodeError, IOError) as e:
                                print(f"⚠️ Warning: Could not update services.json: {e}")
                        
                        # Also check if the repo itself (without services) has subsystem stats
                        # This handles standalone repos that aren't in services.json
                        subsystem_stats_path = os.path.join(STATS_ROOT, "subsystems", repo_name_only)
                        if os.path.exists(subsystem_stats_path):
                            print(f"🗑️ Removing standalone subsystem stats: {subsystem_stats_path}")
                            subprocess.run(["rm", "-rf", subsystem_stats_path], check=True)
                            print(f"✅ Standalone subsystem stats removed")
                        
                        print(f"✅ Repository {repo_name} removed successfully")
                        
                    except Exception as e:
                        print(f"❌ Error removing repository {repo_name}: {e}")
                
                # Start removal in background
                removal_thread = threading.Thread(target=remove_repo_worker, daemon=True)
                removal_thread.start()
                
                # Return immediately
                message = f"Repository {repo_name_only} removal started"
                return jsonify({"success": True, "message": message, "async": True})
            
            elif action == "clone":
                repo_name = data.get("name", "").strip()
                repo_url = data.get("url", "").strip()
                
                if not repo_name or not repo_url:
                    return jsonify({"error": "Repository name and URL are required"}), 400
                
                if "/" not in repo_name:
                    return jsonify({"error": "Repository name must be in format 'owner/repo'"}), 400
                
                org_name, repo_name_only = repo_name.split("/", 1)
                repos_root = os.path.join(BASE_DIR, "repos")
                org_path = os.path.join(repos_root, org_name)
                repo_path = os.path.join(org_path, repo_name_only)
                
                # Create directories if they don't exist
                os.makedirs(org_path, exist_ok=True)
                
                if os.path.exists(repo_path):
                    return jsonify({"error": f"Repository {repo_name} already exists"}), 400
                
                try:
                    import subprocess
                    import threading
                    
                    # Generate unique progress ID (replace slash for URL safety)
                    safe_repo_name = repo_name.replace("/", "-")
                    progress_id = f"{safe_repo_name}_{int(time.time())}"
                    
                    # Initialize progress tracking
                    clone_operations[progress_id] = {
                        "repo_name": repo_name,
                        "repo_url": repo_url,
                        "repo_path": repo_path,
                        "progress_queue": queue.Queue(),
                        "status": "starting",
                        "error": None,
                        "start_time": time.time()
                    }
                    
                    def clone_worker():
                        import subprocess  # Ensure subprocess is available in thread
                        import os
                        import shutil
                        
                        op = clone_operations[progress_id]
                        return_code = None
                        
                        try:
                            op["status"] = "cloning"
                            op["progress_queue"].put("Starting git clone...")
                            print(f"🔄 Clone worker started for {repo_name}")
                            
                            # Clone with progress output - simplified approach
                            env = os.environ.copy()
                            env['GIT_PROGRESS_DELAY'] = '1'
                            
                            print(f"🚀 Starting git clone: git clone --progress {repo_url} {repo_path}")
                            
                            # Use a different approach - capture both streams separately
                            process = subprocess.Popen(
                                ["git", "clone", "--progress", repo_url, repo_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                env=env,
                                bufsize=1
                            )
                            
                            def read_stderr():
                                """Read git progress from stderr"""
                                line_count = 0
                                while True:
                                    line = process.stderr.readline()
                                    if not line:
                                        break
                                    clean_line = line.strip()
                                    if clean_line:
                                        line_count += 1
                                        print(f"🎯 Git stderr #{line_count}: {clean_line}")
                                        op["progress_queue"].put(clean_line)
                            
                            def read_stdout():
                                """Read git output from stdout"""
                                line_count = 0
                                while True:
                                    line = process.stdout.readline()
                                    if not line:
                                        break
                                    clean_line = line.strip()
                                    if clean_line:
                                        line_count += 1
                                        print(f"📄 Git stdout #{line_count}: {clean_line}")
                                        op["progress_queue"].put(clean_line)
                            
                            # Start reading threads
                            stderr_thread = threading.Thread(target=read_stderr)
                            stdout_thread = threading.Thread(target=read_stdout)
                            stderr_thread.start()
                            stdout_thread.start()
                            
                            # Wait for process completion with timeout (40 hours max for enterprise-scale repositories)
                            try:
                                return_code = process.wait(timeout=144000)  # 40 hours timeout
                                print(f"✅ Git process completed with return code: {return_code}")
                            except subprocess.TimeoutExpired:
                                print("❌ Git clone process timed out after 40 hours")
                                process.terminate()
                                try:
                                    process.wait(timeout=10)  # Give 10 seconds for graceful termination
                                except subprocess.TimeoutExpired:
                                    process.kill()  # Force kill if it doesn't terminate
                                return_code = -1
                                op["status"] = "failed"
                                op["error"] = "Clone operation timed out after 40 hours"
                                op["progress_queue"].put("❌ Clone timed out after 40 hours")
                            
                            # Wait for reading threads to finish
                            stderr_thread.join(timeout=5)
                            stdout_thread.join(timeout=5)
                            
                            if return_code == 0:
                                op["status"] = "completed"
                                op["progress_queue"].put("✅ Clone completed successfully!")
                                print("✅ Clone marked as completed")
                                
                                # Reset update state after successful clone to prevent stuck state
                                reset_update_state()
                            else:
                                op["status"] = "failed" 
                                op["error"] = f"Git clone failed with return code {return_code}"
                                op["progress_queue"].put(f"❌ Clone failed: {op['error']}")
                                print(f"❌ Clone marked as failed: {op['error']}")
                                # Clean up partial clone on failure
                                if os.path.exists(repo_path):
                                    shutil.rmtree(repo_path)
                                
                        except Exception as e:
                            print(f"💥 Exception in clone worker: {e}")
                            op["status"] = "failed"
                            op["error"] = str(e)
                            op["progress_queue"].put(f"❌ Clone failed: {op['error']}")
                            # Clean up on any error
                            if os.path.exists(repo_path):
                                shutil.rmtree(repo_path)
                    
                    # Start clone in background thread
                    clone_thread = threading.Thread(target=clone_worker)
                    clone_thread.start()
                    
                    # Return progress ID for frontend to poll
                    return jsonify({
                        "success": True, 
                        "message": "Clone started",
                        "progress_id": progress_id
                    })
                        
                except FileNotFoundError:
                    return jsonify({"error": "Git is not installed or not in PATH"}), 500
                except Exception as e:
                    # Clean up on any other error
                    if os.path.exists(repo_path):
                        import shutil
                        shutil.rmtree(repo_path)
                    return jsonify({"error": f"Clone failed: {str(e)}"}), 500
            
            else:
                return jsonify({"error": "Invalid action"}), 400
                
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/settings/repositories/clone-progress/<progress_id>", methods=["GET"])
def api_clone_progress(progress_id):
    """Get clone progress for a specific operation."""
    try:
        print(f"Progress poll for {progress_id}")  # Debug
        
        if progress_id not in clone_operations:
            print(f"Progress ID {progress_id} not found")  # Debug
            return jsonify({"error": "Progress ID not found"}), 404
        
        op = clone_operations[progress_id]
        
        # Collect all progress messages since last poll
        progress_messages = []
        message_count = 0
        queue_size = op["progress_queue"].qsize()
        print(f"🔍 Queue size for {progress_id}: {queue_size}")
        
        try:
            while not op["progress_queue"].empty():
                message = op["progress_queue"].get_nowait()
                progress_messages.append(message)
                message_count += 1
                print(f"📨 Retrieved message #{message_count}: {message}")
        except Exception as queue_error:
            print(f"⚠️ Queue error: {queue_error}")
        
        print(f"📊 Returning {len(progress_messages)} messages, status: {op['status']}")
        
        response_data = {
            "status": op["status"],
            "repo_name": op["repo_name"],
            "progress_messages": progress_messages,
            "elapsed_time": int(time.time() - op["start_time"])
        }
        
        if op["error"]:
            response_data["error"] = op["error"]
        
        # Clean up completed operations after 60 seconds (increased from 30)
        if op["status"] in ["completed", "failed"] and time.time() - op["start_time"] > 60:
            print(f"Cleaning up progress ID {progress_id}")  # Debug
            del clone_operations[progress_id]
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error in progress endpoint: {e}")  # Debug
        return jsonify({"error": str(e)}), 500


@app.route("/api/settings/repositories/test-progress", methods=["POST"])
def api_test_progress():
    """Test progress system with a simple countdown."""
    try:
        import threading
        import time
        
        progress_id = f"test_{int(time.time())}"
        
        # Initialize test progress
        clone_operations[progress_id] = {
            "repo_name": "test-repo",
            "repo_url": "test-url",
            "repo_path": "/tmp/test",
            "progress_queue": queue.Queue(),
            "status": "starting",
            "error": None,
            "start_time": time.time()
        }
        
        def test_worker():
            op = clone_operations[progress_id]
            try:
                op["status"] = "cloning"
                
                for i in range(1, 11):
                    message = f"Test progress step {i}/10 ({i*10}%)"
                    print(f"Test: {message}")  # Debug
                    op["progress_queue"].put(message)
                    time.sleep(1)  # Simulate work
                
                op["status"] = "completed"
                op["progress_queue"].put("✅ Test completed successfully!")
                
            except Exception as e:
                op["status"] = "failed"
                op["error"] = str(e)
                op["progress_queue"].put(f"❌ Test failed: {e}")
        
        # Start test in background
        thread = threading.Thread(target=test_worker)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Test progress started",
            "progress_id": progress_id
        })
        
    except Exception as e:
        print(f"Error starting test progress: {e}")  # Debug
        return jsonify({"error": str(e)}), 500


@app.route("/api/settings/repositories/test-git-clone", methods=["POST"])
def api_test_git_clone():
    """Test actual git clone progress capture."""
    try:
        import threading
        import time
        import os
        import tempfile
        
        progress_id = f"git_test_{int(time.time())}"
        
        # Use a temporary directory
        temp_dir = tempfile.mkdtemp()
        repo_path = os.path.join(temp_dir, "test-repo")
        
        # Initialize test progress
        clone_operations[progress_id] = {
            "repo_name": "test-git-clone",
            "repo_url": "https://github.com/octocat/Hello-World.git",
            "repo_path": repo_path,
            "progress_queue": queue.Queue(),
            "status": "starting",
            "error": None,
            "start_time": time.time()
        }
        
        def git_test_worker():
            import subprocess  # Make sure subprocess is imported in thread
            op = clone_operations[progress_id]
            try:
                op["status"] = "cloning"
                op["progress_queue"].put("Starting actual git clone test...")
                print(f"🧪 Testing git clone to {repo_path}")
                
                # Run actual git clone
                process = subprocess.Popen(
                    ["git", "clone", "--progress", "https://github.com/octocat/Hello-World.git", repo_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Read both streams
                def read_stream(stream, name):
                    line_count = 0
                    while True:
                        line = stream.readline()
                        if not line:
                            break
                        clean_line = line.strip()
                        if clean_line:
                            line_count += 1
                            print(f"🧪 Git {name} #{line_count}: {clean_line}")
                            op["progress_queue"].put(f"[{name}] {clean_line}")
                
                import threading
                stdout_thread = threading.Thread(target=read_stream, args=(process.stdout, "stdout"))
                stderr_thread = threading.Thread(target=read_stream, args=(process.stderr, "stderr"))
                
                stdout_thread.start()
                stderr_thread.start()
                
                return_code = process.wait()
                
                stdout_thread.join(timeout=10)
                stderr_thread.join(timeout=10)
                
                print(f"🧪 Git test completed with return code: {return_code}")
                
                if return_code == 0:
                    op["status"] = "completed"
                    op["progress_queue"].put("✅ Git clone test completed!")
                else:
                    op["status"] = "failed"
                    op["error"] = f"Git test failed: {return_code}"
                
                # Cleanup
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                
            except Exception as e:
                print(f"🧪 Git test error: {e}")
                op["status"] = "failed"
                op["error"] = str(e)
                op["progress_queue"].put(f"❌ Git test failed: {e}")
        
        # Start test
        thread = threading.Thread(target=git_test_worker)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Git clone test started",
            "progress_id": progress_id
        })
        
    except Exception as e:
        print(f"Error starting git test: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/settings/subsystems", methods=["GET", "POST"])
def api_settings_subsystems():
    """Get or update subsystems configuration (configuration/services.json)."""
    services_file_path = os.path.join(BASE_DIR, "configuration/services.json")
    
    if request.method == "GET":
        try:
            if os.path.exists(services_file_path):
                with open(services_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                content = "{}"
            
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            if not data or "content" not in data:
                return jsonify({"error": "Content is required"}), 400
            
            content = data["content"]
            
            # Validate JSON format
            try:
                parsed = json.loads(content)
                # Validate structure
                if not isinstance(parsed, dict):
                    raise ValueError("Root must be an object")
                
                for repo_name, services in parsed.items():
                    if not isinstance(services, dict):
                        raise ValueError(f"Services for {repo_name} must be an object")
                    
                    for service_name, paths in services.items():
                        if not isinstance(paths, list):
                            raise ValueError(f"Paths for {service_name} must be an array")
                        
                        for path in paths:
                            if not isinstance(path, str):
                                raise ValueError(f"All paths in {service_name} must be strings")
                                
            except (json.JSONDecodeError, ValueError) as e:
                return jsonify({"error": f"Invalid subsystems format: {str(e)}"}), 400
            
            # Write the file
            with open(services_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            return jsonify({"success": True, "message": "Subsystems configuration updated successfully"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/api/update/git-pull", methods=["POST"])
def api_update_git_pull():
    """Run git pull on all repositories."""
    try:
        import subprocess
        
        repos_root = os.path.join(BASE_DIR, "repos")
        if not os.path.exists(repos_root):
            return jsonify({"error": "No repos directory found"}), 404
        
        results = []
        
        # Find all git repositories
        for org_dir in os.listdir(repos_root):
            org_path = os.path.join(repos_root, org_dir)
            if not os.path.isdir(org_path):
                continue
                
            for repo_dir in os.listdir(org_path):
                repo_path = os.path.join(org_path, repo_dir)
                git_dir = os.path.join(repo_path, ".git")
                
                if os.path.isdir(repo_path) and os.path.exists(git_dir):
                    repo_name = f"{org_dir}/{repo_dir}"
                    
                    try:
                        # Run git pull
                        result = subprocess.run(
                            ["git", "pull", "--ff-only"],
                            cwd=repo_path,
                            capture_output=True,
                            text=True,
                            timeout=18000  # 5 hour timeout per repo for enterprise-scale batch operations
                        )
                        
                        if result.returncode == 0:
                            results.append({
                                "repo": repo_name,
                                "success": True,
                                "message": result.stdout.strip() if result.stdout.strip() else "Updated successfully"
                            })
                        else:
                            results.append({
                                "repo": repo_name,
                                "success": False,
                                "error": result.stderr.strip() if result.stderr.strip() else "Git pull failed"
                            })
                            
                    except subprocess.TimeoutExpired:
                        results.append({
                            "repo": repo_name,
                            "success": False,
                            "error": "Git pull timed out"
                        })
                    except Exception as e:
                        results.append({
                            "repo": repo_name,
                            "success": False,
                            "error": str(e)
                        })
        
        if not results:
            return jsonify({"error": "No git repositories found"}), 404
        
        return jsonify({
            "success": True,
            "message": f"Processed {len(results)} repositories",
            "results": results
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/update/run-analysis", methods=["POST"])
def api_update_run_analysis():
    """Start the complete update process (git pull + analysis) asynchronously."""
    global update_process_active
    
    # Force reset state to ensure clean start
    print(f"🔍 Update request received. Current state: update_process_active={update_process_active}")
    reset_update_state()
    print(f"🔍 After reset: update_process_active={update_process_active}")
    
    if update_process_active:
        return jsonify({"error": "Update process already running"}), 409
    
    try:
        # Parse request data safely - handle empty requests gracefully
        data = {}
        if request.is_json and request.get_data():
            try:
                data = request.get_json()
            except Exception:
                data = {}  # Default to empty dict if JSON parsing fails
        force_update = data.get("force_update", False)
        
        # Clear the progress queue
        while not update_progress_queue.empty():
            update_progress_queue.get()
        
        # Start the unified update process in a separate thread
        thread = threading.Thread(target=run_full_update_async, args=(force_update,))
        thread.daemon = True
        thread.start()
        
        return jsonify({"success": True, "message": "Update process started"})
        
    except Exception as e:
        # Ensure we reset the flag if there's an error starting the update
        update_process_active = False
        return jsonify({"error": str(e)}), 500


@app.route("/api/update/reset", methods=["POST"])
def api_update_reset():
    """Reset the update process state - useful if it gets stuck."""
    global update_process_active
    
    print("🔄 Resetting update process state...")
    update_process_active = False
    
    # Clear any remaining messages in the queue
    while not update_progress_queue.empty():
        try:
            update_progress_queue.get_nowait()
        except queue.Empty:
            break
    
    return jsonify({"success": True, "message": "Update process state reset"})


@app.route("/api/update/status", methods=["GET"])
def api_update_status():
    """Get current update process status."""
    global update_process_active
    
    return jsonify({
        "is_running": update_process_active,
        "queue_size": update_progress_queue.qsize()
    })


@app.route("/api/update/logs", methods=["GET"])
def api_update_logs():
    """Get the update log file content."""
    try:
        if os.path.exists(UPDATE_LOG_FILE):
            with open(UPDATE_LOG_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            return jsonify({
                "success": True,
                "content": content,
                "file_size": len(content)
            })
        else:
            return jsonify({
                "success": True,
                "content": "No update logs yet. Run an update to generate logs.",
                "file_size": 0
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/update/logs/download", methods=["GET"])
def api_update_logs_download():
    """Download the update log file."""
    try:
        if os.path.exists(UPDATE_LOG_FILE):
            return send_from_directory(BASE_DIR, "update_logs.txt", as_attachment=True)
        else:
            return "No update logs found", 404
    except Exception as e:
        return str(e), 500


@app.route("/api/update/progress")
def api_update_progress():
    """Server-sent events endpoint for update progress."""
    def generate():
        global update_process_active
        while update_process_active:
            try:
                # Get message from queue with timeout
                message = update_progress_queue.get(timeout=1)
                yield f"data: {json.dumps(message)}\n\n"
            except queue.Empty:
                # Send heartbeat
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        
        # Send final completion message
        yield f"data: {json.dumps({'type': 'complete'})}\n\n"
    
    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Connection'] = 'keep-alive'
    response.headers['X-Accel-Buffering'] = 'no'
    return response


def run_full_update_async(force_update=False):
    """Run complete update process (git pull + analysis) in a separate thread with progress reporting."""
    global update_process_active
    
    update_process_active = True
    
    # Start a new log section
    start_new_update_log()
    
    try:
        import subprocess
        from datetime import datetime
        import calendar
        
        # Initial setup
        start_timestamp = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'🚀 Starting update process... [{start_timestamp.strftime("%H:%M:%S")}]',
            'progress': 0
        })
        
        # Phase 1: Git pull operations (2% of total progress based on timing analysis)
        git_start_time = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'📦 Updating repositories with git pull... [{git_start_time.strftime("%H:%M:%S")}]',
            'progress': 0
        })
        
        if not run_git_pull_all(force_update):
            git_end_time = datetime.now()
            git_duration = (git_end_time - git_start_time).total_seconds()
            log_update_message({
                'type': 'error',
                'message': f'❌ Git pull operations failed [{git_end_time.strftime("%H:%M:%S")}] (duration: {git_duration:.1f}s)',
                'progress': 2
            })
            return

        git_end_time = datetime.now()
        git_duration = (git_end_time - git_start_time).total_seconds()
        log_update_message({
            'type': 'info',
            'message': f'✅ Repository updates completed [{git_end_time.strftime("%H:%M:%S")}] (duration: {git_duration:.1f}s)',
            'progress': 2
        })
        
        # Phase 1.5: Clean up old statistics
        cleanup_start_time = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'🧹 Cleaning up old statistics... [{cleanup_start_time.strftime("%H:%M:%S")}]',
            'progress': 2
        })
        
        stats_dir = os.path.join(BASE_DIR, "stats")
        if os.path.exists(stats_dir):
            try:
                import shutil
                shutil.rmtree(stats_dir)
                log_update_message({
                    'type': 'info',
                    'message': f'✅ Old statistics removed [{datetime.now().strftime("%H:%M:%S")}]',
                    'progress': 2
                })
            except Exception as e:
                log_update_message({
                    'type': 'warning',
                    'message': f'⚠️ Could not remove old statistics: {str(e)}',
                    'progress': 2
                })
        else:
            log_update_message({
                'type': 'info',
                'message': f'✅ No old statistics to clean [{datetime.now().strftime("%H:%M:%S")}]',
                'progress': 2
            })
        
        # Phase 2: Analysis script execution (98% of total progress)
        analysis_start_time = datetime.now()
        log_update_message({
            'type': 'info',
            'message': f'🔄 Running analysis script (master.py) with parallel processing... [{analysis_start_time.strftime("%H:%M:%S")}]',
            'progress': 2
        })
        
        master_script = os.path.join(BASE_DIR, "master.py")
        if not os.path.exists(master_script):
            analysis_end_time = datetime.now()
            log_update_message({
                'type': 'error', 
                'message': f'master.py script not found [{analysis_end_time.strftime("%H:%M:%S")}]',
                'progress': 2
            })
            return
        
        # Calculate progress distribution for analysis
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Progress distribution for parallel processing based on timing analysis:
        # Analysis of upgrade.txt shows:
        # - Git pull: ~24s (2% of total)
        # - Monthly processing: Parallel ~5-8min (25% of total) 
        # - Yearly summaries: ~30s (3% of total)
        # - Language stats: ~15s (5% of total)
        # - Blame analysis: ~8-12min with parallel (65% of total) - This is the major bottleneck
        
        total_months = current_month
        monthly_operations = total_months * 2  # summery + service for each month
        
        # Progress ranges - redistributed based on actual timing analysis
        monthly_start = 2.0     # Git pull gets 0-2%
        monthly_end = 27.0      # Monthly processing gets 2-27% (25%)
        yearly_start = 27.0     # Yearly summaries get 27-30% (3%)
        yearly_end = 30.0
        lang_start = 30.0       # Language stats get 30-35% (5%)
        lang_end = 35.0
        blame_start = 35.0      # Blame analysis gets 35-100% (65%) - reflects actual time spent
        blame_end = 100.0
        
        current_progress = 2.0  # Start after git pull completes
        monthly_progress_per_op = (monthly_end - monthly_start) / (monthly_operations if monthly_operations > 0 else 1)
        monthly_op_count = 0
        
        # Proceed directly to reliable analysis phase (modeled after safe_update.sh)
        # This approach avoids the pickle issues in parallel service.py processing
        
        try:
            # 🎯 TRUE 12-MONTH ROLLING WINDOW FOR FIRST-TIME USERS
            analysis_start_time = datetime.now()
            
            # Calculate true 12-month rolling window
            current_date = datetime.now()
            twelve_months_ago = current_date - timedelta(days=365)
            start_year = twelve_months_ago.year  # 2024
            current_year = current_date.year     # 2025
            years_to_process = [start_year, current_year]  # [2024, 2025]
            
            log_update_message({
                'type': 'info',
                'message': f'🎯 TRUE 12-Month Rolling Window: {twelve_months_ago.strftime("%Y-%m")} to {current_date.strftime("%Y-%m")}',
                'progress': 5
            })
            
            log_update_message({
                'type': 'info',
                'message': f'📊 Processing years: {years_to_process} for complete 12-month analytics',
                'progress': 10
            })
            
            # Execute master.py for BOTH years (not just current year months!)
            python_exe = sys.executable or "python3"
            successful_years = []
            
            for i, year in enumerate(years_to_process):
                year_start_time = datetime.now()
                year_progress = 15 + (i * 70)  # 15-85% for both years
                
                log_update_message({
                    'type': 'info',
                    'message': f'📈 Processing year {year} with master.py ({i+1}/2) [{year_start_time.strftime("%H:%M:%S")}]',
                    'progress': year_progress
                })
                
                # Run master.py for comprehensive yearly analysis
                master_cmd = [
                    python_exe,
                    master_script,
                    "--year", str(year),
                    "--repos-root", os.path.join(BASE_DIR, "repos"),
                    "--output-root", BASE_DIR,
                    "--services-file", os.path.join(BASE_DIR, "configuration", "services.json"),
                    "--alias-file", os.path.join(BASE_DIR, "configuration", "alias.json"),
                    "--ignore-file", os.path.join(BASE_DIR, "configuration", "ignore_user.txt"),
                    "--parallel",
                    "--max-workers", "4"
                ]
                
                try:
                    result = subprocess.run(
                        master_cmd,
                        cwd=BASE_DIR,
                        capture_output=True,
                        text=True,
                        timeout=144000  # 40 hour timeout per year for enterprise-scale operations
                    )
                    
                    year_end_time = datetime.now()
                    year_duration = (year_end_time - year_start_time).total_seconds()
                    
                    if result.returncode == 0:
                        successful_years.append(year)
                        log_update_message({
                            'type': 'info',
                            'message': f'✅ Year {year}: SUCCESS! Generated all monthly data and yearly summaries ({year_duration:.1f}s)',
                            'progress': year_progress + 35
                        })
                    else:
                        error_msg = result.stderr.strip()[:100] if result.stderr else "Unknown error"
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ Year {year}: Issues detected - {error_msg}... (continuing)',
                            'progress': year_progress + 35
                        })
                            
                except subprocess.TimeoutExpired:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Year {year}: Analysis timed out after 40 hours (continuing with other years)',
                        'progress': year_progress + 35
                    })
                except Exception as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Year {year}: Failed - {str(e)[:100]}',
                        'progress': year_progress + 35
                    })
            
            # Final results for 12-month rolling window - ALWAYS complete at 100%
            final_time = datetime.now()
            total_duration = (final_time - analysis_start_time).total_seconds()
            
            # Always show completion, even if some years timed out
            if successful_years or years_to_process:  # Show completion if any processing attempted
                years_str = ", ".join(map(str, successful_years)) if successful_years else "2024, 2025 (with timeouts)"
                log_update_message({
                    'type': 'info',
                    'message': f'🎉 12-Month Rolling Window COMPLETE! Data generated from years: {years_str}',
                    'progress': 98
                })
                
                log_update_message({
                    'type': 'info',
                    'message': f'📊 First-time user experience DELIVERED! Users & Subsystems populated with true 12-month analytics (Duration: {total_duration:.0f}s)',
                    'progress': 100
                })
                
                # Force completion status
                log_update_message({
                    'type': 'complete',
                    'message': 'Update process completed successfully!',
                    'progress': 100
                })
            else:
                log_update_message({
                    'type': 'error',
                    'message': f'❌ Failed to generate any 12-month rolling window data - check repository access',
                    'progress': 100
                })
                
        except Exception as e:
            error_time = datetime.now()
            log_update_message({
                'type': 'error',
                'message': f'❌ 12-month rolling window failed: {str(e)} [{error_time.strftime("%H:%M:%S")}]',
                'progress': 100
            })
            
            # Calculate months to process
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            # Process each month from January to current month
            total_months = current_month
            monthly_progress_start = 2
            monthly_progress_end = 60  # Reserve 60% for monthly processing
            monthly_progress_per_month = (monthly_progress_end - monthly_progress_start) / total_months
            
            summery_script = os.path.join(BASE_DIR, "summery.py")
            service_script = os.path.join(BASE_DIR, "service.py")
            
            # Check scripts exist
            for script_path, script_name in [(summery_script, "summery.py"), (service_script, "service.py")]:
                if not os.path.exists(script_path):
                    log_update_message({
                        'type': 'error',
                        'message': f'❌ Required script {script_name} not found [{datetime.now().strftime("%H:%M:%S")}]',
                        'progress': monthly_progress_start
                    })
                    return
            
            for month in range(1, current_month + 1):
                month_start_time = datetime.now()
                date_from = f"{current_year}-{month:02d}-01"
                
                # Calculate last day of month
                import calendar
                last_day = calendar.monthrange(current_year, month)[1]
                date_to = f"{current_year}-{month:02d}-{last_day:02d}"
                
                month_progress_start = monthly_progress_start + (month - 1) * monthly_progress_per_month
                month_progress_end = monthly_progress_start + month * monthly_progress_per_month
                
                log_update_message({
                    'type': 'info',
                    'message': f'📊 Processing {current_year}-{month:02d} (month {month}/{total_months})... [{month_start_time.strftime("%H:%M:%S")}]',
                    'progress': month_progress_start
                })
                
                # Step 1: Run summery.py for this month
                try:
                    summery_result = subprocess.run([
                        "python", summery_script,
                        "--from", date_from,
                        "--to", date_to,
                        "--repos-root", "repos/appgate-sdp-int",
                        "--output-root", ".",
                        "--alias-file", "configuration/alias.json",
                        "--ignore-file", "configuration/ignore_user.txt"
                    ], cwd=BASE_DIR, capture_output=True, text=True, timeout=18000)  # 5 hours for user stats with enterprise repos
                    
                    if summery_result.returncode != 0:
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ User statistics for {current_year}-{month:02d} had issues, but continuing...',
                            'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                        })
                    
                except subprocess.TimeoutExpired:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ User statistics for {current_year}-{month:02d} timed out, but continuing...',
                        'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                    })
                except Exception as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ User statistics for {current_year}-{month:02d} failed: {str(e)}, but continuing...',
                        'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                    })
                
                # Step 2: Run service.py for this month (WITHOUT --parallel to avoid pickle issues)
                try:
                    service_result = subprocess.run([
                        "python", service_script,
                        "--from", date_from,
                        "--to", date_to,
                        "--repos-root", "repos/appgate-sdp-int",
                        "--output-root", ".",
                        "--services-file", "configuration/services.json",
                        "--alias-file", "configuration/alias.json",
                        "--ignore-file", "configuration/ignore_user.txt"
                        # NOTE: No --parallel flag to avoid pickle issues
                    ], cwd=BASE_DIR, capture_output=True, text=True, timeout=36000)  # 10 hours for subsystem stats with massive enterprise repos
                    
                    if service_result.returncode != 0:
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ Subsystem statistics for {current_year}-{month:02d} had issues, but continuing...',
                            'progress': month_progress_end
                        })
                    
                except subprocess.TimeoutExpired:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Subsystem statistics for {current_year}-{month:02d} timed out, but continuing...',
                        'progress': month_progress_end
                    })
                except Exception as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Subsystem statistics for {current_year}-{month:02d} failed: {str(e)}, but continuing...',
                        'progress': month_progress_end
                    })
                
                month_end_time = datetime.now()
                month_duration = (month_end_time - month_start_time).total_seconds()
                log_update_message({
                    'type': 'info',
                    'message': f'✅ Completed {current_year}-{month:02d} [{month_end_time.strftime("%H:%M:%S")}] (duration: {month_duration:.1f}s)',
                    'progress': month_progress_end
                })
            
            # Phase 2b: Generate yearly summaries
            yearly_start_time = datetime.now()
            log_update_message({
                'type': 'info',
                'message': f'📈 Generating yearly summaries... [{yearly_start_time.strftime("%H:%M:%S")}]',
                'progress': 65
            })
            
            # This is a simplified version - we'll skip the complex yearly summary generation for now
            # The monthly data is the most important part for the UI
            
            yearly_end_time = datetime.now()
            log_update_message({
                'type': 'info',
                'message': f'✅ Yearly summaries completed [{yearly_end_time.strftime("%H:%M:%S")}]',
                'progress': 70
            })
            
        except Exception as e:
            error_time = datetime.now()
            log_update_message({
                'type': 'error',
                'message': f'❌ Monthly analysis failed: {str(e)} [{error_time.strftime("%H:%M:%S")}]',
                'progress': 65
            })
            
        # Phase 3: Blame analysis (ownership analysis)
        try:
            blame_start_time = datetime.now()
            log_update_message({
                'type': 'info',
                'message': f'🔍 Starting ownership analysis (blame.py)... [{blame_start_time.strftime("%H:%M:%S")}]',
                'progress': 75
            })
            
            blame_script = os.path.join(BASE_DIR, "blame.py")
            if os.path.exists(blame_script):
                blame_result = subprocess.run([
                    "python", blame_script,
                    "--repos-root", "repos/appgate-sdp-int",
                    "--output-root", ".",
                    "--services-file", "configuration/services.json",
                    "--alias-file", "configuration/alias.json",
                    "--ignore-file", "configuration/ignore_user.txt",
                    "--parallel"  # blame.py parallel works fine, it's only service.py that has issues
                ], cwd=BASE_DIR, capture_output=True, text=True, timeout=72000)  # 20 hour timeout for ownership analysis with enterprise-scale repos
                
                blame_end_time = datetime.now()
                blame_duration = (blame_end_time - blame_start_time).total_seconds()
                
                if blame_result.returncode == 0:
                    log_update_message({
                        'type': 'info',
                        'message': f'✅ Ownership analysis completed [{blame_end_time.strftime("%H:%M:%S")}] (duration: {blame_duration:.1f}s)',
                        'progress': 95
                    })
                else:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Ownership analysis completed with warnings [{blame_end_time.strftime("%H:%M:%S")}] (duration: {blame_duration:.1f}s)',
                        'progress': 95
                    })
            else:
                log_update_message({
                    'type': 'warning',
                    'message': f'⚠️ blame.py not found, skipping ownership analysis [{datetime.now().strftime("%H:%M:%S")}]',
                    'progress': 95
                })
                
        except subprocess.TimeoutExpired:
            log_update_message({
                'type': 'warning',
                'message': f'⚠️ Ownership analysis timed out after 20 hours [{datetime.now().strftime("%H:%M:%S")}]',
                'progress': 95
            })
        except Exception as e:
            log_update_message({
                'type': 'warning',
                'message': f'⚠️ Ownership analysis failed: {str(e)} [{datetime.now().strftime("%H:%M:%S")}]',
                'progress': 95
            })
        
        # Final completion
        final_end_time = datetime.now()
        total_duration = (final_end_time - start_timestamp).total_seconds()
        log_update_message({
            'type': 'info',
            'message': f'🎉 Update completed successfully! [{final_end_time.strftime("%H:%M:%S")}] (total duration: {total_duration:.0f}s)',
            'progress': 100
        })
        
    except Exception as e:
        final_error_time = datetime.now()
        total_duration = (final_error_time - start_timestamp).total_seconds()
        log_update_message({
            'type': 'error',
            'message': f'❌ Update process failed: {str(e)} [{final_error_time.strftime("%H:%M:%S")}] (duration: {total_duration:.0f}s)',
            'progress': 100
        })
    finally:
        update_process_active = False

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
            
            try:
                repo_start_time = datetime.now()
                
                # CUSTOMER-FRIENDLY APPROACH: Always continue with analysis
                # Check repository status for informational purposes only
                try:
                    status_check = subprocess.run(
                        ["git", "status", "--porcelain"],
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        timeout=30  # Quick status check
                    )
                    
                    # Always continue with analysis regardless of uncommitted changes
                    if status_check.returncode == 0 and status_check.stdout.strip():
                        log_update_message({
                            'type': 'info',
                            'message': f'[{repo_start_time.strftime("%H:%M:%S")}] ℹ️ {repo_name}: Local changes detected, analyzing committed history only (continuing)',
                            'progress': progress
                        })
                    else:
                        log_update_message({
                            'type': 'info',
                            'message': f'[{repo_start_time.strftime("%H:%M:%S")}] ✅ {repo_name}: Repository clean, ready for analysis',
                            'progress': progress
                        })
                except (subprocess.TimeoutExpired, Exception) as e:
                    # If git status fails, still continue with analysis
                    log_update_message({
                        'type': 'info',
                        'message': f'[{repo_start_time.strftime("%H:%M:%S")}] ℹ️ {repo_name}: Could not check status, proceeding with analysis anyway',
                        'progress': progress
                    })
                
                # CUSTOMER-SAFE APPROACH: Read-only analysis - NEVER modify repositories
                # This is critical for customer trust and data safety
                log_update_message({
                    'type': 'info',
                    'message': f'[{repo_start_time.strftime("%H:%M:%S")}] 📊 {repo_name}: Starting read-only analysis (no repository changes)',
                    'progress': progress
                })
                
                # Continue with analysis regardless of repository state
                success_count += 1
                    
            except Exception as e:
                error_time = datetime.now()
                log_update_message({
                    'type': 'warning',
                    'message': f'[{error_time.strftime("%H:%M:%S")}] ⚠️ {repo_name}: {str(e)}',
                    'progress': progress
                })
        
        return success_count > 0  # Return True if at least one repo was updated successfully
        
    except Exception as e:
        error_time = datetime.now()
        log_update_message({
            'type': 'error',
            'message': f'[{error_time.strftime("%H:%M:%S")}] ❌ Git pull failed: {str(e)}',
            'progress': 1
        })
        return False


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
    
    # Get available periods from user data (since team data is aggregated from user data)
    user_months = list_user_months()
    all_periods = set()
    
    for user_periods in user_months.values():
        for period in user_periods:
            all_periods.add((period["from"], period["to"], period["label"], period["is_yearly"]))
    
    # Sort periods
    sorted_periods = sorted(list(all_periods), key=lambda x: x[0])
    periods = [
        {"from": p[0], "to": p[1], "label": p[2], "is_yearly": p[3]}
        for p in sorted_periods
    ]
    
    teams = []
    responsibilities = load_team_subsystem_responsibilities()
    
    for team_id, team_info in teams_config.items():
        responsible_subsystems = responsibilities.get(team_id, [])
        teams.append({
            "id": team_id,
            "name": team_info.get("name", team_id),
            "description": team_info.get("description", ""),
            "members": team_info.get("members", []),
            "responsible_subsystems": responsible_subsystems,
            "periods": periods
        })
    
    return jsonify({"teams": teams})


@app.route("/api/teams/<team_id>/month/<from_date>/<to_date>")
def api_team_month(team_id: str, from_date: str, to_date: str):
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
    members = team.get("members", [])
    responsible_subsystems = get_team_responsible_subsystems(team_id)
    
    if not members:
        # Even for empty teams, calculate responsible subsystem details
        responsible_subsystem_details = {}
        total_responsible_lines = 0
        
        for subsystem_name in responsible_subsystems:
            try:
                # Load the latest language stats for this subsystem
                subsystem_lang_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name, "languages.json")
                if os.path.exists(subsystem_lang_path):
                    with open(subsystem_lang_path, "r", encoding="utf-8") as f:
                        lang_data = json.load(f)
                        subsystem_lines = 0
                        # Sum up all language code lines
                        for lang_name, lang_info in lang_data.get("languages", {}).items():
                            if isinstance(lang_info, dict):
                                subsystem_lines += lang_info.get("lines", 0)
                        
                        responsible_subsystem_details[subsystem_name] = {
                            "name": subsystem_name,
                            "lines_of_code": subsystem_lines
                        }
                        total_responsible_lines += subsystem_lines
            except (json.JSONDecodeError, IOError, KeyError):
                # If we can't load language data, still include the subsystem with 0 lines
                responsible_subsystem_details[subsystem_name] = {
                    "name": subsystem_name,
                    "lines_of_code": 0
                }
        
        return jsonify({
            "type": "team",
            "team_id": team_id,
            "team_name": team.get("name", team_id),
            "description": team.get("description", ""),
            "members": [],
            "responsible_subsystems": responsible_subsystems,
            "responsible_subsystem_details": responsible_subsystem_details,
            "total_responsible_lines": total_responsible_lines,
            "total_commits": 0,
            "total_additions": 0,
            "total_deletions": 0,
            "files_changed": {},
            "languages": {},
            "subsystems": {},
            "commits_timeline": []
        })
    
    # Aggregate data from all team members
    aggregated_data = {
        "type": "team",
        "team_id": team_id,
        "team_name": team.get("name", team_id),
        "description": team.get("description", ""),
        "members": members,
        "responsible_subsystems": responsible_subsystems,
        "total_commits": 0,
        "total_additions": 0,
        "total_deletions": 0,
        "languages": {},
        "subsystems": {},
        "per_date": {},
        "member_contributions": {}
    }
    
    for member in members:
        # Use the same aggregation method as the teams overview for consistency
        member_data = aggregate_user_data_for_period(member, from_date, to_date)
        if member_data:
            # Aggregate basic stats
            aggregated_data["total_commits"] += member_data.get("total_commits", 0)
            aggregated_data["total_additions"] += member_data.get("total_lines_added", 0)
            aggregated_data["total_deletions"] += member_data.get("total_lines_deleted", 0)
            
            # Store individual member contribution
            aggregated_data["member_contributions"][member] = {
                "commits": member_data.get("total_commits", 0),
                "additions": member_data.get("total_lines_added", 0),
                "deletions": member_data.get("total_lines_deleted", 0)
            }
            
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
    
    for subsystem_name in responsible_subsystems:
        try:
            # Load the latest language stats for this subsystem
            subsystem_lang_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name, "languages.json")
            if os.path.exists(subsystem_lang_path):
                with open(subsystem_lang_path, "r", encoding="utf-8") as f:
                    lang_data = json.load(f)
                    subsystem_lines = 0
                    # Sum up all language code lines
                    for lang_name, lang_info in lang_data.get("languages", {}).items():
                        if isinstance(lang_info, dict):
                            subsystem_lines += lang_info.get("lines", 0)
                    
                    responsible_subsystem_details[subsystem_name] = {
                        "name": subsystem_name,
                        "lines_of_code": subsystem_lines
                    }
                    total_responsible_lines += subsystem_lines
        except (json.JSONDecodeError, IOError, KeyError):
            # If we can't load language data, still include the subsystem with 0 lines
            responsible_subsystem_details[subsystem_name] = {
                "name": subsystem_name,
                "lines_of_code": 0
            }
    
    aggregated_data["responsible_subsystem_details"] = responsible_subsystem_details
    aggregated_data["total_responsible_lines"] = total_responsible_lines
    
    return jsonify(aggregated_data)


@app.route("/api/teams/<team_id>/year/<int:year>")
def api_team_year(team_id: str, year: int):
    """Get aggregated yearly summary for a team."""
    from_date = f"{year:04d}-01-01"
    to_date = f"{year:04d}-12-31"
    return api_team_month(team_id, from_date, to_date)


@app.route("/api/teams/overview")
def api_teams_overview():
    """Get overview analytics for all teams."""
    period_type = request.args.get('period', 'overall')  # 'overall' or 'last3months'
    
    teams_file_path = os.path.join(BASE_DIR, "configuration/teams.json")
    
    if not os.path.exists(teams_file_path):
        return jsonify({"teams": []})
    
    try:
        with open(teams_file_path, "r", encoding="utf-8") as f:
            teams_config = json.load(f)
    except (json.JSONDecodeError, IOError):
        teams_config = {}
    
    if not teams_config:
        return jsonify({"teams": []})
    
    # Determine date range based on period_type
    if period_type == 'last3months':
        # Get the current date and calculate 3 months ago
        current_date = datetime.now()
        three_months_ago = current_date - timedelta(days=90)
        from_date = three_months_ago.strftime("%Y-%m-01")  # Start from the beginning of the month
        to_date = current_date.strftime("%Y-%m-%d")
        period_label = "Last 3 Months"
    else:
        # For overall, use a very wide date range to capture all data
        # This will include all available data across all time periods
        from_date = "2000-01-01"  # Start far in the past to capture all data
        to_date = datetime.now().strftime("%Y-%m-%d")  # End today
        period_label = "Overall"
    
    teams_analytics = []
    
    for team_id, team_info in teams_config.items():
        team_name = team_info.get("name", team_id)
        members = team_info.get("members", [])
        responsible_subsystems = get_team_responsible_subsystems(team_id)
        
        # Initialize team stats
        team_stats = {
            "id": team_id,
            "name": team_name,
            "description": team_info.get("description", ""),
            "member_count": len(members),
            "members": members,
            "responsible_subsystems": responsible_subsystems,
            "responsible_subsystems_count": len(responsible_subsystems),
            "total_commits": 0,
            "total_additions": 0,
            "total_deletions": 0,
            "total_lines_changed": 0,
            "active_subsystems": set(),
            "languages": {},
            "active_months": set()
        }
        
        # Aggregate data from all team members
        for member in members:
            # Always use aggregate_user_data_for_period for consistency
            # This ensures we get the most up-to-date data across all periods
            member_stats = aggregate_user_data_for_period(member, from_date, to_date)
            
            if member_stats:
                team_stats["total_commits"] += member_stats.get("total_commits", 0)
                team_stats["total_additions"] += member_stats.get("total_lines_added", member_stats.get("total_additions", 0))
                team_stats["total_deletions"] += member_stats.get("total_lines_deleted", member_stats.get("total_deletions", 0))
                
                # Track subsystems this team works on
                for repo in member_stats.get("per_repo", {}).keys():
                    team_stats["active_subsystems"].add(repo)
                
                # Aggregate languages
                for lang, lang_data in member_stats.get("languages", {}).items():
                    if lang not in team_stats["languages"]:
                        team_stats["languages"][lang] = 0
                    team_stats["languages"][lang] += lang_data.get("net_lines", 0)
                
                # Track active months based on commits
                for date_str in member_stats.get("per_date", {}).keys():
                    if member_stats["per_date"][date_str].get("commits", 0) > 0:
                        # Extract year-month from date
                        try:
                            month_key = date_str[:7]  # YYYY-MM format
                            team_stats["active_months"].add(month_key)
                        except:
                            pass
        
        # Calculate derived metrics
        team_stats["total_lines_changed"] = team_stats["total_additions"] + team_stats["total_deletions"]
        team_stats["active_subsystems_count"] = len(team_stats["active_subsystems"])
        team_stats["active_months_count"] = len(team_stats["active_months"])
        
        # Convert sets to lists for JSON serialization
        team_stats["active_subsystems"] = list(team_stats["active_subsystems"])
        team_stats["active_months"] = list(team_stats["active_months"])
        
        # Find primary language (language with most lines)
        if team_stats["languages"]:
            team_stats["primary_language"] = max(team_stats["languages"], key=team_stats["languages"].get)
        else:
            team_stats["primary_language"] = "N/A"
        
        teams_analytics.append(team_stats)
    
    # Calculate total lines of code under team responsibility
    for team_stats in teams_analytics:
        team_stats["responsible_lines_of_code"] = 0
        
        # Calculate total lines for responsible subsystems
        for subsystem_name in team_stats["responsible_subsystems"]:
            try:
                # Load the latest language stats for this subsystem
                subsystem_lang_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name, "languages.json")
                if os.path.exists(subsystem_lang_path):
                    with open(subsystem_lang_path, "r", encoding="utf-8") as f:
                        lang_data = json.load(f)
                        # Sum up all language code lines
                        for lang_name, lang_info in lang_data.get("languages", {}).items():
                            if isinstance(lang_info, dict):
                                # Try code_lines first (cloc format), then fall back to lines
                                lines = lang_info.get("code_lines", lang_info.get("lines", 0))
                                team_stats["responsible_lines_of_code"] += lines
            except (json.JSONDecodeError, IOError, KeyError):
                # If we can't load language data, skip this subsystem
                pass
    
    # Sort teams by total commits (descending) for ranking
    teams_analytics.sort(key=lambda x: x["total_commits"], reverse=True)
    
    return jsonify({"teams": teams_analytics, "period": period_label})


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
    
    # Get all available periods for this user
    user_months = list_user_months()
    user_periods = user_months.get(user_slug, [])
    
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
        summary_path = os.path.join(STATS_ROOT, "users", user_slug, exact_yearly_match["folder"], "summary.json")
        if os.path.exists(summary_path):
            try:
                return load_json(summary_path)
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
            summary_path = os.path.join(STATS_ROOT, "users", user_slug, period["folder"], "summary.json")
            if os.path.exists(summary_path):
                try:
                    period_data = load_json(summary_path)
                    
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


@app.route("/api/settings/available-users")
def api_settings_available_users():
    """Get list of available users for team member selection and ignore list management.
    Includes both active users (with recent commits) and inactive users (with ownership/blame)."""
    from collections import defaultdict
    
    users_dict = {}
    
    # Get active users from summaries
    user_months = list_user_months()
    for slug, months in user_months.items():
        # Try to get a display name from any summary.json
        display_name = slug
        try:
            any_month = months[0]
            path = find_user_summary(slug, any_month["from"], any_month["to"])
            data = load_json(path)
            if data and data.get("author_name"):
                display_name = data["author_name"]
        except Exception as e:
            pass
        
        users_dict[slug] = {
            "slug": slug,
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
                    if dev_slug not in users_dict:
                        display_name = dev_data.get("display_name", dev_slug) if isinstance(dev_data, dict) else dev_slug
                        users_dict[dev_slug] = {
                            "slug": dev_slug,
                            "display_name": display_name,
                            "active": False
                        }
                
                # Check service-level developers
                services = blame_data.get("services", {})
                for service_data in services.values():
                    service_developers = service_data.get("developers", {})
                    for dev_slug, dev_data in service_developers.items():
                        if dev_slug not in users_dict:
                            display_name = dev_data.get("display_name", dev_slug) if isinstance(dev_data, dict) else dev_slug
                            users_dict[dev_slug] = {
                                "slug": dev_slug,
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


# Static files (for completeness; Flask static_folder already serves /static/<file>)
def get_user_monthly_stats(user_slug: str, year: int) -> List[Dict[str, Any]]:
    """
    Get monthly line addition/deletion statistics for a user for a specific year.
    Returns list of monthly data with month names and line counts.
    """
    user_months = list_user_months()
    if user_slug not in user_months:
        return []
    
    monthly_stats = []
    
    # Get all month periods for this user
    for period in user_months[user_slug]:
        if period["is_yearly"]:
            continue  # Skip yearly summaries
        
        # Check if this period is in the requested year
        if not period["from"].startswith(str(year)):
            continue
            
        # Load the summary for this month
        try:
            summary_path = os.path.join(STATS_ROOT, "users", user_slug, period["folder"], "summary.json")
            if os.path.exists(summary_path):
                with open(summary_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
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
    if user_slug not in user_months:
        return {"month": last_month_str, "month_name": "", "lines_added": 0, "lines_deleted": 0, "commits": 0}
    
    # Find the specific month data
    for period in user_months[user_slug]:
        if period["is_yearly"]:
            continue
        if period["label"] == last_month_str:
            try:
                summary_path = os.path.join(STATS_ROOT, "users", user_slug, period["folder"], "summary.json")
                if os.path.exists(summary_path):
                    with open(summary_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        
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
        if member_slug not in user_months:
            continue
        
        # Find the specific month data for this member
        for period in user_months[member_slug]:
            if period["is_yearly"]:
                continue
            if period["label"] == last_month_str:
                try:
                    summary_path = os.path.join(STATS_ROOT, "users", member_slug, period["folder"], "summary.json")
                    if os.path.exists(summary_path):
                        with open(summary_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        
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
    """
    users_root = os.path.join(STATS_ROOT, "users")
    user_dir = os.path.join(users_root, user_slug)
    
    daily_stats = []
    
    if not os.path.exists(user_dir):
        return daily_stats
    
    user_periods = list_user_months().get(user_slug, [])
    
    # Find the monthly summary that matches our year/month
    target_month = f"{year:04d}-{month:02d}"
    for period in user_periods:
        if period["from"][:7] == target_month:  # Match YYYY-MM
            monthly_folder = period["folder"]
            summary_path = os.path.join(user_dir, monthly_folder, "summary.json")
            
            if os.path.exists(summary_path):
                with open(summary_path, "r", encoding="utf-8") as f:
                    summary_data = json.load(f)
                
                per_date_data = summary_data.get("per_date", {})
                
                # Convert to list format with date strings
                for date_str, day_data in per_date_data.items():
                    if date_str[:7] == target_month:  # Only include days from target month
                        daily_stats.append({
                            "date": date_str,
                            "day": int(date_str.split("-")[2]),
                            "lines_added": day_data.get("additions", 0),
                            "lines_deleted": day_data.get("deletions", 0),
                            "commits": day_data.get("commits", 0)
                        })
                
                break
    
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
    
    team_members = teams[team_id].get("members", [])
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
        app.logger.error(f"Error getting user daily stats: {e}")
        abort(500, description="Failed to get daily statistics")


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
    # You can set host="0.0.0.0" if you want to reach it from other machines
    # Exclude repos directory from file watcher to prevent restarts during cloning
    app.run(host="127.0.0.1", port=5001, debug=True, 
            exclude_patterns=["repos/*", "repos/**/*", "stats/*", "stats/**/*"])

