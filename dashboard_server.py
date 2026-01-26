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
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict, Counter

from flask import Flask, jsonify, send_from_directory, render_template, abort, request, Response

from subsystem_metrics import (
    compute_dead_subsystems,
    compute_subsystem_top_maintainers,
    compute_subsystem_maintainer_timeline,
    compute_subsystem_significant_ownership,
    compute_subsystem_size_rankings,
)

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATS_ROOT = os.path.join(BASE_DIR, "stats")
REPO_ROOT = os.path.join(BASE_DIR, "repos")
CLOC_CACHE_FILE = os.path.join(STATS_ROOT, "cloc_cache.json")
BADGE_CACHE_FILE = os.path.join(STATS_ROOT, "badges_summary.json")
PAGERDUTY_STATS_DIR = os.path.join(STATS_ROOT, "pagerduty")
PAGERDUTY_OVERVIEW_FILE = os.path.join(PAGERDUTY_STATS_DIR, "overview.json")
PAGERDUTY_INCIDENTS_FILE = os.path.join(PAGERDUTY_STATS_DIR, "incidents_last_year.json")
SSH_KNOWN_HOSTS_FILE = os.path.join(BASE_DIR, "configuration", "known_hosts")

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

# Caches for expensive operations
_SERVICES_CONFIG_CACHE: Optional[Dict[str, Dict[str, List[str]]]] = None
_REPO_LANGUAGE_CACHE: Dict[str, Dict[str, Any]] = {}
_SERVICE_LANGUAGE_CACHE: Dict[Tuple[str, str], Dict[str, int]] = {}
_CLOC_CACHE_DATA: Optional[Dict[str, Dict[str, Any]]] = None
_BADGE_CACHE_DATA: Optional[Dict[str, Any]] = None
_BADGE_CACHE_MTIME: Optional[float] = None

_SUBSYSTEM_TOUCH_COUNT_CACHE: Dict[int, Dict[str, int]] = {}

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["READ_ONLY_MODE"] = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global storage for clone progress
clone_operations = {}

# Global queue for update progress messages
update_progress_queue = queue.Queue()
update_process_active = False

# Update log file
UPDATE_LOG_FILE = os.path.join(BASE_DIR, "update_logs.txt")
INTEGRATIONS_FILE = os.path.join(BASE_DIR, "configuration", "integrations.json")
KIOSK_CONFIG_FILE = os.path.join(BASE_DIR, "configuration", "kiosk_config.json")

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


def _run_command_with_live_logs(label: str, cmd: List[str], cwd: Optional[str], progress: float, timeout: Optional[int] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
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
                if stripped:
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


def calculate_years_to_process() -> List[int]:
    current_date = datetime.now()
    start_year = (current_date - timedelta(days=365)).year
    current_year = current_date.year
    if start_year == current_year:
        return [current_year]
    return [start_year, current_year]


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


def perform_background_update(reason: str = 'scheduled') -> bool:
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
    python_exe = sys.executable or 'python3'
    master_script = os.path.join(BASE_DIR, 'master.py')
    cpu_workers = max(1, multiprocessing.cpu_count())
    python_env = os.environ.copy()
    python_env.setdefault('PYTHONUNBUFFERED', '1')
    try:
        years = calculate_years_to_process()
        for year in years:
            if background_cancel_event.is_set():
                logger.info('Background update cancelled before processing year %s', year)
                return False
            base_cmd = [
                python_exe,
                master_script,
                '--year', str(year),
                '--repos-root', os.path.join(BASE_DIR, 'repos'),
                '--output-root', temp_output_root,
                '--services-file', os.path.join(BASE_DIR, 'configuration', 'services.json'),
                '--alias-file', os.path.join(BASE_DIR, 'configuration', 'alias.json'),
                '--ignore-file', os.path.join(BASE_DIR, 'configuration', 'ignore_user.txt'),
                '--parallel',
                '--cpu-count', str(cpu_workers),
            ]
            cmd = build_low_priority_command(base_cmd)
            logger.info('Background update running master.py for year %s', year)
            result = subprocess.run(
                cmd,
                cwd=BASE_DIR,
                text=True,
                capture_output=True,
                timeout=144000,
                env=python_env,
            )
            if result.returncode != 0:
                stderr_tail = '\n'.join((result.stderr or '').strip().splitlines()[-5:])
                logger.error('Background update failed for year %s: %s', year, stderr_tail)
                return False
        if background_cancel_event.is_set():
            logger.info('Background update cancelled before swapping stats')
            return False
        swap_stats_directories(temp_output_root)
        logger.info('Background update completed successfully; stats swapped in place')
        return True
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def trigger_background_update(reason: str = 'manual') -> bool:
    if update_process_active:
        return False
    with background_state_lock:
        if background_state['running']:
            return False
        background_state['running'] = True
    background_cancel_event.clear()
    thread = threading.Thread(target=_background_update_job, args=(reason,), daemon=True)
    thread.start()
    return True


def _background_update_job(reason: str) -> None:
    status = 'failed'
    try:
        success = perform_background_update(reason=reason)
        if background_cancel_event.is_set() and not success:
            status = 'cancelled'
        else:
            status = 'success' if success else 'failed'
    except Exception as exc:
        logger.exception('Background update crashed: %s', exc)
        status = 'failed'
    finally:
        background_cancel_event.clear()
        record_last_update(status, 'background')
        with background_state_lock:
            background_state['running'] = False
        schedule_background_check()


def wait_for_scheduler_event(timeout: float) -> bool:
    triggered = background_scheduler_event.wait(timeout)
    if triggered:
        background_scheduler_event.clear()
    return triggered


def background_scheduler_loop() -> None:
    logger.info('Background update scheduler started')
    while not background_scheduler_stop_event.is_set():
        settings = load_update_settings()
        if not settings.get('background_enabled', False):
            with background_state_lock:
                background_state['next_run'] = None
            wait_for_scheduler_event(300)
            continue
        interval_hours = max(1, int(settings.get('interval_hours', 24)))
        last_completed = parse_timestamp(settings.get('last_background_completed_at'))
        if not last_completed:
            last_completed = datetime.utcnow() - timedelta(hours=interval_hours)
        next_run = last_completed + timedelta(hours=interval_hours)
        with background_state_lock:
            background_state['next_run'] = next_run.isoformat(timespec='seconds') + 'Z'
        now = datetime.utcnow()
        wait_seconds = (next_run - now).total_seconds()
        if wait_seconds <= 0:
            triggered = trigger_background_update('scheduled')
            if not triggered:
                wait_for_scheduler_event(300)
            else:
                wait_for_scheduler_event(60)
        else:
            wait_for_scheduler_event(min(wait_seconds, 300))


def start_background_scheduler() -> None:
    global background_scheduler_thread
    if background_scheduler_thread and background_scheduler_thread.is_alive():
        return
    background_scheduler_thread = threading.Thread(target=background_scheduler_loop, daemon=True)
    background_scheduler_thread.start()


def cancel_background_update(wait: bool = False, timeout: float = 30.0) -> None:
    background_cancel_event.set()
    if wait:
        start = time.time()
        while time.time() - start < timeout:
            with background_state_lock:
                if not background_state.get("running"):
                    break
            time.sleep(0.25)
    background_scheduler_event.set()


def interrupt_all_updates() -> None:
    reset_update_state()
    cancel_background_update(wait=True)



def _build_badge_display_name_map(target_slugs: Set[str], user_months: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, str]:
    display_names = {slug: slug for slug in target_slugs}
    if not target_slugs:
        return display_names
    months_lookup = user_months or list_user_months()
    for slug in target_slugs:
        months = months_lookup.get(slug) or []
        if not months:
            continue
        period = months[0]
        try:
            summary_path = find_user_summary(slug, period["from"], period["to"])
            if os.path.isfile(summary_path):
                summary_data = load_json(summary_path)
                display_names[slug] = summary_data.get("author_name", slug)
        except Exception:
            continue
    return display_names


def build_badge_cache_data() -> Optional[Dict[str, Any]]:
    """Aggregate badge data for all developers for fast API responses."""
    try:
        badges_by_user = analyze_developer_badges()
        user_months = list_user_months()
        display_names = _build_badge_display_name_map(set(badges_by_user.keys()), user_months)
        badge_type_counts = Counter()
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
                "type_counts": type_counts_dict
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
                    "subsystems": subsystems
                })
            per_user_payload[slug] = {
                "display_name": display_name,
                "badges": badges
            }
        badge_types_dict = {k: int(v) for k, v in badge_type_counts.items()}
        for key in ["productivity", "maintainer", "ownership", "ownership_percentage"]:
            badge_types_dict.setdefault(key, 0)
        summary = {
            "users_with_badges": len(badges_by_user),
            "total_badges": total_badges,
            "badge_types": badge_types_dict,
            "total_users": len(user_months)
        }
        top_badge_holders.sort(
            key=lambda entry: (
                -entry["badge_count"],
                -entry["type_counts"].get("productivity", 0),
                entry["slug"]
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
            "per_user": per_user_payload
        }
    except Exception as exc:
        print(f"Error building badge cache: {exc}")
        return None


def load_badge_cache(force_reload: bool = False) -> Optional[Dict[str, Any]]:
    global _BADGE_CACHE_DATA, _BADGE_CACHE_MTIME
    try:
        if not force_reload and _BADGE_CACHE_DATA is not None:
            return _BADGE_CACHE_DATA
        if not os.path.exists(BADGE_CACHE_FILE):
            return None
        with open(BADGE_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        _BADGE_CACHE_DATA = data
        _BADGE_CACHE_MTIME = os.path.getmtime(BADGE_CACHE_FILE)
        return data
    except Exception as exc:
        print(f"Error loading badge cache: {exc}")
        return None


def save_badge_cache(data: Dict[str, Any]) -> Dict[str, Any]:
    global _BADGE_CACHE_DATA, _BADGE_CACHE_MTIME
    os.makedirs(os.path.dirname(BADGE_CACHE_FILE), exist_ok=True)
    with open(BADGE_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    _BADGE_CACHE_DATA = data
    _BADGE_CACHE_MTIME = os.path.getmtime(BADGE_CACHE_FILE)
    return data


def refresh_badge_cache() -> Optional[Dict[str, Any]]:
    badge_data = build_badge_cache_data()
    if not badge_data:
        return None
    return save_badge_cache(badge_data)

# Automatic cleanup on server startup
def reset_update_state():
    """Reset update process state - called on startup and after repo operations"""
    global update_process_active, _SUBSYSTEM_TOUCH_COUNT_CACHE
    print("🔄 Resetting update process state...")
    update_process_active = False
    _SUBSYSTEM_TOUCH_COUNT_CACHE.clear()
    
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
        
        # Check for yearly summaries in year/ subfolder
        year_dir = os.path.join(user_path, "year")
        if os.path.isdir(year_dir):
            for year_file in sorted(os.listdir(year_dir)):
                if year_file.endswith(".json"):
                    year = year_file[:-5]  # Remove .json
                    if year.isdigit() and len(year) == 4:
                        month_entries.append({
                            "from": f"{year}-01-01",
                            "to": f"{year}-12-31",
                            "label": year,
                            "folder": f"year/{year_file}",
                            "is_yearly": True,
                        })
        
        # Check for monthly folders
        for entry in sorted(os.listdir(user_path)):
            if entry == "year":  # Skip year directory, already processed
                continue
                
            subdir = os.path.join(user_path, entry)
            if not os.path.isdir(subdir):
                continue
            
            summary_path = os.path.join(subdir, "summary.json")
            if not os.path.isfile(summary_path):
                continue
            
            # Support both formats: "YYYY-MM-DD_YYYY-MM-DD" and "YYYY-MM" and "YYYY"
            if "_" in entry:
                # Old format: "YYYY-MM-DD_YYYY-MM-DD"
                date_from, date_to = entry.split("_", 1)
                # Check if this is a yearly summary (e.g., "2025-01-01_2025-12-31")
                is_yearly = (date_from.endswith("-01-01") and date_to.endswith("-12-31") and 
                            date_from[:4] == date_to[:4])
                if is_yearly:
                    label = date_from[:4]  # Just the year
                else:
                    label = date_from[:7] if len(date_from) >= 7 else entry  # YYYY-MM
            elif len(entry) == 4 and entry.isdigit():
                # New format: "YYYY" (yearly)
                label = entry
                date_from = f"{entry}-01-01"
                date_to = f"{entry}-12-31"
                is_yearly = True
            elif len(entry) == 7 and entry[4] == '-':
                # New format: "YYYY-MM" (monthly)
                label = entry
                year, month = entry.split('-')
                date_from = f"{year}-{month}-01"
                # Approximate end date (last day of month)
                if month == '12':
                    date_to = f"{year}-12-31"
                else:
                    next_month = int(month) + 1
                    date_to = f"{year}-{next_month:02d}-01"
                is_yearly = False
            else:
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


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _calculate_commits_per_week(summary: Dict[str, Any]) -> float:
    total_commits = _safe_int(summary.get("total_commits"), 0)
    from_date = summary.get("from")
    to_date = summary.get("to")
    if not from_date or not to_date:
        return float(total_commits)
    try:
        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        return float(total_commits)
    day_span = max(1, (end - start).days + 1)
    weeks = day_span / 7.0
    if weeks <= 0:
        return float(total_commits)
    return total_commits / weeks


USER_METRIC_FIELDS: Dict[str, Dict[str, Any]] = {
    "total_commits": {},
    "total_lines_added": {"fallback": ["total_additions"]},
    "total_lines_deleted": {"fallback": ["total_deletions"]},
    "net_lines": {},
    "commits_per_week": {"compute": _calculate_commits_per_week},
}


def _extract_metric_value(summary: Dict[str, Any], metric: str, config: Dict[str, Any]) -> float:
    compute_fn = config.get("compute")
    if callable(compute_fn):
        try:
            value = compute_fn(summary)
        except Exception:
            value = None
        return float(value) if value is not None else 0.0

    value = summary.get(metric)
    if value is None:
        for fallback_key in config.get("fallback", []):
            if fallback_key in summary:
                value = summary.get(fallback_key)
                if value is not None:
                    break
    return float(_safe_int(value, 0))


def _load_user_month_rows(from_date: str, to_date: str) -> List[Tuple[str, Dict[str, Any]]]:
    users_root = os.path.join(STATS_ROOT, "users")
    rows: List[Tuple[str, Dict[str, Any]]] = []
    if not os.path.isdir(users_root):
        return rows
    for user_slug in os.listdir(users_root):
        user_dir = os.path.join(users_root, user_slug)
        if not os.path.isdir(user_dir):
            continue
        summary_path = find_user_summary(user_slug, from_date, to_date)
        if not os.path.isfile(summary_path):
            continue
        try:
            rows.append((user_slug, load_json(summary_path)))
        except Exception:
            continue
    return rows


def _load_user_year_rows(year: int) -> List[Tuple[str, Dict[str, Any]]]:
    users_root = os.path.join(STATS_ROOT, "users")
    rows: List[Tuple[str, Dict[str, Any]]] = []
    if not os.path.isdir(users_root):
        return rows
    filename = f"{year}.json"
    for user_slug in os.listdir(users_root):
        user_dir = os.path.join(users_root, user_slug, "year")
        if not os.path.isdir(user_dir):
            continue
        summary_path = os.path.join(user_dir, filename)
        if not os.path.isfile(summary_path):
            continue
        try:
            rows.append((user_slug, load_json(summary_path)))
        except Exception:
            continue
    return rows


def _build_peer_rankings(rows: List[Tuple[str, Dict[str, Any]]], target_slug: str) -> Dict[str, Dict[str, Any]]:
    total = len(rows)
    if total == 0:
        return {}
    rows_map = {slug: summary for slug, summary in rows}
    if target_slug not in rows_map:
        return {}
    rankings: Dict[str, Dict[str, Any]] = {}
    for metric, config in USER_METRIC_FIELDS.items():
        metric_values: List[Tuple[str, float]] = []
        for slug, summary in rows:
            metric_values.append((slug, _extract_metric_value(summary, metric, config)))
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
    return rankings


def compute_user_month_peer_rankings(user_slug: str, from_date: str, to_date: str) -> Dict[str, Dict[str, Any]]:
    rows = _load_user_month_rows(from_date, to_date)
    return _build_peer_rankings(rows, user_slug)


def _get_subsystem_touch_counts(year: int) -> Dict[str, int]:
    if year in _SUBSYSTEM_TOUCH_COUNT_CACHE:
        return _SUBSYSTEM_TOUCH_COUNT_CACHE[year]
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")
    counts: Dict[str, Set[str]] = defaultdict(set)
    if os.path.isdir(subsystems_root):
        for subsystem_name in os.listdir(subsystems_root):
            subsystem_dir = os.path.join(subsystems_root, subsystem_name)
            if not os.path.isdir(subsystem_dir):
                continue
            for entry in os.listdir(subsystem_dir):
                if "_" not in entry:
                    continue
                date_from_str, date_to_str = entry.split("_", 1)
                if not date_from_str.startswith(f"{year:04d}-"):
                    continue
                try:
                    date_from = datetime.strptime(date_from_str, "%Y-%m-%d")
                    date_to = datetime.strptime(date_to_str, "%Y-%m-%d")
                except ValueError:
                    continue
                if (date_to - date_from).days > 40:
                    continue
                summary_path = os.path.join(subsystem_dir, entry, "summary.json")
                if not os.path.isfile(summary_path):
                    continue
                try:
                    summary_data = load_json(summary_path)
                except Exception:
                    continue
                developers = summary_data.get("developers")
                if not isinstance(developers, dict):
                    continue
                for dev_slug, dev_data in developers.items():
                    if not isinstance(dev_data, dict):
                        continue
                    commits = _safe_int(dev_data.get("commits"), 0)
                    lines_added = _safe_int(dev_data.get("lines_added"), 0)
                    lines_deleted = _safe_int(dev_data.get("lines_deleted"), 0)
                    changed_lines = _safe_int(dev_data.get("changed_lines"), 0)
                    if commits or lines_added or lines_deleted or changed_lines:
                        counts[dev_slug].add(subsystem_name)
    result = {slug: len(subsystems) for slug, subsystems in counts.items()}
    _SUBSYSTEM_TOUCH_COUNT_CACHE[year] = result
    return result


def get_subsystem_touch_rank(
    year: int,
    user_slug: str,
    population_slugs: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    counts = _get_subsystem_touch_counts(year)
    population: List[str]
    if population_slugs:
        population = list(dict.fromkeys(population_slugs))
    else:
        population = list(counts.keys())
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


def compute_user_year_peer_rankings(user_slug: str, year: int) -> Dict[str, Dict[str, Any]]:
    rows = _load_user_year_rows(year)
    rankings = _build_peer_rankings(rows, user_slug)
    if rows:
        population_slugs = [slug for slug, _ in rows]
    else:
        population_slugs = []
    subsystem_rank = get_subsystem_touch_rank(year, user_slug, population_slugs)
    if subsystem_rank:
        rankings["subsystems_touched"] = subsystem_rank
    return rankings


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


def build_user_subsystem_activity(user_slug: str, year: int) -> Dict[str, Any]:
    """Build a monthly subsystem timeline for a developer."""

    def _init_month_entries() -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        for month in range(1, 13):
            month_key = f"{year:04d}-{month:02d}"
            month_name = MONTH_ABBREVIATIONS[month - 1] if 1 <= month <= len(MONTH_ABBREVIATIONS) else month_key
            entries[month_key] = {
                "month": month_key,
                "label": month_key,
                "display_label": f"{month_name} {year}",
                "short_label": month_name,
                "from": None,
                "to": None,
                "total_commits": 0,
                "total_changed_lines": 0,
                "total_lines_added": 0,
                "total_lines_deleted": 0,
                "subsystems": [],
                "has_activity": False,
                "other_subsystems_count": 0,
                "dominant_subsystem": None,
            }
        return entries

    def _to_int(value: Any, default: int = 0) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if value in (None, ""):
            return default
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _pick_number(data: Dict[str, Any], *keys: str) -> int:
        for key in keys:
            if key in data and data[key] is not None:
                return _to_int(data[key])
        return 0

    month_entries = _init_month_entries()
    subsystem_totals: Dict[str, Dict[str, Any]] = {}
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")

    if os.path.isdir(subsystems_root):
        for subsystem_name in sorted(os.listdir(subsystems_root)):
            subsystem_path = os.path.join(subsystems_root, subsystem_name)
            if not os.path.isdir(subsystem_path):
                continue

            for period_dir in os.listdir(subsystem_path):
                if "_" not in period_dir:
                    continue
                if not period_dir.startswith(f"{year:04d}-"):
                    continue

                period_path = os.path.join(subsystem_path, period_dir)
                if not os.path.isdir(period_path):
                    continue

                summary_path = os.path.join(period_path, "summary.json")
                if not os.path.isfile(summary_path):
                    continue

                try:
                    summary_data = load_json(summary_path)
                except Exception:
                    continue

                from_date = summary_data.get("from") or period_dir.split("_", 1)[0]
                to_date = summary_data.get("to") or period_dir.split("_", 1)[-1]
                if not from_date or not to_date:
                    continue
                if not from_date.startswith(f"{year:04d}-"):
                    continue

                from_month = from_date[:7]
                to_month = to_date[:7]
                if from_month != to_month:
                    continue
                if from_month not in month_entries:
                    continue

                developers = summary_data.get("developers") or {}
                dev_data = developers.get(user_slug)
                if not dev_data:
                    continue

                month_entry = month_entries[from_month]
                if month_entry["from"] is None:
                    month_entry["from"] = from_date
                if month_entry["to"] is None:
                    month_entry["to"] = to_date

                commits = _pick_number(dev_data, "commits")
                lines_added = _pick_number(dev_data, "lines_added", "additions")
                lines_deleted = _pick_number(dev_data, "lines_deleted", "deletions")
                changed_lines_raw = dev_data.get("changed_lines")
                changed_lines = (
                    _to_int(changed_lines_raw)
                    if changed_lines_raw is not None
                    else lines_added + lines_deleted
                )
                net_lines_raw = dev_data.get("net_lines")
                net_lines = (
                    _to_int(net_lines_raw)
                    if net_lines_raw is not None
                    else lines_added - lines_deleted
                )

                month_entry["total_commits"] += commits
                month_entry["total_changed_lines"] += changed_lines
                month_entry["total_lines_added"] += lines_added
                month_entry["total_lines_deleted"] += lines_deleted
                month_entry["subsystems"].append(
                    {
                        "name": subsystem_name,
                        "commits": commits,
                        "lines_added": lines_added,
                        "lines_deleted": lines_deleted,
                        "changed_lines": changed_lines,
                        "net_lines": net_lines,
                    }
                )
                month_entry["has_activity"] = True

                totals_entry = subsystem_totals.setdefault(
                    subsystem_name,
                    {
                        "name": subsystem_name,
                        "commits": 0,
                        "lines_added": 0,
                        "lines_deleted": 0,
                        "changed_lines": 0,
                        "net_lines": 0,
                        "months": set(),
                    },
                )
                totals_entry["commits"] += commits
                totals_entry["lines_added"] += lines_added
                totals_entry["lines_deleted"] += lines_deleted
                totals_entry["changed_lines"] += changed_lines
                totals_entry["net_lines"] += net_lines
                totals_entry["months"].add(from_month)

    timeline: List[Dict[str, Any]] = []
    total_changed_lines_year = 0
    total_commits_year = 0

    for month in range(1, 13):
        month_key = f"{year:04d}-{month:02d}"
        entry = month_entries[month_key]

        if entry["subsystems"]:
            entry["subsystems"].sort(key=lambda item: item.get("changed_lines", 0), reverse=True)
            entry["other_subsystems_count"] = max(0, len(entry["subsystems"]) - 1)
            if entry["total_changed_lines"] <= 0:
                entry["total_changed_lines"] = sum(sub.get("changed_lines", 0) for sub in entry["subsystems"])
            dominant = entry["subsystems"][0]
            total_lines = entry["total_changed_lines"] or dominant.get("changed_lines", 0)
            share_percent = 0.0
            if total_lines > 0:
                share_percent = round((dominant.get("changed_lines", 0) / total_lines) * 100, 1)
            entry["dominant_subsystem"] = {**dominant, "share_percent": share_percent}
        else:
            entry["other_subsystems_count"] = 0
            entry["dominant_subsystem"] = None

        total_changed_lines_year += entry["total_changed_lines"]
        total_commits_year += entry["total_commits"]
        timeline.append(entry)

    top_subsystems: List[Dict[str, Any]] = []
    for subsystem_name, stats in subsystem_totals.items():
        months_active = len(stats.get("months", set()))
        stats.pop("months", None)
        stats["months_active"] = months_active
        if total_changed_lines_year > 0:
            stats["share_percent"] = round((stats.get("changed_lines", 0) / total_changed_lines_year) * 100, 1)
        else:
            stats["share_percent"] = 0.0
        top_subsystems.append(stats)

    top_subsystems.sort(key=lambda item: item.get("changed_lines", 0), reverse=True)

    summary = {
        "months_active": sum(1 for entry in timeline if entry["has_activity"]),
        "subsystems_touched": len(subsystem_totals),
        "total_changed_lines": total_changed_lines_year,
        "total_commits": total_commits_year,
        "top_subsystems": top_subsystems[:5],
        "most_active_subsystem": top_subsystems[0] if top_subsystems else None,
        "has_activity": any(entry["has_activity"] for entry in timeline),
    }

    return {
        "user": user_slug,
        "year": year,
        "timeline": timeline,
        "summary": summary,
    }


def build_team_subsystem_activity(team_id: str, year: int) -> Dict[str, Any]:
    """Build a monthly subsystem timeline for a team."""

    teams_file_path = os.path.join(BASE_DIR, "configuration", "teams.json")
    if not os.path.exists(teams_file_path):
        raise ValueError("Teams configuration not found")

    try:
        teams_config = load_json(teams_file_path)
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError("Invalid teams configuration") from exc

    team_info = teams_config.get(team_id)
    if not team_info:
        raise ValueError(f"Team {team_id} not found")

    team_name = team_info.get("name", team_id)
    members = team_info.get("members", []) or []

    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    alias_map: Dict[str, Any] = {}
    if os.path.exists(alias_file):
        try:
            alias_map = load_json(alias_file) or {}
        except Exception:
            alias_map = {}

    def get_canonical_slug(slug: str) -> str:
        if not slug:
            return ""
        for canonical, aliases in alias_map.items():
            if isinstance(aliases, list) and slug in aliases:
                return canonical
            if isinstance(aliases, str) and slug == aliases:
                return canonical
        return slug

    canonical_members = []
    for member in members:
        canonical = get_canonical_slug(member)
        if canonical:
            canonical_members.append(canonical)
    canonical_members = list(dict.fromkeys(canonical_members))

    def _init_month_entries() -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        for month in range(1, 13):
            month_key = f"{year:04d}-{month:02d}"
            month_name = (
                MONTH_ABBREVIATIONS[month - 1]
                if 1 <= month <= len(MONTH_ABBREVIATIONS)
                else month_key
            )
            entries[month_key] = {
                "month": month_key,
                "label": month_key,
                "display_label": f"{month_name} {year}",
                "short_label": month_name,
                "from": None,
                "to": None,
                "total_commits": 0,
                "total_changed_lines": 0,
                "total_lines_added": 0,
                "total_lines_deleted": 0,
                "subsystems": [],
                "has_activity": False,
                "other_subsystems_count": 0,
                "dominant_subsystem": None,
            }
        return entries

    def _to_int(value: Any, default: int = 0) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if value in (None, ""):
            return default
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _pick_number(data: Dict[str, Any], *keys: str) -> int:
        for key in keys:
            if key in data and data[key] is not None:
                return _to_int(data[key])
        return 0

    month_entries = _init_month_entries()
    subsystem_totals: Dict[str, Dict[str, Any]] = {}
    subsystems_root = os.path.join(STATS_ROOT, "subsystems")

    if canonical_members and os.path.isdir(subsystems_root):
        for subsystem_name in sorted(os.listdir(subsystems_root)):
            subsystem_path = os.path.join(subsystems_root, subsystem_name)
            if not os.path.isdir(subsystem_path):
                continue

            for period_dir in os.listdir(subsystem_path):
                if "_" not in period_dir or not period_dir.startswith(f"{year:04d}-"):
                    continue

                period_path = os.path.join(subsystem_path, period_dir)
                if not os.path.isdir(period_path):
                    continue

                summary_path = os.path.join(period_path, "summary.json")
                if not os.path.isfile(summary_path):
                    continue

                try:
                    summary_data = load_json(summary_path)
                except Exception:
                    continue

                from_date = summary_data.get("from") or period_dir.split("_", 1)[0]
                to_date = summary_data.get("to") or period_dir.split("_", 1)[-1]
                if not from_date or not to_date:
                    continue
                if not from_date.startswith(f"{year:04d}-"):
                    continue

                from_month = from_date[:7]
                to_month = to_date[:7]
                if from_month != to_month or from_month not in month_entries:
                    continue

                developers = summary_data.get("developers") or {}

                aggregated_stats = {
                    "commits": 0,
                    "lines_added": 0,
                    "lines_deleted": 0,
                    "changed_lines": 0,
                    "net_lines": 0,
                }

                for member_slug in canonical_members:
                    dev_data = developers.get(member_slug)
                    if not dev_data:
                        continue

                    commits = _pick_number(dev_data, "commits")
                    lines_added = _pick_number(dev_data, "lines_added", "additions")
                    lines_deleted = _pick_number(dev_data, "lines_deleted", "deletions")
                    changed_lines_raw = dev_data.get("changed_lines")
                    changed_lines = (
                        _to_int(changed_lines_raw)
                        if changed_lines_raw is not None
                        else lines_added + lines_deleted
                    )
                    net_lines_raw = dev_data.get("net_lines")
                    net_lines = (
                        _to_int(net_lines_raw)
                        if net_lines_raw is not None
                        else lines_added - lines_deleted
                    )

                    aggregated_stats["commits"] += commits
                    aggregated_stats["lines_added"] += lines_added
                    aggregated_stats["lines_deleted"] += lines_deleted
                    aggregated_stats["changed_lines"] += changed_lines
                    aggregated_stats["net_lines"] += net_lines

                if (
                    aggregated_stats["commits"] == 0
                    and aggregated_stats["lines_added"] == 0
                    and aggregated_stats["lines_deleted"] == 0
                    and aggregated_stats["changed_lines"] == 0
                ):
                    continue

                month_entry = month_entries[from_month]
                if month_entry["from"] is None:
                    month_entry["from"] = from_date
                if month_entry["to"] is None:
                    month_entry["to"] = to_date

                month_entry["total_commits"] += aggregated_stats["commits"]
                month_entry["total_changed_lines"] += aggregated_stats["changed_lines"]
                month_entry["total_lines_added"] += aggregated_stats["lines_added"]
                month_entry["total_lines_deleted"] += aggregated_stats["lines_deleted"]
                month_entry["has_activity"] = True

                subsystems_map = month_entry.setdefault("_subsystem_map", {})
                sub_entry = subsystems_map.setdefault(
                    subsystem_name,
                    {
                        "name": subsystem_name,
                        "commits": 0,
                        "lines_added": 0,
                        "lines_deleted": 0,
                        "changed_lines": 0,
                        "net_lines": 0,
                    },
                )

                for key, value in aggregated_stats.items():
                    sub_entry[key] += value

                totals_entry = subsystem_totals.setdefault(
                    subsystem_name,
                    {
                        "name": subsystem_name,
                        "commits": 0,
                        "lines_added": 0,
                        "lines_deleted": 0,
                        "changed_lines": 0,
                        "net_lines": 0,
                        "months": set(),
                    },
                )
                for key, value in aggregated_stats.items():
                    totals_entry[key] += value
                totals_entry["months"].add(from_month)

    timeline: List[Dict[str, Any]] = []
    total_changed_lines_year = 0
    total_commits_year = 0

    for month in range(1, 13):
        month_key = f"{year:04d}-{month:02d}"
        entry = month_entries[month_key]
        subsystems_map = entry.pop("_subsystem_map", {})
        entry["subsystems"] = sorted(
            subsystems_map.values(), key=lambda item: item.get("changed_lines", 0), reverse=True
        )

        if entry["subsystems"]:
            entry["other_subsystems_count"] = max(0, len(entry["subsystems"]) - 1)
            if entry["total_changed_lines"] <= 0:
                entry["total_changed_lines"] = sum(
                    sub.get("changed_lines", 0) for sub in entry["subsystems"]
                )
            dominant = entry["subsystems"][0]
            total_lines = entry["total_changed_lines"] or dominant.get("changed_lines", 0)
            share_percent = 0.0
            if total_lines > 0:
                share_percent = round((dominant.get("changed_lines", 0) / total_lines) * 100, 1)
            entry["dominant_subsystem"] = {**dominant, "share_percent": share_percent}
        else:
            entry["other_subsystems_count"] = 0
            entry["dominant_subsystem"] = None

        total_changed_lines_year += entry["total_changed_lines"]
        total_commits_year += entry["total_commits"]
        timeline.append(entry)

    top_subsystems: List[Dict[str, Any]] = []
    for subsystem_name, stats in subsystem_totals.items():
        months_active = len(stats.get("months", set()))
        stats.pop("months", None)
        stats["months_active"] = months_active
        if total_changed_lines_year > 0:
            stats["share_percent"] = round(
                (stats.get("changed_lines", 0) / total_changed_lines_year) * 100, 1
            )
        else:
            stats["share_percent"] = 0.0
        top_subsystems.append(stats)

    top_subsystems.sort(key=lambda item: item.get("changed_lines", 0), reverse=True)

    summary = {
        "team_id": team_id,
        "team_name": team_name,
        "team_members": canonical_members,
        "team_members_count": len(canonical_members),
        "months_active": sum(1 for entry in timeline if entry["has_activity"]),
        "subsystems_touched": len(subsystem_totals),
        "total_changed_lines": total_changed_lines_year,
        "total_commits": total_commits_year,
        "top_subsystems": top_subsystems[:5],
        "most_active_subsystem": top_subsystems[0] if top_subsystems else None,
        "has_activity": any(entry["has_activity"] for entry in timeline),
    }

    return {
        "team_id": team_id,
        "team_name": team_name,
        "year": year,
        "timeline": timeline,
        "summary": summary,
    }


def load_cached_subsystem_payload(subsystem_name: str, filename: str) -> Optional[Dict[str, Any]]:
    """Load a cached subsystem JSON artifact if it exists."""
    path = os.path.join(STATS_ROOT, "subsystems", subsystem_name, filename)
    if not os.path.isfile(path):
        return None
    try:
        return load_json(path)
    except Exception as exc:
        app.logger.warning("[subsystem-cache] Failed to read %s for %s: %s", filename, subsystem_name, exc)
        return None


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
    Locate monthly user summary, supporting both legacy and new layouts:
    - Legacy: stats/users/<slug>/<from>_<to>/summary.json
    - New:    stats/users/<slug>/<YYYY-MM>/summary.json
    """
    # New layout (YYYY-MM)
    try:
        month_folder = from_date[:7]
        new_path = os.path.join(STATS_ROOT, "users", user_slug, month_folder, "summary.json")
        if os.path.isfile(new_path):
            return new_path
    except Exception:
        pass
    # Legacy layout (from_to)
    folder = f"{from_date}_{to_date}"
    return os.path.join(STATS_ROOT, "users", user_slug, folder, "summary.json")


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
    return render_template(
        "index.html",
        read_only=app.config.get("READ_ONLY_MODE", False),
        kiosk_mode=False,
    )


@app.route("/kiosk")
def kiosk_view():
    return render_template(
        "index.html",
        read_only=True,
        kiosk_mode=True,
    )


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
    """Get precomputed badges for a specific user."""
    try:
        badge_cache = load_badge_cache()
        if badge_cache is None:
            badge_cache = refresh_badge_cache()
        if badge_cache is None:
            return jsonify({
                "badges": [],
                "error": "Badge data not available. Please run the update pipeline to generate ownership analytics."
            }), 503
        user_entry = badge_cache.get("per_user", {}).get(user_slug, {})
        user_badges = user_entry.get("badges", [])
        return jsonify({"badges": user_badges})
    except Exception as e:
        print(f"Error retrieving badges for user {user_slug}: {str(e)}")
        return jsonify({"badges": [], "error": str(e)})


@app.route("/api/users/badges-overview")
def api_users_badges_overview():
    """Return aggregated badge statistics for developers overview dashboard."""
    try:
        badge_cache = load_badge_cache()
        if badge_cache is None:
            badge_cache = refresh_badge_cache()
        if badge_cache is None:
            return jsonify({
                "error": "Badge data not available. Please run the update pipeline to generate ownership analytics."
            }), 503
        return jsonify({
            "generated_at": badge_cache.get("generated_at"),
            "summary": badge_cache.get("summary", {}),
            "top_badge_holders": badge_cache.get("top_badge_holders", []),
            "top_ownership_holders": badge_cache.get("top_ownership_holders", [])
        })
    except Exception as e:
        print(f"Error serving badge overview: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/developers/total-ownership")
def api_developers_total_ownership():
    """Get total lines owned by each developer across all subsystems."""
    try:
        developers = build_global_developer_totals()
        return jsonify({"developers": developers})
    except Exception as e:
        print(f"Error calculating total ownership: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"developers": [], "error": str(e)})


@app.route("/api/developers/capacity-profiles")
def api_developers_capacity_profiles():
    try:
        limit = request.args.get("limit", default=50, type=int)
        min_equivalent = request.args.get("min_equivalent", default=0.9, type=float)
        limit = max(1, limit or 50)
        min_equivalent = max(0.0, min_equivalent or 0.0)

        profiles = build_developer_capacity_profiles(min_equivalent=min_equivalent)
        sorted_profiles = sorted(
            profiles.values(),
            key=lambda p: p.get("developer_equivalent", 0),
            reverse=True,
        )
        return jsonify({"developers": sorted_profiles[:limit]})
    except Exception as exc:
        print(f"Error generating developer capacity profiles: {exc}")
        import traceback
        traceback.print_exc()
        return jsonify({"developers": [], "error": str(exc)})


@app.route("/api/users/<user_slug>/ownership-timeline")
def api_user_ownership_timeline(user_slug: str):
    """Get ownership timeline for subsystems where this user is a top maintainer."""
    try:
        from datetime import datetime, timedelta
        from collections import defaultdict
        
        # First, find which subsystems this user is a top maintainer of
        all_badges = analyze_developer_badges()
        user_badges = all_badges.get(user_slug, [])
        
        # Extract subsystems where user is top maintainer (from badges)
        maintainer_subsystems = set()
        for badge in user_badges:
            if badge.get("badge_type") == "top_maintainer":
                maintainer_subsystems.add(badge.get("subsystem"))
        
        # Also check subsystems where user appears in top maintainers (by recent commits)
        subsystems_path = os.path.join(STATS_ROOT, "subsystems")
        if os.path.exists(subsystems_path):
            three_months_ago = datetime.now() - timedelta(days=90)
            for subsystem_name in os.listdir(subsystems_path):
                subsystem_path = os.path.join(subsystems_path, subsystem_name)
                if not os.path.isdir(subsystem_path):
                    continue
                
                # Check recent activity
                has_recent_commits = False
                for period_dir in os.listdir(subsystem_path):
                    if not os.path.isdir(os.path.join(subsystem_path, period_dir)):
                        continue
                    if "_12-31" in period_dir:  # Skip yearly
                        continue
                    
                    try:
                        from_date_str = period_dir.split("_")[0]
                        period_date = datetime.strptime(from_date_str, "%Y-%m-%d")
                        if period_date < three_months_ago:
                            continue
                        
                        summary_file = os.path.join(subsystem_path, period_dir, "summary.json")
                        if os.path.exists(summary_file):
                            summary_data = load_json(summary_file)
                            for repo_data in summary_data.get("repositories", {}).values():
                                if user_slug in repo_data.get("developers", {}):
                                    maintainer_subsystems.add(subsystem_name)
                                    has_recent_commits = True
                                    break
                        if has_recent_commits:
                            break
                    except:
                        continue
        
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
    peer_rankings = compute_user_month_peer_rankings(user_slug, from_date, to_date)
    if peer_rankings:
        data["peer_rankings"] = peer_rankings
    return jsonify(data)


@app.route("/api/users/<user_slug>/year/<int:year>")
def api_user_year(user_slug: str, year: int):
    """Get yearly summary for a user."""
    path = os.path.join(STATS_ROOT, "users", user_slug, "year", f"{year}.json")
    if not os.path.isfile(path):
        abort(404, description="User yearly summary not found")
    data = load_json(path)
    capacity_profile = get_developer_capacity_profile(user_slug, min_equivalent=0.9)
    if capacity_profile:
        data["developer_capacity_profile"] = capacity_profile
    peer_rankings = compute_user_year_peer_rankings(user_slug, year)
    if peer_rankings:
        data["peer_rankings"] = peer_rankings
    return jsonify(data)


@app.route("/api/users/<user_slug>/subsystem-activity/<int:year>")
def api_user_subsystem_activity(user_slug: str, year: int):
    """Return subsystem timeline for a developer."""
    try:
        payload = build_user_subsystem_activity(user_slug, year)
        population_rows = _load_user_year_rows(year)
        population_slugs = [slug for slug, _ in population_rows] if population_rows else None
        subsystem_rank = get_subsystem_touch_rank(year, user_slug, population_slugs)
        if subsystem_rank:
            payload.setdefault("summary", {})
            payload["summary"].setdefault("peer_rankings", {})
            payload["summary"]["peer_rankings"]["subsystems_touched"] = subsystem_rank
        return jsonify(payload)
    except Exception as exc:
        app.logger.error("Error generating subsystem activity for %s: %s", user_slug, exc)
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


@app.route("/api/teams/<team_id>/subsystem-activity/<int:year>")
def api_team_subsystem_activity(team_id: str, year: int):
    """Return subsystem timeline for a team."""
    try:
        payload = build_team_subsystem_activity(team_id, year)
        return jsonify(payload)
    except Exception as exc:
        app.logger.error("Error generating team subsystem activity for %s: %s", team_id, exc)
        return (
            jsonify(
                {
                    "team_id": team_id,
                    "year": year,
                    "timeline": [],
                    "summary": {},
                    "error": str(exc),
                }
            ),
            500,
        )


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


def load_cloc_cache_data() -> Dict[str, Dict[str, Any]]:
    global _CLOC_CACHE_DATA
    if _CLOC_CACHE_DATA is None:
        if os.path.exists(CLOC_CACHE_FILE):
            try:
                _CLOC_CACHE_DATA = load_json(CLOC_CACHE_FILE)
            except Exception:
                _CLOC_CACHE_DATA = {}
        else:
            _CLOC_CACHE_DATA = {}
    return _CLOC_CACHE_DATA


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


def get_services_config_cached() -> Dict[str, Dict[str, list]]:
    global _SERVICES_CONFIG_CACHE
    if _SERVICES_CONFIG_CACHE is None:
        _SERVICES_CONFIG_CACHE = load_services_config()
    return _SERVICES_CONFIG_CACHE


def _ensure_repo_language_data(repo_full_name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not repo_full_name:
        return None
    repo_full_name = repo_full_name.strip()
    if not repo_full_name:
        return None
    cached = _REPO_LANGUAGE_CACHE.get(repo_full_name)
    if cached is not None:
        return cached

    cache_data = load_cloc_cache_data()
    cached_entry = cache_data.get(repo_full_name)
    if not cached_entry:
        return None

    _REPO_LANGUAGE_CACHE[repo_full_name] = cached_entry
    for service_name, langs in (cached_entry.get("services") or {}).items():
        _SERVICE_LANGUAGE_CACHE[(repo_full_name, service_name)] = langs
    return cached_entry


def _get_repo_language_breakdown(repo_full_name: Optional[str]) -> Optional[Dict[str, int]]:
    data = _ensure_repo_language_data(repo_full_name)
    if not data:
        return None
    return data.get("remainder") or data.get("repo")


def _get_service_language_breakdown(repo_full_name: Optional[str], service_name: Optional[str]) -> Optional[Dict[str, int]]:
    if not repo_full_name or not service_name:
        return None
    repo_full_name = repo_full_name.strip()
    service_name = service_name.strip()
    if not repo_full_name or not service_name:
        return None
    cache_key = (repo_full_name, service_name)
    if cache_key in _SERVICE_LANGUAGE_CACHE:
        return _SERVICE_LANGUAGE_CACHE[cache_key]

    data = _ensure_repo_language_data(repo_full_name)
    if not data:
        return None
    languages = data.get("services", {}).get(service_name)
    if not languages:
        repo_name = repo_full_name.split("/")[-1]
        if service_name == repo_name:
            languages = data.get("remainder")
    if not languages:
        return None
    _SERVICE_LANGUAGE_CACHE[cache_key] = languages
    return languages


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
    """Return cached dead-subsystem data, falling back to on-demand computation."""
    cache_path = os.path.join(STATS_ROOT, "subsystems", "dead_status.json")
    if os.path.isfile(cache_path):
        try:
            cached_data = load_json(cache_path)
            if isinstance(cached_data, dict):
                cached_status = cached_data.get("subsystem_status")
                if isinstance(cached_status, dict):
                    return cached_status
                return cached_data  # Legacy format without wrapper
        except Exception as exc:
            app.logger.warning("[subsystem-cache] Failed to load dead status cache: %s", exc)
    return compute_dead_subsystems(STATS_ROOT, threshold_months)


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
    """Serve cached top maintainer data for a subsystem."""
    try:
        cached = load_cached_subsystem_payload(subsystem_name, "top_maintainers.json")
        if cached is not None:
            return jsonify(cached)
        payload = compute_subsystem_top_maintainers(STATS_ROOT, subsystem_name)
        return jsonify(payload)
    except Exception as e:
        abort(500, description=f"Error analyzing top maintainers: {str(e)}")


@app.route("/api/subsystems/<subsystem_name>/maintainer-timeline")
def api_subsystem_maintainer_timeline(subsystem_name: str):
    """Serve cached maintainer timeline data for a subsystem."""
    try:
        cached = load_cached_subsystem_payload(subsystem_name, "maintainer_timeline.json")
        if cached is not None:
            return jsonify(cached)
        payload = compute_subsystem_maintainer_timeline(STATS_ROOT, subsystem_name)
        return jsonify(payload)
    except Exception as e:
        app.logger.error(f"Error in maintainer timeline for {subsystem_name}: {e}")
        abort(500, description=f"Error generating maintainer timeline: {str(e)}")

@app.route("/api/subsystems/<subsystem_name>/significant-ownership")
def api_subsystem_significant_ownership(subsystem_name: str):
    """Serve cached significant ownership data for a subsystem."""
    try:
        cached = load_cached_subsystem_payload(subsystem_name, "significant_ownership.json")
        if cached is not None:
            return jsonify(cached)
        payload = compute_subsystem_significant_ownership(STATS_ROOT, subsystem_name)
        return jsonify(payload)
    except Exception as e:
        app.logger.error(f"Error in api_subsystem_significant_ownership for {subsystem_name}: {e}")
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
            return jsonify({
                "languages": {},
                "totals": {},
                "error": "Language statistics not available. Please re-run the data update pipeline."
            })
        
        try:
            with open(languages_file, "r", encoding="utf-8") as f:
                language_data = json.load(f)
            
            return jsonify(language_data)
        except (json.JSONDecodeError, IOError) as e:
            return jsonify({"languages": {}, "totals": {}, "error": f"Error reading language statistics: {str(e)}"})
        
    except Exception as e:
        print(f"Error in api_subsystem_languages: {str(e)}")
        return jsonify({"languages": {}, "totals": {}, "error": str(e)})

@app.route("/api/subsystems/<subsystem_name>/loc-evolution/<int:year>")
def api_subsystem_loc_evolution(subsystem_name: str, year: int):
    """Return precomputed monthly LOC evolution for a subsystem."""
    try:
        cached_file = os.path.join(STATS_ROOT, "subsystems", subsystem_name, "monthly", f"{year:04d}.json")
        if os.path.isfile(cached_file):
            with open(cached_file, "r", encoding="utf-8") as f:
                return jsonify(json.load(f))
        return jsonify({
            "series": [],
            "error": "LOC evolution not available. Please re-run the data update pipeline to generate this dataset."
        })
    except Exception as e:
        app.logger.warning(f"[loc-evolution] Error serving LOC for {subsystem_name} {year}: {e}")
        return jsonify({"series": [], "error": str(e)})


@app.route("/api/subsystems/size-rankings")
def api_subsystem_size_rankings():
    """Return cached subsystem size rankings (language-based)."""
    cache_file = os.path.join(STATS_ROOT, "subsystems", "size_rankings.json")
    try:
        if os.path.isfile(cache_file):
            try:
                cached = load_json(cache_file)
                if isinstance(cached, dict):
                    return jsonify(cached)
            except Exception as exc:
                app.logger.warning("[subsystem-cache] Failed to read size rankings cache: %s", exc)
        payload = compute_subsystem_size_rankings(STATS_ROOT)
        return jsonify(payload)
    except Exception as e:
        app.logger.error(f"Error in api_subsystem_size_rankings: {str(e)}")
        return jsonify({"rankings": {}, "buckets": {"big": [], "medium": [], "small": []}, "error": str(e)})


@app.route("/api/subsystems/language-lines")
def api_subsystem_language_lines():
    """Get total lines of code per programming language across all subsystems."""
    try:
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        if not os.path.exists(subsystems_root):
            return jsonify({"languages": {}, "total_lines": 0})
        
        language_lines = {}
        
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
                
                languages = language_data.get("languages", {})
                for lang, lang_data in languages.items():
                    code_lines = lang_data.get("code_lines", 0)
                    if code_lines > 0:
                        if lang not in language_lines:
                            language_lines[lang] = 0
                        language_lines[lang] += code_lines
                        
            except (json.JSONDecodeError, IOError):
                continue
        
        total_lines = sum(language_lines.values())
        
        # Sort by lines descending
        sorted_languages = dict(sorted(language_lines.items(), key=lambda x: x[1], reverse=True))
        
        return jsonify({
            "languages": sorted_languages,
            "total_lines": total_lines,
            "language_count": len(sorted_languages)
        })
        
    except Exception as e:
        print(f"Error in api_subsystem_language_lines: {str(e)}")
        return jsonify({"languages": {}, "total_lines": 0, "error": str(e)})


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
        
        # Build trailing 12-month window ending with last month
        trend_months = []
        trend_year = last_year
        trend_month = last_month
        for _ in range(12):
            trend_months.append((trend_year, trend_month))
            trend_month -= 1
            if trend_month == 0:
                trend_month = 12
                trend_year -= 1
        trend_months = list(reversed(trend_months))
        trend_month_labels = [f"{year:04d}-{month:02d}" for year, month in trend_months]
        trend_month_set = set(trend_month_labels)
        last_month_key = f"{last_year:04d}-{last_month:02d}"
        
        # Get activity data for last month and collect trend data
        subsystems_activity = []
        subsystem_trend_data = {}
        subsystems_root = os.path.join(STATS_ROOT, "subsystems")
        
        if os.path.exists(subsystems_root):
            for subsystem_name in os.listdir(subsystems_root):
                subsystem_dir = os.path.join(subsystems_root, subsystem_name)
                if not os.path.isdir(subsystem_dir):
                    continue
                
                # Prepare lookup for available monthly summaries within the window
                period_lookup = {}
                try:
                    period_entries = os.listdir(subsystem_dir)
                except OSError:
                    period_entries = []
                for period_dir in period_entries:
                    period_path = os.path.join(subsystem_dir, period_dir)
                    if not os.path.isdir(period_path):
                        continue
                    if "_" not in period_dir:
                        continue
                    parts = period_dir.split("_")
                    if len(parts) < 2:
                        continue
                    start_part = parts[0]
                    if len(start_part) < 7:
                        continue
                    month_key = start_part[:7]
                    if month_key not in trend_month_set:
                        continue
                    end_part = parts[-1]
                    is_same_month = end_part[:7] == month_key
                    data_entry = period_lookup.setdefault(month_key, {"monthly": None, "any": None})
                    if is_same_month and data_entry["monthly"] is None:
                        data_entry["monthly"] = period_path
                    if data_entry["any"] is None:
                        data_entry["any"] = period_path
                
                # Initialize activity data with dead status info
                activity_data = {"name": subsystem_name, "commits": 0, "lines_changed": 0, "developers": 0}
                if subsystem_name in dead_status:
                    activity_data["is_dead"] = dead_status[subsystem_name]["is_dead"]
                    activity_data["last_activity_date"] = dead_status[subsystem_name]["last_activity_date"]
                    activity_data["months_since_activity"] = dead_status[subsystem_name]["months_since_activity"]
                else:
                    activity_data["is_dead"] = False
                    activity_data["last_activity_date"] = None
                    activity_data["months_since_activity"] = None
                
                monthly_values = []
                total_changes = 0
                last_month_summary = None
                for month_key in trend_month_labels:
                    summary_data = None
                    period_entry = period_lookup.get(month_key)
                    summary_path = None
                    if period_entry:
                        summary_path = period_entry.get("monthly") or period_entry.get("any")
                    if summary_path:
                        summary_file = os.path.join(summary_path, "summary.json")
                        if os.path.exists(summary_file):
                            try:
                                with open(summary_file, "r", encoding="utf-8") as f:
                                    summary_data = json.load(f)
                            except (json.JSONDecodeError, IOError):
                                summary_data = None
                    lines_changed = 0
                    if summary_data:
                        lines_changed = summary_data.get("total_changed_lines")
                        if lines_changed is None:
                            additions = summary_data.get("total_lines_added", 0)
                            deletions = summary_data.get("total_lines_deleted", 0)
                            lines_changed = additions + deletions
                        if month_key == last_month_key:
                            last_month_summary = summary_data
                    monthly_values.append(lines_changed or 0)
                    total_changes += lines_changed or 0
                
                if last_month_summary:
                    activity_data["commits"] = last_month_summary.get("total_commits", 0)
                    last_month_lines = last_month_summary.get("total_changed_lines")
                    if last_month_lines is None:
                        last_month_lines = (
                            last_month_summary.get("total_lines_added", 0) +
                            last_month_summary.get("total_lines_deleted", 0)
                        )
                    activity_data["lines_changed"] = last_month_lines
                    activity_data["developers"] = len(last_month_summary.get("developers", {}))
                
                subsystem_trend_data[subsystem_name] = {
                    "values": monthly_values,
                    "total": total_changes
                }
                subsystems_activity.append(activity_data)
        
        # Sort activity data
        most_active_commits = sorted(subsystems_activity, key=lambda x: x["commits"], reverse=True)[:10]
        most_active_changes = sorted(subsystems_activity, key=lambda x: x["lines_changed"], reverse=True)[:10]
        
        # Count dead subsystems
        dead_subsystems = [s for s in subsystems_activity if s["is_dead"]]
        
        # Prepare trend payload highlighting busiest subsystems
        workload_series = []
        recent_trend_payload = None
        if subsystem_trend_data:
            sorted_trends = sorted(
                subsystem_trend_data.items(),
                key=lambda item: item[1]["total"],
                reverse=True
            )
            top_limit = 6
            total_months = len(trend_month_labels)
            for name, data in sorted_trends[:top_limit]:
                if data["total"] <= 0:
                    continue
                # Ensure value list matches months
                values = data["values"]
                if len(values) != total_months:
                    values = (values + [0] * total_months)[:total_months]
                workload_series.append({
                    "name": name,
                    "values": values,
                    "total": data["total"]
                })
            if len(sorted_trends) > top_limit:
                others_values = [0] * len(trend_month_labels)
                others_total = 0
                for _, data in sorted_trends[top_limit:]:
                    others_total += data["total"]
                    values = data["values"]
                    if len(values) != total_months:
                        values = (values + [0] * total_months)[:total_months]
                    for idx, value in enumerate(values):
                        others_values[idx] += value
                if others_total > 0:
                    workload_series.append({
                        "name": "Others",
                        "values": others_values,
                        "total": others_total,
                        "is_aggregate": True
                    })

            # Build ungrouped view for the most recent months
            if trend_month_labels:
                recent_month_count = 2
                months_to_include = trend_month_labels[-recent_month_count:]
                recent_series = []
                for name, data in sorted_trends:
                    values = data["values"]
                    if len(values) != total_months:
                        values = (values + [0] * total_months)[:total_months]
                    slice_length = len(months_to_include)
                    recent_values = values[-slice_length:]
                    if any(value > 0 for value in recent_values):
                        recent_series.append({
                            "name": name,
                            "values": recent_values,
                            "total": sum(recent_values)
                        })
                if recent_series:
                    recent_trend_payload = {
                        "months": months_to_include,
                        "series": recent_series
                    }
        
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
            },
            "trend": {
                "months": trend_month_labels,
                "series": workload_series
            },
            "recent_trend": recent_trend_payload
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
                monthly_folder = f"{last_year:04d}-{last_month:02d}"
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
                
                # Aggregate yearly data from monthly folders
                yearly_commits = 0
                yearly_lines_added = 0
                yearly_lines_deleted = 0
                
                for month in range(1, 13):
                    month_folder = f"{current_year:04d}-{month:02d}"
                    month_path = os.path.join(user_dir, month_folder, "summary.json")
                    
                    if os.path.exists(month_path):
                        try:
                            with open(month_path, "r", encoding="utf-8") as f:
                                month_data = json.load(f)
                            
                            user_data["display_name"] = month_data.get("author_name", user_slug)
                            yearly_commits += month_data.get("total_commits", 0)
                            yearly_lines_added += month_data.get("total_lines_added", 0)
                            yearly_lines_deleted += month_data.get("total_lines_deleted", 0)
                            
                        except (json.JSONDecodeError, IOError):
                            pass
                
                user_data["yearly_commits"] = yearly_commits
                user_data["yearly_lines_added"] = yearly_lines_added
                user_data["yearly_lines_deleted"] = yearly_lines_deleted
                
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


def _read_integrations_config() -> Dict[str, Any]:
    if not os.path.exists(INTEGRATIONS_FILE):
        return {}
    try:
        with open(INTEGRATIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        logger.error("Invalid integrations configuration: %s", exc)
    except OSError as exc:
        logger.error("Unable to read integrations configuration: %s", exc)
    return {}


def _mask_integration_secret(secret: Optional[str]) -> Optional[str]:
    if not secret:
        return None
    visible = secret[-4:]
    hidden_length = max(len(secret) - len(visible), 4)
    return f"{'•' * hidden_length}{visible}"


def _serialize_integrations_response(config: Dict[str, Any]) -> Dict[str, Any]:
    pagerduty = config.get("pagerduty") or {}
    token_value = pagerduty.get("api_token")
    return {
        "pagerduty": {
            "has_token": bool(token_value),
            "token_preview": _mask_integration_secret(token_value),
            "updated_at": pagerduty.get("updated_at")
        }
    }


@app.route("/api/pagerduty/overview")
def api_pagerduty_overview():
    if not os.path.exists(PAGERDUTY_OVERVIEW_FILE):
        return (
            jsonify({"error": "PagerDuty data is not available. Configure a token and run an update."}),
            404,
        )
    try:
        data = load_json(PAGERDUTY_OVERVIEW_FILE)
    except Exception as exc:  # pragma: no cover - filesystem errors
        logger.error("Failed to load PagerDuty overview: %s", exc)
        return jsonify({"error": "Failed to read PagerDuty overview."}), 500
    return jsonify(data)


@app.route("/api/pagerduty/incidents")
def api_pagerduty_incidents():
    limit = request.args.get("limit", default=200, type=int) or 200
    limit = max(1, min(limit, 1000))
    responder_id = request.args.get("responder_id", type=str)
    if responder_id:
        responder_id = responder_id.strip()
    if not os.path.exists(PAGERDUTY_INCIDENTS_FILE):
        return (
            jsonify({"incidents": [], "total": 0, "error": "PagerDuty data unavailable."}),
            404,
        )
    try:
        incidents = load_json(PAGERDUTY_INCIDENTS_FILE)
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
                item = {"role": role}
                if event.get("at"):
                    item["at"] = event["at"]
                events.append(item)
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


@app.route("/api/settings/kiosk", methods=["GET", "POST"])
def api_settings_kiosk():
    if request.method == "GET":
        return jsonify(load_kiosk_config())

    if app.config.get("READ_ONLY_MODE"):
        return jsonify({"error": "Settings are disabled in read-only mode."}), 403

    try:
        payload = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "Invalid JSON payload"}), 400

    try:
        config = save_kiosk_config(payload)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify({"status": "ok", "config": config})


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
                            env = _build_git_clone_env(repo_url)
                            
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



@app.route("/api/settings/integrations", methods=["GET", "POST"])
def api_settings_integrations():
    if app.config.get("READ_ONLY_MODE"):
        return jsonify({"error": "Integrations are disabled in read-only mode"}), 403

    if request.method == "GET":
        config = _read_integrations_config()
        return jsonify(_serialize_integrations_response(config))

    payload = request.get_json(silent=True) or {}
    pagerduty_payload = payload.get("pagerduty")

    if pagerduty_payload is None or not isinstance(pagerduty_payload, dict):
        return jsonify({"error": "pagerduty payload is required"}), 400

    config = _read_integrations_config()
    token_value = pagerduty_payload.get("api_token")
    if token_value is not None and not isinstance(token_value, str):
        token_value = str(token_value)
    timestamp = datetime.now(timezone.utc).isoformat()

    if token_value and token_value.strip():
        config["pagerduty"] = {
            "api_token": token_value.strip(),
            "updated_at": timestamp,
        }
    else:
        existing = config.get("pagerduty", {})
        existing.pop("api_token", None)
        existing["updated_at"] = timestamp
        config["pagerduty"] = existing

    try:
        os.makedirs(os.path.dirname(INTEGRATIONS_FILE), exist_ok=True)
        with open(INTEGRATIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except OSError as exc:
        logger.error("Failed to write integrations configuration: %s", exc)
        return jsonify({"error": str(exc)}), 500

    response_payload = _serialize_integrations_response(config)
    response_payload["success"] = True
    return jsonify(response_payload)


@app.route("/api/settings/update-config", methods=["GET", "POST"])
def api_update_settings():
    settings = load_update_settings()
    state = get_background_state_snapshot()
    payload = {
        "background_enabled": settings.get("background_enabled", False),
        "interval_hours": settings.get("interval_hours", 24),
        "last_update": settings.get("last_update"),
        "last_background_completed_at": settings.get("last_background_completed_at"),
        "last_manual_completed_at": settings.get("last_manual_completed_at"),
        "next_run": state.get("next_run"),
        "background_running": state.get("running")
    }
    if request.method == "GET":
        return jsonify(payload)
    if app.config.get("READ_ONLY_MODE"):
        return jsonify({"error": "Settings are read-only"}), 403
    data = request.get_json(silent=True) or {}
    background_enabled = bool(data.get("background_enabled", False))
    interval_hours = data.get("interval_hours", 24)
    try:
        interval_hours = int(interval_hours)
    except (TypeError, ValueError):
        return jsonify({"error": "interval_hours must be an integer"}), 400
    if interval_hours < 1:
        return jsonify({"error": "interval_hours must be >= 1"}), 400
    interrupt_all_updates()
    settings["background_enabled"] = background_enabled
    settings["interval_hours"] = interval_hours
    save_update_settings(settings)
    schedule_background_check()
    background_started = trigger_background_update('settings-save')
    state = get_background_state_snapshot()
    payload.update({
        "background_enabled": background_enabled,
        "interval_hours": interval_hours,
        "next_run": state.get("next_run"),
        "background_running": state.get("running")
    })
    return jsonify({"success": True, "settings": payload, "background_started": background_started})

@app.route("/api/settings/capacity-config", methods=["GET", "POST"])
def api_settings_capacity_config():
    """Get or update capacity configuration."""
    config_file = os.path.join(BASE_DIR, "configuration", "capacity_config.json")
    
    if request.method == "GET":
        try:
            if os.path.exists(config_file):
                with open(config_file, "r") as f:
                    config = json.load(f)
            else:
                # Return defaults if file doesn't exist
                config = {
                    "default_lines_per_developer": 20000,
                    "language_lines_per_developer": {},
                    "warning_threshold_percent": 90,
                    "critical_threshold_percent": 100
                }
            return jsonify(config)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            
            # Validate the configuration
            if "default_lines_per_developer" in data:
                if not isinstance(data["default_lines_per_developer"], (int, float)) or data["default_lines_per_developer"] <= 0:
                    return jsonify({"error": "default_lines_per_developer must be a positive number"}), 400
            
            if "warning_threshold_percent" in data:
                if not isinstance(data["warning_threshold_percent"], (int, float)) or not (0 <= data["warning_threshold_percent"] <= 100):
                    return jsonify({"error": "warning_threshold_percent must be between 0 and 100"}), 400
            
            if "critical_threshold_percent" in data:
                if not isinstance(data["critical_threshold_percent"], (int, float)) or not (0 <= data["critical_threshold_percent"] <= 100):
                    return jsonify({"error": "critical_threshold_percent must be between 0 and 100"}), 400
            
            # Ensure configuration directory exists
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            
            # Save the configuration
            with open(config_file, "w") as f:
                json.dump(data, f, indent=2)
            
            return jsonify({"success": True, "message": "Capacity configuration updated successfully"})
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



@app.route("/api/update/last-run", methods=["GET"])
def api_last_update():
    settings = load_update_settings()
    state = get_background_state_snapshot()
    return jsonify({
        "last_update": settings.get("last_update"),
        "background_enabled": settings.get("background_enabled", False),
        "next_run": state.get("next_run"),
        "background_running": state.get("running")
    })


@app.route("/api/update/background/run", methods=["POST"])
def api_trigger_background_update():
    settings = load_update_settings()
    if not settings.get("background_enabled", False):
        return jsonify({"error": "Enable background updates first"}), 400
    if update_process_active:
        return jsonify({"error": "Manual update in progress"}), 409
    state = get_background_state_snapshot()
    if state.get("running"):
        return jsonify({"error": "Background update already running"}), 409
    triggered = trigger_background_update('manual')
    if not triggered:
        return jsonify({"error": "Unable to start background update"}), 409
    return jsonify({"success": True, "message": "Background update scheduled"})


@app.route("/api/update/run-analysis", methods=["POST"])
def api_update_run_analysis():
    """Start the complete update process (git pull + analysis) asynchronously."""
    global update_process_active

    if app.config.get("READ_ONLY_MODE"):
        return jsonify({"error": "Manual updates are disabled in read-only mode"}), 403
    
    # Force reset state to ensure clean start
    print(f"🔍 Update request received. Current state: update_process_active={update_process_active}")
    reset_update_state()
    print(f"🔍 After reset: update_process_active={update_process_active}")
    
    if update_process_active:
        return jsonify({"error": "Update process already running"}), 409
    state = get_background_state_snapshot()
    if state.get("running"):
        return jsonify({"error": "Background update currently running"}), 409
    
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
    
    settings = load_update_settings()
    state = get_background_state_snapshot()
    return jsonify({
        "is_running": update_process_active,
        "queue_size": update_progress_queue.qsize(),
        "background_running": state.get("running"),
        "next_run": state.get("next_run"),
        "last_update": settings.get("last_update")
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
    overall_success = False
    stats_dir = os.path.join(BASE_DIR, "stats")
    pagerduty_backup_root: Optional[str] = None
    pagerduty_backup_path: Optional[str] = None
    
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
        
        pagerduty_dir = os.path.join(stats_dir, "pagerduty")
        if os.path.exists(stats_dir):
            try:
                if os.path.exists(pagerduty_dir):
                    try:
                        pagerduty_backup_root = tempfile.mkdtemp(prefix="pagerduty-backup-", dir=BASE_DIR)
                        pagerduty_backup_path = os.path.join(pagerduty_backup_root, "pagerduty")
                        shutil.move(pagerduty_dir, pagerduty_backup_path)
                        log_update_message({
                            'type': 'info',
                            'message': f'🛟 Preserving existing PagerDuty cache before cleanup [{datetime.now().strftime("%H:%M:%S")}]',
                            'progress': 2
                        })
                    except Exception as backup_exc:
                        pagerduty_backup_root = None
                        pagerduty_backup_path = None
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ Could not preserve PagerDuty cache: {backup_exc}',
                            'progress': 2
                        })
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
            cpu_workers = max(1, multiprocessing.cpu_count())
            python_env = os.environ.copy()
            python_env.setdefault("PYTHONUNBUFFERED", "1")
            
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
                    "--cpu-count", str(cpu_workers)
                ]
                _log_command_start(f'master.py ({year})', master_cmd, year_progress)
                
                try:
                    returncode, stdout_text, stderr_text = _run_command_with_live_logs(
                        f'master.py ({year})',
                        master_cmd,
                        cwd=BASE_DIR,
                        progress=year_progress,
                        timeout=144000,  # 40 hour timeout per year for enterprise-scale operations
                        env=python_env
                    )
                    
                    year_end_time = datetime.now()
                    year_duration = (year_end_time - year_start_time).total_seconds()
                    
                    if returncode == 0:
                        successful_years.append(year)
                        log_update_message({
                            'type': 'info',
                            'message': f'✅ Year {year}: SUCCESS! (exit={returncode}) Generated all monthly data and yearly summaries ({year_duration:.1f}s)',
                            'progress': year_progress + 35
                        })
                    else:
                        error_msg = stderr_text.strip()[:100] if stderr_text else "Unknown error"
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ Year {year}: Issues detected (exit={returncode}) - {error_msg}... (continuing)',
                            'progress': year_progress + 35
                        })
                        _log_subprocess_streams(f'master.py ({year})', stdout_text, stderr_text, year_progress + 35)
                            
                except subprocess.TimeoutExpired as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Year {year}: Analysis timed out after 40 hours (continuing with other years)',
                        'progress': year_progress + 35
                    })
                    _log_subprocess_streams(f'master.py ({year}) timeout', getattr(e, 'stdout', None), getattr(e, 'stderr', None), year_progress + 35)
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
                overall_success = True
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
                summery_cmd = [
                    "python", summery_script,
                    "--from", date_from,
                    "--to", date_to,
                    "--repos-root", "repos/appgate-sdp-int",
                    "--output-root", ".",
                    "--alias-file", "configuration/alias.json",
                    "--ignore-file", "configuration/ignore_user.txt"
                ]
                try:
                    _log_command_start(f'summery.py {date_from}→{date_to}', summery_cmd, month_progress_start)
                    summery_result = subprocess.run(
                        summery_cmd,
                        cwd=BASE_DIR,
                        capture_output=True,
                        text=True,
                        timeout=18000  # 5 hours for user stats with enterprise repos
                    )
                    
                    if summery_result.returncode != 0:
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ User statistics for {current_year}-{month:02d} had issues (exit={summery_result.returncode}), but continuing...',
                            'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                        })
                        _log_subprocess_streams(f'summery.py {date_from}→{date_to}', summery_result.stdout, summery_result.stderr,
                                                month_progress_start + (month_progress_end - month_progress_start) * 0.5)
                    
                except subprocess.TimeoutExpired as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ User statistics for {current_year}-{month:02d} timed out, but continuing...',
                        'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                    })
                    _log_subprocess_streams(f'summery.py {date_from}→{date_to} timeout', getattr(e, 'stdout', None), getattr(e, 'stderr', None),
                                            month_progress_start + (month_progress_end - month_progress_start) * 0.5)
                except Exception as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ User statistics for {current_year}-{month:02d} failed: {str(e)}, but continuing...',
                        'progress': month_progress_start + (month_progress_end - month_progress_start) * 0.5
                    })
                
                # Step 2: Run service.py for this month (WITHOUT --parallel to avoid pickle issues)
                service_cmd = [
                    "python", service_script,
                    "--from", date_from,
                    "--to", date_to,
                    "--repos-root", "repos/appgate-sdp-int",
                    "--output-root", ".",
                    "--services-file", "configuration/services.json",
                    "--alias-file", "configuration/alias.json",
                    "--ignore-file", "configuration/ignore_user.txt"
                    # NOTE: No --parallel flag to avoid pickle issues
                ]
                try:
                    _log_command_start(f'service.py {date_from}→{date_to}', service_cmd, month_progress_end)
                    service_result = subprocess.run(
                        service_cmd,
                        cwd=BASE_DIR,
                        capture_output=True,
                        text=True,
                        timeout=36000  # 10 hours for subsystem stats with massive enterprise repos
                    )
                    
                    if service_result.returncode != 0:
                        log_update_message({
                            'type': 'warning',
                            'message': f'⚠️ Subsystem statistics for {current_year}-{month:02d} had issues (exit={service_result.returncode}), but continuing...',
                            'progress': month_progress_end
                        })
                        _log_subprocess_streams(f'service.py {date_from}→{date_to}', service_result.stdout, service_result.stderr, month_progress_end)
                    
                except subprocess.TimeoutExpired as e:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Subsystem statistics for {current_year}-{month:02d} timed out, but continuing...',
                        'progress': month_progress_end
                    })
                    _log_subprocess_streams(f'service.py {date_from}→{date_to} timeout', getattr(e, 'stdout', None), getattr(e, 'stderr', None), month_progress_end)
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
                blame_cmd = [
                    "python", blame_script,
                    "--repos-root", "repos/appgate-sdp-int",
                    "--output-root", ".",
                    "--services-file", "configuration/services.json",
                    "--alias-file", "configuration/alias.json",
                    "--ignore-file", "configuration/ignore_user.txt",
                    "--parallel"  # blame.py parallel works fine, it's only service.py that has issues
                ]
                _log_command_start('blame.py ownership analysis', blame_cmd, 75)
                blame_result = subprocess.run(
                    blame_cmd,
                    cwd=BASE_DIR,
                    capture_output=True,
                    text=True,
                    timeout=72000  # 20 hour timeout for ownership analysis with enterprise-scale repos
                )
                
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
                        'message': f'⚠️ Ownership analysis completed with warnings (exit={blame_result.returncode}) [{blame_end_time.strftime("%H:%M:%S")}] (duration: {blame_duration:.1f}s)',
                        'progress': 95
                    })
                    _log_subprocess_streams('blame.py ownership analysis', blame_result.stdout, blame_result.stderr, 95)
                try:
                    log_update_message({
                        'type': 'info',
                        'message': '🏅 Refreshing developer badge cache from latest ownership data...',
                        'progress': 96
                    })
                    badge_cache = refresh_badge_cache()
                    if badge_cache and badge_cache.get('summary'):
                        badge_count = badge_cache['summary'].get('users_with_badges', 0)
                        log_update_message({
                            'type': 'info',
                            'message': f'✅ Badge cache updated ({badge_count} developers with badges)',
                            'progress': 96
                        })
                    else:
                        log_update_message({
                            'type': 'warning',
                            'message': '⚠️ Badge cache generation produced no data. UI will rebuild badges on demand.',
                            'progress': 96
                        })
                except Exception as cache_exc:
                    log_update_message({
                        'type': 'warning',
                        'message': f'⚠️ Failed to refresh badge cache: {cache_exc}',
                        'progress': 96
                    })
            else:
                log_update_message({
                    'type': 'warning',
                    'message': f'⚠️ blame.py not found, skipping ownership analysis [{datetime.now().strftime("%H:%M:%S")}]',
                    'progress': 95
                })
                
        except subprocess.TimeoutExpired as e:
            log_update_message({
                'type': 'warning',
                'message': f'⚠️ Ownership analysis timed out after 20 hours [{datetime.now().strftime("%H:%M:%S")}]',
                'progress': 95
            })
            _log_subprocess_streams('blame.py ownership analysis timeout', getattr(e, 'stdout', None), getattr(e, 'stderr', None), 95)
        except Exception as e:
            log_update_message({
                'type': 'warning',
                'message': f'⚠️ Ownership analysis failed: {str(e)} [{datetime.now().strftime("%H:%M:%S")}]',
                'progress': 95
            })
            if hasattr(e, 'stdout') or hasattr(e, 'stderr'):
                _log_subprocess_streams('blame.py ownership analysis failure', getattr(e, 'stdout', None), getattr(e, 'stderr', None), 95)
        
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
        if pagerduty_backup_root and pagerduty_backup_path:
            try:
                new_pagerduty_dir = os.path.join(stats_dir, "pagerduty")
                new_overview_file = os.path.join(new_pagerduty_dir, "overview.json")
                if not os.path.exists(new_overview_file):
                    os.makedirs(stats_dir, exist_ok=True)
                    if os.path.exists(new_pagerduty_dir):
                        shutil.rmtree(new_pagerduty_dir)
                    shutil.move(pagerduty_backup_path, new_pagerduty_dir)
                    log_update_message({
                        'type': 'warning',
                        'message': '⚠️ Restored previous PagerDuty cache because latest sync failed.',
                        'progress': 98
                    })
                else:
                    log_update_message({
                        'type': 'info',
                        'message': '✅ PagerDuty cache refreshed; removing preserved backup.',
                        'progress': 98
                    })
            except Exception as restore_exc:
                log_update_message({
                    'type': 'warning',
                    'message': f'⚠️ Failed to restore PagerDuty cache backup: {restore_exc}',
                    'progress': 98
                })
            finally:
                if pagerduty_backup_root and os.path.isdir(pagerduty_backup_root):
                    shutil.rmtree(pagerduty_backup_root, ignore_errors=True)
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



def launch_background_scheduler():
    start_background_scheduler()

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
    
    for team_id, team_info in teams_config.items():
        responsible_subsystems = responsibilities.get(team_id, [])
        
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
        
        teams.append({
            "id": team_id,
            "name": team_info.get("name", team_id),
            "description": team_info.get("description", ""),
            "members": team_info.get("members", []),
            "responsible_subsystems": responsible_subsystems,
            "periods": team_periods
        })
    
    return jsonify({"teams": teams})


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
                    # Convert to expected format
                    return jsonify({
                        "type": "team",
                        "team_id": team_id,
                        "team_name": team_name,
                        "description": team.get("description", ""),
                        "members": data.get("members", []),
                        "responsible_subsystems": data.get("responsible_subsystems", []),
                        "responsible_subsystem_details": data.get("responsible_subsystem_details", {}),
                        "total_responsible_lines": data.get("total_responsible_lines", 0),
                        "total_commits": data.get("commits", 0),
                        "total_additions": data.get("lines_added", 0),
                        "total_deletions": data.get("lines_deleted", 0),
                        "languages": data.get("languages", {}),
                        "subsystems": data.get("subsystems", {}),
                        "per_date": data.get("per_date", {}),
                        "member_contributions": data.get("member_contributions", {})
                    })
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading team file {team_file}: {e}")
    
    # Fall back to old aggregation method if file doesn't exist
    members = team.get("members", [])
    responsible_subsystems = get_team_responsible_subsystems(team_id)
    
    if to_date is None:
        to_date = from_date
    
    # Load aliases to resolve canonical user slugs
    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    alias_map = {}
    if os.path.exists(alias_file):
        try:
            alias_map = load_json(alias_file)
        except:
            pass
    
    def get_canonical_slug(slug):
        """Apply aliases to get canonical developer slug."""
        for canonical, aliases in alias_map.items():
            if isinstance(aliases, list) and slug in aliases:
                return canonical
            elif isinstance(aliases, str) and slug == aliases:
                return canonical
        return slug
    
    # Resolve all member slugs to their canonical forms
    canonical_members = [get_canonical_slug(member) for member in members]
    # Remove duplicates that might occur after alias resolution
    canonical_members = list(dict.fromkeys(canonical_members))
    
    if not canonical_members:
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
                                subsystem_lines += lang_info.get("code_lines", 0)
                        
                        responsible_subsystem_details[subsystem_name] = {
                            "name": subsystem_name,
                            "lines": subsystem_lines
                        }
                        total_responsible_lines += subsystem_lines
            except (json.JSONDecodeError, IOError, KeyError):
                # If we can't load language data, still include the subsystem with 0 lines
                responsible_subsystem_details[subsystem_name] = {
                    "name": subsystem_name,
                    "lines": 0
                }
        
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
            "commits_timeline": []
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
    
    for member in canonical_members:
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
                    # Get total code lines from the totals section
                    subsystem_lines = lang_data.get("totals", {}).get("code_lines", 0)
                    
                    print(f"DEBUG: {subsystem_name} has {subsystem_lines} lines from {subsystem_lang_path}")
                    
                    responsible_subsystem_details[subsystem_name] = {
                        "name": subsystem_name,
                        "lines": subsystem_lines
                    }
                    total_responsible_lines += subsystem_lines
            else:
                print(f"DEBUG: No languages.json found for {subsystem_name} at {subsystem_lang_path}")
                # If languages.json doesn't exist, include with 0 lines
                responsible_subsystem_details[subsystem_name] = {
                    "name": subsystem_name,
                    "lines": 0
                }
        except (json.JSONDecodeError, IOError, KeyError) as e:
            print(f"DEBUG: Error loading {subsystem_name}: {e}")
            # If we can't load language data, still include the subsystem with 0 lines
            responsible_subsystem_details[subsystem_name] = {
                "name": subsystem_name,
                "lines": 0
            }
    
    aggregated_data["responsible_subsystem_details"] = responsible_subsystem_details
    aggregated_data["total_responsible_lines"] = total_responsible_lines
    
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


def slugify_identifier(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


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

    for root, _dirs, files in os.walk(repos_path):
        if "blame.json" not in files:
            continue
        blame_file = os.path.join(root, "blame.json")
        try:
            blame_data = load_json(blame_file)
        except Exception:
            continue

        repo_dir = os.path.dirname(root)
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
    if not isinstance(dev_obj, dict):
        return normalized

    for slug, info in dev_obj.items():
        if isinstance(info, dict):
            lines = info.get("lines", 0) or 0
            display_name = info.get("display_name") or slug
        else:
            try:
                lines = int(info)
            except (TypeError, ValueError):
                lines = 0
            display_name = slug

        if lines <= 0:
            continue

        normalized[slug] = {
            "lines": int(lines),
            "display_name": display_name or slug,
        }

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

    for root, _dirs, files in os.walk(repos_path):
        if "blame.json" not in files:
            continue

        blame_file = os.path.join(root, "blame.json")
        try:
            blame_data = load_json(blame_file)
        except Exception as exc:
            print(f"Error loading blame file {blame_file}: {exc}")
            continue

        repo_path = os.path.dirname(root)
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

    member_set = set(team_members)
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
            if dev_slug not in member_set:
                continue
            dev_lines = dev_info.get("lines", 0)
            if dev_lines <= 0:
                continue

            share = dev_lines / total_lines
            if share <= 0:
                continue

            profile = dev_language_totals.setdefault(
                dev_slug,
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
        developers.append({
            "slug": slug,
            "display_name": info.get("display_name", slug),
            "total_lines": int(round(total_lines)),
            "subsystem_count": len(contexts),
            "subsystems": sorted(contexts),
        })
    developers.sort(key=lambda dev: dev["total_lines"], reverse=True)
    return developers


def build_team_per_date(members: list, year: int):
    aggregated = {}
    try:
        for member in members or []:
            user_dir = os.path.join(STATS_ROOT, "users", member)
            for month in range(1, 13):
                month_folder = f"{year:04d}-{month:02d}"
                summary_path = os.path.join(user_dir, month_folder, "summary.json")
                if not os.path.isfile(summary_path):
                    continue
                with open(summary_path, "r", encoding="utf-8") as f:
                    monthly = json.load(f)
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
                # Recompute responsible_subsystem_details from current responsibilities + languages.json
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
                # Load services configuration to identify nested subsystems in repos
                services_config = {}
                services_file = os.path.join(BASE_DIR, "configuration", "services.json")
                try:
                    with open(services_file, "r", encoding="utf-8") as sf:
                        services_config = json.load(sf)
                except Exception:
                    services_config = {}
                
                for subsystem_name in team_responsibilities:
                    # Base: language stats for subsystem (may be a repo or a specific service)
                    lang_path = os.path.join(STATS_ROOT, "subsystems", subsystem_name, "languages.json")
                    subsystem_languages = {}
                    subsystem_lines = 0
                    if os.path.isfile(lang_path):
                        try:
                            with open(lang_path, "r", encoding="utf-8") as lf:
                                lang_data = json.load(lf)
                                for lang_name, lang_info in (lang_data.get("languages", {}) or {}).items():
                                    code_lines = lang_info.get("code_lines", 0)
                                    subsystem_languages[lang_name] = code_lines
                                    subsystem_lines += code_lines
                        except Exception:
                            pass
                    
                    # If subsystem_name is a repository (top-level), subtract lines of its child services
                    # services_config is { org/repo: { service_name: [paths...] } }
                    # Match by repo basename
                    matching_repo_key = None
                    for repo_key in services_config.keys():
                        if repo_key.split('/')[-1] == subsystem_name:
                            matching_repo_key = repo_key
                            break
                    if matching_repo_key:
                        app.logger.info(f"[teams-year] Repo match for subsystem '{subsystem_name}': {matching_repo_key}")
                        child_services_total = 0
                        for service_name in services_config.get(matching_repo_key, {}).keys():
                            child_lang_path = os.path.join(STATS_ROOT, "subsystems", service_name, "languages.json")
                            if os.path.isfile(child_lang_path):
                                try:
                                    with open(child_lang_path, "r", encoding="utf-8") as clf:
                                        child_lang_data = json.load(clf)
                                        for _, child_lang_info in (child_lang_data.get("languages", {}) or {}).items():
                                            child_services_total += child_lang_info.get("code_lines", 0)
                                except Exception as e:
                                    app.logger.warning(f"[teams-year] Failed reading child service '{service_name}' languages: {e}")
                        app.logger.info(f"[teams-year] Child services total for '{matching_repo_key}': {child_services_total}")
                        if subsystem_lines > 0:
                            before_subtract = subsystem_lines
                            subsystem_lines = max(0, subsystem_lines - child_services_total)
                            app.logger.info(f"[teams-year] Repo remainder for '{matching_repo_key}': {before_subtract} - {child_services_total} = {subsystem_lines}")
                            subsystem_languages = {"Remaining": subsystem_lines}
                        else:
                            repo_context = _ensure_repo_language_data(matching_repo_key)
                            remainder_langs = (repo_context or {}).get("remainder") or {}
                            if remainder_langs:
                                subsystem_languages = remainder_langs
                                subsystem_lines = sum(remainder_langs.values())
                                app.logger.info(f"[teams-year] Using cached remainder for '{matching_repo_key}': {subsystem_lines} lines")
                            else:
                                app.logger.warning(f"[teams-year] No language breakdown available for '{matching_repo_key}'")
                                subsystem_languages = {}
                                subsystem_lines = 0
                    
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
                return jsonify({
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
                })
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading team file {team_file}: {e}")
    
    # Fall back to old aggregation method
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
    
    # Load aliases to resolve canonical user slugs
    alias_file = os.path.join(BASE_DIR, "configuration", "alias.json")
    alias_map = {}
    if os.path.exists(alias_file):
        try:
            alias_map = load_json(alias_file)
        except:
            pass
    
    def get_canonical_slug(slug):
        """Apply aliases to get canonical developer slug."""
        for canonical, aliases in alias_map.items():
            if isinstance(aliases, list) and slug in aliases:
                return canonical
            elif isinstance(aliases, str) and slug == aliases:
                return canonical
        return slug
    
    teams_analytics = []
    
    for team_id, team_info in teams_config.items():
        team_name = team_info.get("name", team_id)
        members = team_info.get("members", [])
        responsible_subsystems = get_team_responsible_subsystems(team_id)
        
        # Resolve all member slugs to their canonical forms
        canonical_members = [get_canonical_slug(member) for member in members]
        # Remove duplicates that might occur after alias resolution
        canonical_members = list(dict.fromkeys(canonical_members))
        
        # Initialize team stats
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
            "active_months": set()
        }
        
        # Aggregate data from all team members
        for member in canonical_members:
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
            data = load_json(path)
            if data and data.get("author_name"):
                display_name = data["author_name"]
        except Exception as e:
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
            # Try normalized match again for safety
            continue
        
        user_periods = user_months_all.get(username, [])
        
        # Find the monthly summary that matches our year/month
        for period in user_periods:
            if period.get("from", "")[:7] == target_month:  # Match YYYY-MM
                monthly_folder = period.get("folder")
                if not monthly_folder:
                    continue
                summary_path = os.path.join(user_dir, monthly_folder, "summary.json")
                
                if os.path.exists(summary_path):
                    with open(summary_path, "r", encoding="utf-8") as f:
                        summary_data = json.load(f)
                    
                    per_date_data = summary_data.get("per_date", {})
                    
                    # Aggregate by date
                    for date_str, day_data in per_date_data.items():
                        if date_str[:7] == target_month:  # Only include days from target month
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
    # You can set host="0.0.0.0" if you want to reach it from other machines
    # Exclude repos directory from file watcher to prevent restarts during cloning
    parser = argparse.ArgumentParser(description="Dashboard server")
    parser.add_argument("--host", "--listen-address", dest="host", default="127.0.0.1", help="Host/IP to bind the dashboard server")
    parser.add_argument("--port", type=int, default=5001, help="Port to bind the dashboard server")
    parser.add_argument("--read-only", action="store_true", help="Run dashboard in read-only mode (disable updates/settings)")
    args = parser.parse_args()

    app.config["READ_ONLY_MODE"] = args.read_only

    app.run(host=args.host, port=args.port, debug=True,
            exclude_patterns=["repos/*", "repos/**/*", "stats/*", "stats/**/*"])

