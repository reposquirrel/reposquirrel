"""PagerDuty ingestion helpers for repo-squirrel."""
from __future__ import annotations

import json
import logging
import math
import os
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

try:  # pragma: no cover - optional dependency
    import requests
except ImportError:  # pragma: no cover - handled gracefully at runtime
    requests = None  # type: ignore

INCIDENTS_URL = "https://api.pagerduty.com/incidents"
INCIDENT_LOG_ENTRIES_URL = "https://api.pagerduty.com/incidents/{incident_id}/log_entries"
USERS_URL = "https://api.pagerduty.com/users"
HEADERS_BASE = {
    "Accept": "application/vnd.pagerduty+json;version=2",
    "Content-Type": "application/json",
}
DEFAULT_LOOKBACK_DAYS = 365
LOGGER = logging.getLogger(__name__)


def iso_utc(dt: datetime) -> str:
    """Return an ISO-8601 string with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso8601(ts: Optional[str]) -> Optional[datetime]:
    """Parse PagerDuty timestamps to aware UTC datetimes."""
    if not ts:
        return None
    ts = ts.strip()
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except ValueError:
        return None


def _load_integrations(base_dir: str) -> Dict[str, Any]:
    path = os.path.join(base_dir, "configuration", "integrations.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:  # pragma: no cover - config errors
        LOGGER.info("Warning: unable to read integrations configuration: %s", exc)
        return {}


def _load_pagerduty_token(base_dir: str) -> Optional[str]:
    env_token = os.environ.get("PAGERDUTY_API_TOKEN")
    if env_token:
        return env_token.strip()
    config = _load_integrations(base_dir)
    token = (config.get("pagerduty") or {}).get("api_token")
    if isinstance(token, str) and token.strip():
        return token.strip()
    return None


def _load_existing_incident_events(path: str) -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            existing = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    cache: Dict[str, Dict[str, Any]] = {}
    for entry in existing:
        incident_id = entry.get("id") or entry.get("incident_number")
        if incident_id is None:
            continue
        cache[str(incident_id)] = {
            "updated_at": entry.get("updated_at"),
            "responder_events": entry.get("responder_events"),
        }
    return cache


class _SimpleResponse:
    def __init__(self, status_code: int, headers: Dict[str, str], text: str):
        self.status_code = status_code
        self.headers = headers
        self.text = text

    def json(self) -> Any:
        if not self.text:
            return {}
        return json.loads(self.text)


def _build_query_string(params: Optional[Dict[str, Any]]) -> str:
    if not params:
        return ""
    return urllib_parse.urlencode(params, doseq=True)


def _raise_for_status(resp: Any) -> None:
    body_preview = ""
    try:
        data = resp.json()
        if isinstance(data, str):
            body_preview = data
        else:
            body_preview = json.dumps(data)[:200]
    except Exception:
        body_preview = (getattr(resp, "text", "") or "")[:200]
    raise RuntimeError(f"HTTP {resp.status_code}: {body_preview}")


def _perform_http_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]]) -> _SimpleResponse:
    if requests is not None:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        return resp  # type: ignore[return-value]

    full_url = url
    query = _build_query_string(params)
    if query:
        separator = "&" if "?" in url else "?"
        full_url = f"{url}{separator}{query}"
    req = urllib_request.Request(full_url, headers=headers, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=60) as handle:  # type: ignore[arg-type]
            text = handle.read().decode(handle.headers.get_content_charset("utf-8"), errors="replace")
            header_map = {k: v for k, v in handle.headers.items()}
            return _SimpleResponse(getattr(handle, "status", 200), header_map, text)
    except urllib_error.HTTPError as exc:  # pragma: no cover - network errors
        body = exc.read().decode(exc.headers.get_content_charset("utf-8"), errors="replace") if exc.fp else ""
        header_map = {k: v for k, v in (exc.headers or {}).items()}
        return _SimpleResponse(exc.code, header_map, body)
    except urllib_error.URLError as exc:  # pragma: no cover - network errors
        raise RuntimeError(f"Network error contacting PagerDuty: {exc}") from exc


def _get_with_retry(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 7,
) -> Any:
    """HTTP GET helper with polite retry handling."""
    attempt = 0
    backoff = 1.0
    while True:
        resp = _perform_http_get(url, headers, params)

        if resp.status_code == 429:
            attempt += 1
            retry_after = None
            header_value = resp.headers.get("Retry-After") if resp.headers else None
            if header_value:
                try:
                    retry_after = float(header_value)
                except (TypeError, ValueError):
                    retry_after = None
            wait_time = retry_after if retry_after is not None else backoff
            backoff = min(backoff * 2, 30.0)
            time.sleep(wait_time + random.uniform(0, 0.25))
            if attempt > max_retries:
                _raise_for_status(resp)
            continue

        if resp.status_code >= 400:
            _raise_for_status(resp)
        return resp


def _fetch_incident_log_entries(token: str, incident_id: str) -> List[Dict[str, Any]]:
    headers = dict(HEADERS_BASE)
    headers["Authorization"] = f"Token token={token}"
    url = INCIDENT_LOG_ENTRIES_URL.format(incident_id=urllib_parse.quote(str(incident_id)))
    params: Dict[str, Any] = {"limit": 100, "offset": 0, "time_zone": "UTC"}
    entries: List[Dict[str, Any]] = []
    while True:
        response = _get_with_retry(url, headers, params)
        payload = response.json()
        entries.extend(payload.get("log_entries") or [])
        if not payload.get("more"):
            break
        params["offset"] = params.get("offset", 0) + params.get("limit", 100)
    return entries


def _append_responder_event(
    events: List[Dict[str, Any]],
    user_ref: Optional[Dict[str, Any]],
    role: str,
    timestamp: Optional[str],
) -> None:
    if not timestamp:
        return
    user_id = _extract_user_id(user_ref)
    if not user_id:
        return
    event: Dict[str, Any] = {"user_id": user_id, "role": role, "at": timestamp}
    if isinstance(user_ref, dict):
        summary = user_ref.get("summary") or user_ref.get("name")
        if isinstance(summary, str) and summary.strip():
            event["user_summary"] = summary.strip()
        html_url = user_ref.get("html_url")
        if isinstance(html_url, str) and html_url.strip():
            event["user_html_url"] = html_url.strip()
    events.append(event)


def _extract_responder_events_from_logs(log_entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not log_entries:
        return []
    events: List[Dict[str, Any]] = []
    for entry in log_entries:
        entry_type = (entry.get("type") or "").lower()
        timestamp = entry.get("created_at") or entry.get("timestamp") or entry.get("occurred_at")
        if "acknowledge" in entry_type:
            _append_responder_event(events, entry.get("agent"), "acknowledged", timestamp)
        elif "resolve" in entry_type:
            _append_responder_event(events, entry.get("agent"), "resolved", timestamp)
        elif "assign" in entry_type or "delegate" in entry_type:
            assignments = entry.get("assignments") or []
            if assignments:
                for assignment in assignments:
                    _append_responder_event(events, assignment.get("assignee"), "assigned", timestamp)
            else:
                _append_responder_event(events, entry.get("agent"), "assigned", timestamp)
    events.sort(key=lambda item: item.get("at") or "")
    return events


def _augment_incidents_with_logs(
    token: str,
    incidents: Sequence[Dict[str, Any]],
    cache: Dict[str, Dict[str, Any]],
    logger: logging.Logger,
) -> None:
    if not incidents:
        return
    remaining: List[Dict[str, Any]] = []
    for incident in incidents:
        incident_id = incident.get("id") or incident.get("incident_number")
        if incident_id is None:
            continue
        key = str(incident_id)
        cached = cache.get(key)
        if cached and cached.get("updated_at") == incident.get("updated_at") and "responder_events" in cached:
            incident["responder_events"] = cached.get("responder_events") or []
            continue
        remaining.append(incident)
    if not remaining:
        return
    logger.info("Fetching PagerDuty log entries for %s incidents", len(remaining))
    for idx, incident in enumerate(remaining, start=1):
        incident_id = incident.get("id") or incident.get("incident_number")
        if not incident_id:
            continue
        try:
            logs = _fetch_incident_log_entries(token, str(incident_id))
        except Exception as exc:  # pragma: no cover - network errors handled upstream
            logger.info("Warning: failed to fetch log entries for incident %s: %s", incident_id, exc)
            continue
        incident["responder_events"] = _extract_responder_events_from_logs(logs)
        if idx % 25 == 0:
            logger.info("Processed %s/%s incidents for log entries", idx, len(remaining))


def _month_range_iter(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    ranges: List[Tuple[datetime, datetime]] = []
    cursor = start
    while cursor <= end:
        first_of_month = cursor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if first_of_month.month == 12:
            next_month_start = first_of_month.replace(year=first_of_month.year + 1, month=1)
        else:
            next_month_start = first_of_month.replace(month=first_of_month.month + 1)
        window_end = min(end, next_month_start - timedelta(seconds=1))
        ranges.append((cursor, window_end))
        cursor = window_end + timedelta(seconds=1)
    return ranges


def _fetch_incidents(
    token: str,
    since: datetime,
    until: datetime,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch incidents between two dates using classic pagination."""
    headers = dict(HEADERS_BASE)
    headers["Authorization"] = f"Token token={token}"

    params: Dict[str, Any] = {"limit": 100, "since": iso_utc(since), "until": iso_utc(until)}
    if extra_params:
        params.update(extra_params)

    all_items: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params["offset"] = offset
        response = _get_with_retry(INCIDENTS_URL, headers, params=params)
        payload = response.json()
        items = payload.get("incidents", [])
        all_items.extend(items)
        if not payload.get("more"):
            break
        offset += int(params["limit"])
    deduped: Dict[str, Dict[str, Any]] = {}
    for incident in all_items:
        incident_id = incident.get("id")
        if incident_id and incident_id in deduped:
            continue
        if incident_id:
            deduped[incident_id] = incident
        else:
            deduped[str(len(deduped))] = incident
    return list(deduped.values())


def _fetch_pagerduty_users(token: str) -> List[Dict[str, Any]]:
    headers = dict(HEADERS_BASE)
    headers["Authorization"] = f"Token token={token}"
    params: Dict[str, Any] = {"limit": 100, "offset": 0}
    users: List[Dict[str, Any]] = []
    while True:
        response = _get_with_retry(USERS_URL, headers, params=params)
        payload = response.json()
        batch = payload.get("users") or []
        if isinstance(batch, list):
            users.extend(batch)
        if not payload.get("more"):
            break
        params["offset"] = params.get("offset", 0) + params.get("limit", 100)
    return users


def _find_latest_user_summary(user_dir: str) -> Optional[str]:
    year_dir = os.path.join(user_dir, "year")
    if os.path.isdir(year_dir):
        for filename in sorted(os.listdir(year_dir), reverse=True):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(year_dir, filename)
            if os.path.isfile(path):
                return path
    for entry in sorted(os.listdir(user_dir), reverse=True):
        if entry == "year":
            continue
        summary_path = os.path.join(user_dir, entry, "summary.json")
        if os.path.isfile(summary_path):
            return summary_path
    return None


def _build_github_user_lookup(stats_root: str) -> Dict[str, List[Dict[str, str]]]:
    users_root = os.path.join(stats_root, "users")
    lookup: Dict[str, List[Dict[str, str]]] = {}
    if not os.path.isdir(users_root):
        return lookup
    for slug in sorted(os.listdir(users_root)):
        user_dir = os.path.join(users_root, slug)
        if not os.path.isdir(user_dir):
            continue
        summary_path = _find_latest_user_summary(user_dir)
        if not summary_path:
            continue
        try:
            with open(summary_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        email_value: Optional[str] = None
        raw_email = data.get("author_email")
        if isinstance(raw_email, str) and raw_email.strip():
            email_value = raw_email.strip()
        elif isinstance(data.get("author_emails"), list):
            for raw in data["author_emails"]:
                if isinstance(raw, str) and raw.strip():
                    email_value = raw.strip()
                    break
        if not email_value:
            continue
        key = email_value.lower()
        entry = {
            "slug": slug,
            "display_name": data.get("author_name") or slug,
            "email": email_value,
        }
        lookup.setdefault(key, []).append(entry)
    return lookup


def _extract_user_id(ref: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(ref, dict):
        return None
    user_id = ref.get("id")
    if not user_id:
        return None
    return str(user_id)


def _build_responder_leaderboard(
    incidents: Sequence[Dict[str, Any]],
    pagerduty_users: Sequence[Dict[str, Any]],
    github_lookup: Dict[str, List[Dict[str, str]]],
) -> Optional[Dict[str, Any]]:
    if not incidents:
        return None
    pd_by_id = {str(user.get("id")): user for user in pagerduty_users if user.get("id")}
    stats: Dict[str, Dict[str, Any]] = {}

    def _ensure(user_id: str) -> Dict[str, Any]:
        entry = stats.get(user_id)
        if entry is None:
            entry = {
                "resolved": set(),
                "ack": set(),
                "assigned": set(),
                "durations": [],
                "pd_name": None,
                "pd_html": None,
            }
            stats[user_id] = entry
        return entry

    def _record(user_id: str, ref: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        entry = _ensure(user_id)
        if isinstance(ref, dict):
            display_name = ref.get("summary") or ref.get("name")
            if display_name and not entry.get("pd_name"):
                entry["pd_name"] = display_name
            html_url = ref.get("html_url")
            if html_url and not entry.get("pd_html"):
                entry["pd_html"] = html_url
        return entry

    for incident in incidents:
        incident_id = str(incident.get("id") or incident.get("incident_number") or len(stats))
        status = (incident.get("status") or "").lower()
        created = parse_iso8601(incident.get("created_at"))
        resolved_dt = parse_iso8601(incident.get("resolved_at")) or parse_iso8601(
            incident.get("last_status_change_at")
        )
        duration_minutes = None
        if created and resolved_dt:
            duration_minutes = (resolved_dt - created).total_seconds() / 60.0

        events_handled = False
        responder_events = incident.get("responder_events")
        if isinstance(responder_events, list) and responder_events:
            events_handled = True
            for event in responder_events:
                user_id = event.get("user_id")
                if not user_id:
                    continue
                role = (event.get("role") or "").lower()
                entry = _record(user_id, None)
                if role == "resolved":
                    if incident_id not in entry["resolved"]:
                        entry["resolved"].add(incident_id)
                        if duration_minutes is not None:
                            entry["durations"].append(duration_minutes)
                elif role == "acknowledged":
                    entry["ack"].add(incident_id)
                elif role == "assigned":
                    entry["assigned"].add(incident_id)
        if events_handled:
            continue

        if status == "resolved":
            resolver_ref = incident.get("last_status_change_by")
            resolver_id = _extract_user_id(resolver_ref)
            if resolver_id:
                entry = _record(resolver_id, resolver_ref)
                entry["resolved"].add(incident_id)
                if duration_minutes is not None:
                    entry["durations"].append(duration_minutes)
        for acknowledgement in incident.get("acknowledgements") or []:
            ack_ref = acknowledgement.get("acknowledger")
            ack_id = _extract_user_id(ack_ref)
            if ack_id:
                entry = _record(ack_id, ack_ref)
                entry["ack"].add(incident_id)
        for assignment in incident.get("assignments") or []:
            assignee_ref = assignment.get("assignee")
            assignee_id = _extract_user_id(assignee_ref)
            if assignee_id:
                entry = _record(assignee_id, assignee_ref)
                entry["assigned"].add(incident_id)

    entries: List[Dict[str, Any]] = []
    for user_id, bucket in stats.items():
        resolved_count = len(bucket["resolved"])
        if resolved_count == 0:
            continue
        ack_count = len(bucket["ack"])
        assignment_count = len(bucket["assigned"])
        touch_count = len(bucket["resolved"] | bucket["ack"] | bucket["assigned"])
        durations = bucket["durations"]
        avg_minutes = (sum(durations) / len(durations)) if durations else None
        median_minutes = _percentile(durations, 0.5) if durations else None
        fastest_minutes = min(durations) if durations else None
        slowest_minutes = max(durations) if durations else None
        pd_info = pd_by_id.get(user_id) or {}
        email = (pd_info.get("email") or "").strip()
        email_lower = email.lower() if email else None
        entry = {
            "pagerduty_user_id": user_id,
            "pagerduty_name": pd_info.get("name") or pd_info.get("summary") or bucket.get("pd_name"),
            "pagerduty_email": email or None,
            "pagerduty_html_url": pd_info.get("html_url") or bucket.get("pd_html"),
            "resolved_count": resolved_count,
            "acknowledged_count": ack_count,
            "assignment_count": assignment_count,
            "touch_count": touch_count,
            "avg_resolution_minutes": avg_minutes,
            "median_resolution_minutes": median_minutes,
            "fastest_resolution_minutes": fastest_minutes,
            "slowest_resolution_minutes": slowest_minutes,
            "github_user": None,
            "github_match_count": 0,
        }
        matches = github_lookup.get(email_lower) if email_lower else None
        if matches:
            entry["github_match_count"] = len(matches)
            entry["github_user"] = matches[0].copy()
        entries.append(entry)

    if not entries:
        return None

    entries.sort(
        key=lambda item: (
            -item["resolved_count"],
            -item["acknowledged_count"],
            item["avg_resolution_minutes"] if item["avg_resolution_minutes"] is not None else float("inf"),
        )
    )
    total = len(entries)
    matched = sum(1 for item in entries if item.get("github_user"))
    return {
        "total_responders": total,
        "matched_responders": matched,
        "entries": entries[:25],
    }


def _incident_windows(
    incidents: Sequence[Dict[str, Any]]
) -> List[Tuple[datetime, Optional[datetime]]]:
    windows: List[Tuple[datetime, Optional[datetime]]] = []
    for incident in incidents:
        created = parse_iso8601(incident.get("created_at"))
        if not created:
            continue
        resolved: Optional[datetime] = None
        status = (incident.get("status") or "").lower()
        if status == "resolved":
            resolved = (
                parse_iso8601(incident.get("resolved_at"))
                or parse_iso8601(incident.get("last_status_change_at"))
            )
        windows.append((created, resolved))
    return windows


def _daterange(start: datetime, end: datetime) -> List[datetime]:
    dates: List[datetime] = []
    cursor = start
    while cursor <= end:
        dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def _week_start(dt: datetime) -> datetime:
    monday = dt - timedelta(days=dt.weekday())
    return datetime.combine(monday.date(), datetime.min.time(), tzinfo=timezone.utc)


def _weekrange(start: datetime, end: datetime) -> List[datetime]:
    weeks: List[datetime] = []
    cursor = start
    while cursor <= end:
        weeks.append(cursor)
        cursor += timedelta(days=7)
    return weeks


def _build_daily_counts(
    windows: Sequence[Tuple[datetime, Optional[datetime]]]
) -> List[Tuple[datetime, int]]:
    if not windows:
        return []
    start_day = min(win[0].date() for win in windows)
    latest_resolved = [win[1] for win in windows if win[1]]
    end_day = max(
        [max(win[0].date() for win in windows)]
        + ([max(dt.date() for dt in latest_resolved)] if latest_resolved else [])
    )
    day_start = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
    counts: List[Tuple[datetime, int]] = []
    for day in _daterange(day_start, day_end):
        window_open = day
        window_close = day + timedelta(days=1)
        open_count = sum(
            1
            for opened, resolved in windows
            if opened < window_close and (resolved is None or resolved >= window_open)
        )
        counts.append((day, open_count))
    return counts


def _build_daily_open_closed_counts(
    windows: Sequence[Tuple[datetime, Optional[datetime]]]
) -> List[Tuple[datetime, int, int]]:
    if not windows:
        return []
    opened_counter = Counter(win[0].date() for win in windows)
    closed_counter = Counter(win[1].date() for win in windows if win[1])
    start_day = min(opened_counter)
    if closed_counter:
        combined_keys = set(opened_counter) | set(closed_counter)
        end_day = max(combined_keys)
    else:
        end_day = max(opened_counter)
    day_start = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
    counts: List[Tuple[datetime, int, int]] = []
    for day in _daterange(day_start, day_end):
        key = day.date()
        counts.append((day, opened_counter.get(key, 0), closed_counter.get(key, 0)))
    return counts


def _build_weekly_open_closed_counts(
    windows: Sequence[Tuple[datetime, Optional[datetime]]]
) -> List[Tuple[datetime, int, int]]:
    if not windows:
        return []
    opened_counter = Counter(_week_start(win[0]) for win in windows)
    closed_counter = Counter(_week_start(win[1]) for win in windows if win[1])
    start_week = min(opened_counter)
    if closed_counter:
        combined_keys = set(opened_counter) | set(closed_counter)
        end_week = max(combined_keys)
    else:
        end_week = max(opened_counter)
    counts: List[Tuple[datetime, int, int]] = []
    for week in _weekrange(start_week, end_week):
        counts.append((week, opened_counter.get(week, 0), closed_counter.get(week, 0)))
    return counts


def _normalize_severity_label(value: Optional[str]) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    if not normalized:
        return "unknown"
    aliases = {
        "sev1": "critical",
        "sev2": "high",
        "sev3": "medium",
        "sev4": "low",
        "sev5": "info",
        "p1": "critical",
        "p2": "high",
        "p3": "medium",
        "p4": "low",
        "p5": "info",
        "informational": "info",
        "information": "info",
        "warn": "medium",
        "warning": "medium",
        "pdown": "p-down",
    }
    return aliases.get(normalized, normalized)


def _build_weekly_severity_timeline(
    incidents: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not incidents:
        return []
    buckets: Dict[datetime, Counter[str]] = {}
    for incident in incidents:
        timestamp = (
            parse_iso8601(incident.get("created_at"))
            or parse_iso8601(incident.get("last_status_change_at"))
            or parse_iso8601(incident.get("updated_at"))
            or parse_iso8601(incident.get("resolved_at"))
        )
        if not timestamp:
            continue
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        week_start = _week_start(timestamp)
        severity = _normalize_severity_label(_extract_severity(incident))
        bucket = buckets.setdefault(week_start, Counter())
        bucket[severity] += 1
    timeline: List[Dict[str, Any]] = []
    for week in sorted(buckets):
        timeline.append(
            {
                "week_start": week.strftime("%Y-%m-%d"),
                "severities": dict(buckets[week]),
            }
        )
    return timeline


def _serialize_daily_counts(counts: Sequence[Tuple[datetime, int]]) -> List[Dict[str, Any]]:
    return [
        {"date": day.strftime("%Y-%m-%d"), "open": value}
        for day, value in counts
    ]


def _serialize_open_closed_counts(
    counts: Sequence[Tuple[datetime, int, int]],
    key: str,
) -> List[Dict[str, Any]]:
    return [
        {key: day.strftime("%Y-%m-%d"), "opened": opened, "closed": closed}
        for day, opened, closed in counts
    ]


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    k = (len(ordered) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    return ordered[f] * (c - k) + ordered[c] * (k - f)


def _extract_severity(incident: Dict[str, Any]) -> str:
    priority = incident.get("priority") or {}
    for key in ("summary", "name"):
        value = priority.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    urgency = incident.get("urgency")
    if isinstance(urgency, str) and urgency.strip():
        return urgency.strip().title()
    return "Unprioritized"


def _summarize_incident(
    incident: Dict[str, Any],
    now: datetime,
) -> Dict[str, Any]:
    created = parse_iso8601(incident.get("created_at"))
    resolved = parse_iso8601(incident.get("resolved_at")) or parse_iso8601(
        incident.get("last_status_change_at")
    )
    duration_minutes: Optional[float] = None
    if created:
        end_time = resolved or now
        duration_minutes = (end_time - created).total_seconds() / 60.0
    service = (incident.get("service") or {}).get("summary") or "Unassigned"
    teams = [team.get("summary") for team in (incident.get("teams") or []) if team.get("summary")]
    return {
        "id": incident.get("id"),
        "number": incident.get("incident_number"),
        "title": incident.get("title") or incident.get("summary"),
        "status": incident.get("status"),
        "service": service,
        "severity": _extract_severity(incident),
        "urgency": incident.get("urgency"),
        "teams": teams,
        "created_at": incident.get("created_at"),
        "resolved_at": incident.get("resolved_at") or incident.get("last_status_change_at"),
        "html_url": incident.get("html_url"),
        "duration_minutes": duration_minutes,
    }


def _summarize_incidents(
    incidents: Sequence[Dict[str, Any]],
    since: datetime,
    until: datetime,
    lookback_days: int,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    status_counts = Counter((incident.get("status") or "unknown").lower() for incident in incidents)
    service_stats: Dict[str, Dict[str, int]] = {}
    team_stats: Counter[str] = Counter()
    severity_stats: Counter[str] = Counter()
    urgency_stats: Counter[str] = Counter()
    resolution_minutes: List[float] = []
    resolved_within_24h = 0
    for incident in incidents:
        status = (incident.get("status") or "unknown").lower()
        service = (incident.get("service") or {}).get("summary") or "Unassigned"
        service_entry = service_stats.setdefault(service, {"total": 0, "open": 0, "resolved": 0})
        service_entry["total"] += 1
        if status == "resolved":
            service_entry["resolved"] += 1
        else:
            service_entry["open"] += 1
        severity = _extract_severity(incident)
        severity_stats[severity] += 1
        urgency_value = incident.get("urgency") or "unknown"
        urgency_stats[str(urgency_value).title()] += 1
        for team in incident.get("teams") or []:
            label = team.get("summary") or team.get("name")
            if label:
                team_stats[label] += 1
        created = parse_iso8601(incident.get("created_at"))
        resolved = parse_iso8601(incident.get("resolved_at"))
        if status == "resolved" and created and resolved:
            minutes = (resolved - created).total_seconds() / 60.0
            resolution_minutes.append(minutes)
            if minutes <= 24 * 60:
                resolved_within_24h += 1

    windows = _incident_windows(incidents)
    daily_open_counts = _build_daily_counts(windows)
    daily_open_vs_closed = _build_daily_open_closed_counts(windows)
    weekly_open_vs_closed = _build_weekly_open_closed_counts(windows)
    weekly_severity = _build_weekly_severity_timeline(incidents)
    current_open = daily_open_counts[-1][1] if daily_open_counts else status_counts.get("triggered", 0)
    peak_open = max((count for _, count in daily_open_counts), default=current_open)
    peak_date = None
    for day, count in daily_open_counts:
        if count == peak_open:
            peak_date = day.strftime("%Y-%m-%d")
            break
    target_date = (until - timedelta(days=30)).date()
    open_thirty_days_ago = None
    for day, count in reversed(daily_open_counts):
        if day.date() <= target_date:
            open_thirty_days_ago = count
            break

    total_incidents = len(incidents)
    resolved_count = status_counts.get("resolved", 0)
    open_count = total_incidents - resolved_count
    distinct_services = len(service_stats)
    distinct_teams = len(team_stats)
    avg_resolution = sum(resolution_minutes) / len(resolution_minutes) if resolution_minutes else None
    median_resolution = _percentile(resolution_minutes, 0.5)
    p95_resolution = _percentile(resolution_minutes, 0.95)
    fastest_resolution = min(resolution_minutes) if resolution_minutes else None
    slowest_resolution = max(resolution_minutes) if resolution_minutes else None
    resolved_24h_pct = (
        (resolved_within_24h / resolved_count) * 100 if resolved_count else None
    )

    open_incidents = [
        _summarize_incident(incident, now)
        for incident in incidents
        if (incident.get("status") or "").lower() != "resolved"
    ]
    open_incidents.sort(key=lambda entry: entry.get("created_at") or "", reverse=True)

    recent_incidents = [
        _summarize_incident(incident, now)
        for incident in sorted(
            incidents,
            key=lambda item: item.get("created_at") or "",
            reverse=True,
        )[:40]
    ]

    totals = {
        "total": total_incidents,
        "open": open_count,
        "resolved": resolved_count,
        "services": distinct_services,
        "teams": distinct_teams,
        "last_incident_at": max(
            (incident.get("created_at") for incident in incidents if incident.get("created_at")),
            default=None,
        ),
    }

    trend = {
        "daily_open": _serialize_daily_counts(daily_open_counts),
        "daily_open_vs_closed": _serialize_open_closed_counts(daily_open_vs_closed, "date"),
        "weekly_open_vs_closed": _serialize_open_closed_counts(weekly_open_vs_closed, "week_start"),
        "weekly_severity": weekly_severity,
        "snapshot": {
            "current_open": current_open,
            "open_30_days_ago": open_thirty_days_ago,
            "peak_open": {"count": peak_open, "date": peak_date},
        },
    }

    def _breakdown(counter: Counter[str]) -> List[Dict[str, Any]]:
        total = sum(counter.values()) or 1
        entries = [
            {"label": label, "count": count, "percent": (count / total) * 100}
            for label, count in counter.most_common()
        ]
        return entries

    service_breakdown = [
        {
            "service": name,
            "total": stats["total"],
            "open": stats["open"],
            "resolved": stats["resolved"],
            "percent": (stats["total"] / total_incidents * 100) if total_incidents else 0,
        }
        for name, stats in sorted(
            service_stats.items(), key=lambda item: item[1]["total"], reverse=True
        )[:15]
    ]

    team_breakdown = [
        {
            "team": name,
            "total": count,
            "percent": (count / total_incidents * 100) if total_incidents else 0,
        }
        for name, count in team_stats.most_common(15)
    ]

    metrics = {
        "avg_resolution_minutes": avg_resolution,
        "median_resolution_minutes": median_resolution,
        "p95_resolution_minutes": p95_resolution,
        "fastest_resolution_minutes": fastest_resolution,
        "slowest_resolution_minutes": slowest_resolution,
        "resolved_within_24h_percent": resolved_24h_pct,
        "incidents_per_day": (total_incidents / max(1, lookback_days)),
        "incidents_per_week": (total_incidents / max(1, lookback_days / 7)),
    }

    return {
        "generated_at": iso_utc(now),
        "period": {"from": iso_utc(since), "to": iso_utc(until)},
        "lookback_days": lookback_days,
        "totals": totals,
        "status_counts": _breakdown(status_counts),
        "severity_breakdown": _breakdown(severity_stats),
        "urgency_breakdown": _breakdown(urgency_stats),
        "service_breakdown": service_breakdown,
        "team_breakdown": team_breakdown,
        "trend": trend,
        "metrics": metrics,
        "open_incidents": open_incidents[:20],
        "recent_incidents": recent_incidents,
        "source": {
            "incident_count": total_incidents,
            "generated_at": iso_utc(now),
        },
    }


def sync_pagerduty_data(
    base_dir: str,
    output_root: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch PagerDuty data (if configured) and write summary artifacts."""
    log = logger or LOGGER
    token = _load_pagerduty_token(base_dir)
    if not token:
        log.info("PagerDuty token not configured; skipping alerts sync")
        return None
    if requests is None:  # pragma: no cover
        log.info("Python 'requests' package is required for PagerDuty sync. Install it with 'pip install requests'.")
        return None

    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    until = datetime.now(timezone.utc)
    log.info(
        "Fetching PagerDuty incidents (%s days, %s → %s)",
        lookback_days,
        iso_utc(since),
        iso_utc(until),
    )

    total_span_days = max(1, int((until - since).total_seconds() // 86400))
    if total_span_days <= 32:
        windows = [(since, until)]
    else:
        windows = _month_range_iter(since, until)

    incidents_by_id: Dict[str, Dict[str, Any]] = {}
    for idx, (window_start, window_end) in enumerate(windows, start=1):
        log.info(
            "Chunk %s/%s: %s → %s",
            idx,
            len(windows),
            iso_utc(window_start),
            iso_utc(window_end),
        )
        try:
            batch = _fetch_incidents(token, window_start, window_end)
        except Exception as exc:  # pragma: no cover - network failure
            log.info("Warning: failed to fetch PagerDuty incidents: %s", exc)
            return None
        for incident in batch:
            incident_id = incident.get("id") or incident.get("incident_number")
            key = str(incident_id) if incident_id is not None else f"no-id-{len(incidents_by_id)}"
            if key in incidents_by_id:
                continue
            incidents_by_id[key] = incident

    incidents = list(incidents_by_id.values())
    output_dir = os.path.join(os.path.abspath(output_root), "stats", "pagerduty")
    incidents_path = os.path.join(output_dir, "incidents_last_year.json")
    existing_incident_cache = _load_existing_incident_events(incidents_path)
    _augment_incidents_with_logs(token, incidents, existing_incident_cache, log)
    for incident in incidents:
        incident["severity"] = _extract_severity(incident)

    summary = _summarize_incidents(incidents, since, until, lookback_days)
    stats_root = os.path.join(os.path.abspath(output_root), "stats")
    github_lookup = _build_github_user_lookup(stats_root)
    pagerduty_users: List[Dict[str, Any]] = []
    try:
        pagerduty_users = _fetch_pagerduty_users(token)
    except Exception as exc:  # pragma: no cover - network failure
        log.info("Warning: failed to fetch PagerDuty users: %s", exc)
    responder_leaderboard = _build_responder_leaderboard(incidents, pagerduty_users, github_lookup)
    if responder_leaderboard:
        summary["responders"] = responder_leaderboard
        log.info(
            "PagerDuty responders linked: %s total, %s matched to developers",
            responder_leaderboard.get("total_responders"),
            responder_leaderboard.get("matched_responders"),
        )
    os.makedirs(output_dir, exist_ok=True)
    overview_path = os.path.join(output_dir, "overview.json")

    with open(incidents_path, "w", encoding="utf-8") as handle:
        json.dump(incidents, handle, indent=2)
    with open(overview_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    log.info(
        "PagerDuty sync complete: %s incidents written to %s",
        summary["totals"].get("total", 0),
        overview_path,
    )
    return summary
