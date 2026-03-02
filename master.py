#!/usr/bin/env python3
"""Run subsystem_stats.py for all repos across a rolling window of months."""
from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from subsystem_stats import discover_git_repos, load_json_file

UTC = timezone.utc
MASTER_PROGRESS_PREFIX = "[[MASTER_PROGRESS]]"
SERVICES_FILENAME = "services.json"


def _detect_cpu_count() -> int:
    try:
        affinity = os.sched_getaffinity(0)
        if affinity:
            return max(1, len(affinity))
    except (AttributeError, OSError):
        pass
    return os.cpu_count() or 1


CPU_COUNT = _detect_cpu_count()
DEFAULT_PARALLELISM = max(1, CPU_COUNT * 2)


def compute_default_month_window(reference: datetime | None = None) -> int:
    if reference is None:
        reference = datetime.now(tz=UTC)
    current_month = max(1, min(12, reference.month))
    return 12 + current_month


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months",
        type=int,
        default=None,
        help=(
            "How many months (including current) to process. "
            "Defaults to previous full calendar year plus months elapsed in the current year."
        ),
    )
    parser.add_argument(
        "--repos-dir",
        type=Path,
        default=Path("repos"),
        help="Directory containing git repositories (default: repos)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("configuration"),
        help="Configuration directory passed through to subsystem_stats (default: configuration)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("stats"),
        help="Statistics output directory passed through to subsystem_stats (default: stats)",
    )
    parser.add_argument(
        "--subsystem-script",
        type=Path,
        default=Path(__file__).with_name("subsystem_stats.py"),
        help="Path to subsystem_stats.py (default: alongside this script)",
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path(sys.executable),
        help="Python interpreter to use when invoking subsystem_stats.py",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=DEFAULT_PARALLELISM,
        help=f"Number of months to process concurrently (default: 2 x CPU count = {DEFAULT_PARALLELISM})",
    )
    parser.add_argument(
        "--progress-events",
        action="store_true",
        help="Emit structured progress events for dashboards",
    )
    return parser.parse_args()


def compute_month_sequence(months: int, reference: datetime | None = None) -> List[Tuple[int, int]]:
    if months <= 0:
        raise ValueError("--months must be a positive integer")
    if reference is None:
        reference = datetime.now(tz=UTC)
    year = reference.year
    month = reference.month
    sequence: List[Tuple[int, int]] = []
    for _ in range(months):
        sequence.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    sequence.reverse()
    return sequence


def load_services_config(config_dir: Path) -> Dict[str, Dict[str, List[str]]]:
    services_path = config_dir / SERVICES_FILENAME
    data = load_json_file(services_path)
    return data if isinstance(data, dict) else {}


def count_effective_subsystems(
    repos: Sequence[str], services_config: Dict[str, Dict[str, List[str]]]
) -> int:
    total = 0
    for rel_path in repos:
        repo_config = services_config.get(rel_path)
        if isinstance(repo_config, dict) and repo_config:
            entries = [name for name in repo_config.keys() if isinstance(name, str)]
            total += len(entries)
        else:
            total += 1
    return total


class ProgressEmitter:
    def __init__(self, total_steps: int, steps_per_month: int, total_months: int, enabled: bool) -> None:
        self.total_steps = max(total_steps, 0)
        self.steps_per_month = max(steps_per_month, 0)
        self.total_months = max(total_months, 0)
        self.enabled = (
            enabled
            and self.total_steps > 0
            and self.steps_per_month > 0
            and self.total_months > 0
        )
        self._lock = threading.Lock()
        self._steps_completed = 0
        self._months_completed = 0

    def emit_initial(self) -> None:
        if not self.enabled:
            return
        self._print_payload("Analysis started")

    def unit_completed(self, repo: str, year: int, month: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._steps_completed = min(self.total_steps, self._steps_completed + 1)
            steps_completed = self._steps_completed
            months_completed = self._months_completed
        label = f"Processed {repo} ({year}-{month:02d})"
        self._print_payload(label, steps_completed, months_completed)

    def month_completed(self, year: int, month: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._months_completed += 1
            self._steps_completed = max(
                self._steps_completed,
                min(self.total_steps, self._months_completed * self.steps_per_month),
            )
            label = f"Completed {year}-{month:02d} ({self._months_completed}/{self.total_months} months)"
            steps_completed = self._steps_completed
            months_completed = self._months_completed
        self._print_payload(label, steps_completed, months_completed)

    def _print_payload(
        self,
        label: str,
        steps_completed: int | None = None,
        months_completed: int | None = None,
    ) -> None:
        if not self.enabled:
            return
        payload = {
            "label": label,
            "steps_completed": steps_completed if steps_completed is not None else self._steps_completed,
            "total_steps": self.total_steps,
            "months_completed": months_completed if months_completed is not None else self._months_completed,
            "total_months": self.total_months,
        }
        print(f"{MASTER_PROGRESS_PREFIX} {json.dumps(payload)}", flush=True)


def run_for_month(
    interpreter: Path,
    script_path: Path,
    repos: Sequence[str],
    year: int,
    month: int,
    repos_dir: Path,
    config_dir: Path,
    output_dir: Path,
    progress: Optional[ProgressEmitter] = None,
) -> None:
    cmd: List[str] = [
        str(interpreter),
        str(script_path),
        "--year",
        str(year),
        "--month",
        str(month),
        "--repos-dir",
        str(repos_dir),
        "--config-dir",
        str(config_dir),
        "--output-dir",
        str(output_dir),
    ]
    for repo in repos:
        cmd.extend(["--repo", repo])
    if progress is not None:
        cmd.append("--progress-events")
    repo_label = f"{len(repos)} repos" if repos else "all discovered repos"
    print(f"[MASTER] Running subsystem_stats for {repo_label} {year}-{month:02d}")
    completed_repos: Set[str] = set()
    proc = subprocess.Popen(
        cmd,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.rstrip("\n")
        if progress is not None and line.startswith(MASTER_PROGRESS_PREFIX):
            payload_text = line[len(MASTER_PROGRESS_PREFIX) :].strip()
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                continue
            repo_name = payload.get("repo")
            if repo_name and repo_name not in completed_repos:
                completed_repos.add(repo_name)
                progress.unit_completed(repo_name, year, month)
            continue
        print(line)
    result = proc.wait()
    if progress is not None:
        for repo in repos:
            if repo not in completed_repos:
                progress.unit_completed(repo, year, month)
    if result != 0:
        raise RuntimeError(
            f"subsystem_stats failed for {repo_label} {year}-{month:02d} (exit code {result})"
        )


def process_month(
    year: int,
    month: int,
    repos: Sequence[str],
    interpreter: Path,
    script_path: Path,
    repos_dir: Path,
    config_dir: Path,
    output_dir: Path,
    progress: Optional[ProgressEmitter] = None,
) -> Tuple[int, int]:
    label = f"{year}-{month:02d}"
    print(f"[MASTER] Starting month {label}")
    run_for_month(
        interpreter,
        script_path,
        repos,
        year,
        month,
        repos_dir,
        config_dir,
        output_dir,
        progress,
    )
    print(f"[MASTER] Completed month {label}")
    return year, month


_PAGERDUTY_MODULE_CACHE: Optional[Tuple[Callable[..., Any], Any]] = None
_PAGERDUTY_LOAD_FAILED = False


def _has_pagerduty_token(config_dir: Path) -> bool:
    env_token = os.environ.get("PAGERDUTY_API_TOKEN")
    if env_token and env_token.strip():
        return True
    integrations_path = config_dir / "integrations.json"
    if not integrations_path.is_file():
        return False
    try:
        data = json.loads(integrations_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    pagerduty_config = data.get("pagerduty") or {}
    token = pagerduty_config.get("api_token")
    return isinstance(token, str) and bool(token.strip())


def _load_pagerduty_sync() -> Tuple[Optional[Callable[..., Any]], Optional[Any]]:
    global _PAGERDUTY_MODULE_CACHE, _PAGERDUTY_LOAD_FAILED
    if _PAGERDUTY_MODULE_CACHE is not None:
        return _PAGERDUTY_MODULE_CACHE
    if _PAGERDUTY_LOAD_FAILED:
        return None, None
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / "pagerduty_sync.py",
        script_dir / "legacy" / "pagerduty_sync.py",
    ]
    module_path = next((path for path in candidates if path.is_file()), None)
    if module_path is None:
        _PAGERDUTY_LOAD_FAILED = True
        return None, None
    module_name = "pagerduty_sync_loader"
    if "legacy" in module_path.parts:
        module_name = "legacy_pagerduty_sync"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        _PAGERDUTY_LOAD_FAILED = True
        return None, None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[call-arg, attr-defined]
    except Exception as exc:  # noqa: BLE001
        print(f"[MASTER] Unable to load PagerDuty integration: {exc}")
        _PAGERDUTY_LOAD_FAILED = True
        return None, None
    sync_fn = getattr(module, "sync_pagerduty_data", None)
    if not callable(sync_fn):
        _PAGERDUTY_LOAD_FAILED = True
        return None, None
    _PAGERDUTY_MODULE_CACHE = (sync_fn, module)
    return _PAGERDUTY_MODULE_CACHE


def _resolve_pagerduty_base_dir(
    config_dir: Path, stack: contextlib.ExitStack
) -> Optional[Path]:
    config_dir = config_dir.resolve()
    if not config_dir.exists():
        return None
    if config_dir.name == "configuration" and config_dir.parent.exists():
        return config_dir.parent
    temp_dir_obj = stack.enter_context(tempfile.TemporaryDirectory(prefix="pagerduty-base-"))
    base_dir = Path(temp_dir_obj.name)
    target = base_dir / "configuration"
    try:
        target.symlink_to(config_dir, target_is_directory=True)
    except OSError:
        return None
    return base_dir


def _resolve_pagerduty_output_root(
    output_dir: Path, stack: contextlib.ExitStack
) -> Optional[Path]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_dir.name == "stats" and output_dir.parent.exists():
        return output_dir.parent
    temp_dir_obj = stack.enter_context(tempfile.TemporaryDirectory(prefix="pagerduty-output-"))
    temp_root = Path(temp_dir_obj.name)
    target = temp_root / "stats"
    try:
        target.symlink_to(output_dir, target_is_directory=True)
    except OSError:
        return None
    return temp_root


def _get_pagerduty_logger() -> logging.Logger:
    logger = logging.getLogger("pagerduty_sync")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("[PAGERDUTY] %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def run_pagerduty_sync_if_configured(config_dir: Path, output_dir: Path) -> None:
    config_dir = config_dir.resolve()
    output_dir = output_dir.resolve()
    if not _has_pagerduty_token(config_dir):
        return
    sync_entry = _load_pagerduty_sync()
    if not sync_entry or not sync_entry[0]:
        print("[MASTER] PagerDuty integration unavailable; skipping alerts sync")
        return
    sync_fn, module = sync_entry
    with contextlib.ExitStack() as stack:
        base_dir = _resolve_pagerduty_base_dir(config_dir, stack)
        if base_dir is None:
            print(f"[MASTER] PagerDuty configuration directory not found at {config_dir}")
            return
        output_root = _resolve_pagerduty_output_root(output_dir, stack)
        if output_root is None:
            print(f"[MASTER] PagerDuty output directory unavailable at {output_dir}")
            return
        lookback = getattr(module, "DEFAULT_LOOKBACK_DAYS", 365)
        logger = _get_pagerduty_logger()
        print(f"[MASTER] Syncing PagerDuty incidents (lookback: {lookback} days)")
        try:
            summary = sync_fn(
                str(base_dir),
                str(output_root),
                lookback_days=lookback,
                logger=logger,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[MASTER] PagerDuty sync failed: {exc}")
            return
        if summary:
            print("[MASTER] PagerDuty sync completed")
        else:
            print("[MASTER] PagerDuty sync skipped (no data returned)")


def generate_precomputed_assets(output_dir: Path) -> None:
    output_dir = output_dir.resolve()
    ownership_target = output_dir / "ownership_distribution.json"
    team_overview_target = output_dir / "team_overview_overall.json"
    os.environ["DISABLE_DASHBOARD_SCHEDULER"] = "1"
    os.environ["REPO_SQUIRREL_STATS_ROOT"] = str(output_dir)
    try:
        from dashboard_server import build_ownership_distribution_snapshot, build_team_overview_snapshot
    except Exception as exc:  # noqa: BLE001
        print(f"[MASTER] Skipping precomputed assets: unable to import dashboard server ({exc})")
        return
    try:
        ownership_snapshot = build_ownership_distribution_snapshot(stats_root=str(output_dir))
    except Exception as exc:  # noqa: BLE001
        print(f"[MASTER] Failed to build ownership distribution snapshot: {exc}")
        ownership_snapshot = None
    if ownership_snapshot:
        try:
            ownership_target.parent.mkdir(parents=True, exist_ok=True)
            ownership_target.write_text(json.dumps(ownership_snapshot, indent=2), encoding="utf-8")
            print(f"[MASTER] Ownership distribution snapshot updated ({ownership_target})")
        except Exception as exc:  # noqa: BLE001
            print(f"[MASTER] Failed to save ownership distribution snapshot: {exc}")
    try:
        team_snapshot = build_team_overview_snapshot("overall")
    except Exception as exc:  # noqa: BLE001
        print(f"[MASTER] Failed to build team overview snapshot: {exc}")
        team_snapshot = None
    if team_snapshot and team_snapshot.get("teams"):
        try:
            team_overview_target.parent.mkdir(parents=True, exist_ok=True)
            team_overview_target.write_text(json.dumps(team_snapshot, indent=2), encoding="utf-8")
            print(f"[MASTER] Team overview snapshot updated ({team_overview_target})")
        except Exception as exc:  # noqa: BLE001
            print(f"[MASTER] Failed to save team overview snapshot: {exc}")


def main() -> None:
    args = parse_args()
    if args.parallel <= 0:
        raise ValueError("--parallel must be a positive integer")

    requested_months = args.months
    if requested_months is None:
        requested_months = compute_default_month_window()
    if requested_months <= 0:
        raise ValueError("--months must be a positive integer")

    repos_dir = args.repos_dir.resolve()
    config_dir = args.config_dir.resolve()
    output_dir = args.output_dir.resolve()
    script_path = args.subsystem_script.resolve()
    interpreter = args.python.resolve()

    if not script_path.exists():
        raise FileNotFoundError(f"subsystem_stats script not found at {script_path}")

    discovered = discover_git_repos(repos_dir)
    if not discovered:
        raise SystemExit(f"No git repositories found under {repos_dir}")

    months_to_run = compute_month_sequence(requested_months)
    repos = sorted(discovered.keys())
    total_months = len(months_to_run)
    services_config = load_services_config(config_dir)
    subsystem_units = count_effective_subsystems(repos, services_config)
    if subsystem_units <= 0:
        subsystem_units = max(len(repos), 1)
    total_steps = subsystem_units * total_months
    progress_emitter = ProgressEmitter(
        total_steps=total_steps,
        steps_per_month=subsystem_units,
        total_months=total_months,
        enabled=args.progress_events,
    )
    print(
        f"[MASTER] Processing {len(repos)} repos across {total_months} months"
    )
    if progress_emitter.enabled:
        print(
            f"[MASTER] Progress tracking: {subsystem_units} subsystems x {total_months} months = {total_steps} steps"
        )
        progress_emitter.emit_initial()

    if args.parallel == 1:
        for year, month in months_to_run:
            process_month(
                year,
                month,
                repos,
                interpreter,
                script_path,
                repos_dir,
                config_dir,
                output_dir,
                progress=progress_emitter,
            )
            progress_emitter.month_completed(year, month)
    else:
        max_workers = min(args.parallel, len(months_to_run))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    process_month,
                    year,
                    month,
                    repos,
                    interpreter,
                    script_path,
                    repos_dir,
                    config_dir,
                    output_dir,
                    progress_emitter,
                ): (year, month)
                for year, month in months_to_run
            }
            for future in as_completed(future_map):
                year, month = future_map[future]
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(
                        f"Month {year}-{month:02d} failed: {exc}"
                    ) from exc
                progress_emitter.month_completed(year, month)

    run_pagerduty_sync_if_configured(config_dir, output_dir)
    generate_precomputed_assets(output_dir)
    print("[MASTER] Completed all subsystem statistics runs")


if __name__ == "__main__":
    main()
