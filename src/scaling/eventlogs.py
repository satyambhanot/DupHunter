"""Read Spark event logs and turn each run into one row of the E8 run table.

An event log is a file of JSON lines, one event per line. For every run we keep the total time,
the time of each stage, the shuffle volume, the peak execution memory and how much slower the
slowest task was than the median one (stragglers). The experiment, cores, data fraction and run
number come from the application name (see ``src.scaling.commands.log_name``).
"""

import csv
import json
import re
import statistics
from collections.abc import Iterable
from pathlib import Path

COLUMNS = [
    "test", "cores", "fraction", "run", "total_ms", "n_stages", "n_tasks",
    "shuffle_read_bytes", "shuffle_write_bytes", "peak_execution_memory",
    "slowest_task_ms", "median_task_ms", "stage_ms_json",
]  # fmt: skip
_NAME = re.compile(r"^e8_(?P<test>[ABC])_(?P<cores>[^_]+)_(?P<fraction>[\d.]+)_run(?P<run>\d+)$")


def parse_name(app_name: str) -> dict:
    """Split ``e8_A_2_0.25_run3`` into ``test``, ``cores``, ``fraction`` and ``run``.

    Raises:
        ValueError: if the name does not follow the pattern.
    """
    match = _NAME.match(app_name)
    if not match:
        raise ValueError(f"not an E8 application name: {app_name!r}")
    return {
        "test": match["test"],
        "cores": match["cores"].replace("-", ":"),
        "fraction": float(match["fraction"]),
        "run": int(match["run"]),
    }


def parse_log(lines: Iterable[str]) -> dict:
    """Summarise one event log.

    Args:
        lines: the JSON lines of the log.

    Returns:
        ``app_name``, ``total_ms`` (application end minus start), ``stages`` (a list of
        ``{"id", "name", "ms"}``), ``n_tasks``, shuffle read and write bytes, the peak
        execution memory over all tasks, and the slowest and median task times in milliseconds.

    Raises:
        ValueError: if the log has no application start or end (an unfinished run).
    """
    start = end = None
    app_name = ""
    stages = []
    task_ms = []
    shuffle_read = shuffle_write = peak = 0
    for line in lines:
        if not line.strip():
            continue
        event = json.loads(line)
        kind = event.get("Event")
        if kind == "SparkListenerApplicationStart":
            start, app_name = event["Timestamp"], event["App Name"]
        elif kind == "SparkListenerApplicationEnd":
            end = event["Timestamp"]
        elif kind == "SparkListenerStageCompleted":
            info = event["Stage Info"]
            stages.append(
                {
                    "id": info["Stage ID"],
                    "name": info["Stage Name"],
                    "ms": info["Completion Time"] - info["Submission Time"],
                }
            )
        elif kind == "SparkListenerTaskEnd" and event.get("Task Metrics"):
            task = event["Task Info"]
            metrics = event["Task Metrics"]
            task_ms.append(task["Finish Time"] - task["Launch Time"])
            read = metrics.get("Shuffle Read Metrics", {})
            shuffle_read += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
            shuffle_write += metrics.get("Shuffle Write Metrics", {}).get(
                "Shuffle Bytes Written", 0
            )
            peak = max(peak, metrics.get("Peak Execution Memory", 0))
    if start is None or end is None:
        raise ValueError("the event log has no application start or end (the run did not finish)")
    return {
        "app_name": app_name,
        "total_ms": end - start,
        "stages": stages,
        "n_tasks": len(task_ms),
        "shuffle_read_bytes": shuffle_read,
        "shuffle_write_bytes": shuffle_write,
        "peak_execution_memory": peak,
        "slowest_task_ms": max(task_ms, default=0),
        "median_task_ms": statistics.median(task_ms) if task_ms else 0,
    }


def run_row(summary: dict) -> dict:
    """One row of ``runs.csv`` from a parsed log (settings come from the application name)."""
    row = parse_name(summary["app_name"])
    row.update(
        total_ms=summary["total_ms"],
        n_stages=len(summary["stages"]),
        n_tasks=summary["n_tasks"],
        shuffle_read_bytes=summary["shuffle_read_bytes"],
        shuffle_write_bytes=summary["shuffle_write_bytes"],
        peak_execution_memory=summary["peak_execution_memory"],
        slowest_task_ms=summary["slowest_task_ms"],
        median_task_ms=summary["median_task_ms"],
        stage_ms_json=json.dumps({s["id"]: s["ms"] for s in summary["stages"]}),
    )
    return row


def collect_runs(paths: Iterable[str | Path]) -> list[dict]:
    """Parse every event log under the given files or directories, sorted by setting and run."""
    rows = []
    for path in map(Path, paths):
        files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
        for file in files:
            if file.name.startswith(".") or file.suffix == ".crc":
                continue
            try:
                with file.open() as handle:
                    rows.append(run_row(parse_log(handle)))
            except ValueError as error:
                raise ValueError(f"{file}: {error}") from error
    return sorted(rows, key=lambda r: (r["test"], r["cores"], r["fraction"], r["run"]))


def write_runs(rows: list[dict], path: str | Path) -> None:
    """Write the run table as CSV."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
