"""Validate E8 scaling run timings and completeness for results/e8_scaling/runs.csv (#32)."""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

REQUIRED_COLUMNS = ("test", "cores", "fraction", "run", "total_ms")
EXPECTED_RUNS_PER_SETTING = 3
DEFAULT_MAX_REL_DEVIATION = 0.25


def validate_setting_times(
    times: Sequence[float],
    *,
    expected_runs: int = EXPECTED_RUNS_PER_SETTING,
    max_rel_deviation: float = DEFAULT_MAX_REL_DEVIATION,
) -> list[str]:
    """Return validation error messages for one setting's run times.

    Accepts consistent triplicates such as ``[100, 102, 98]`` and flags high
    variance such as ``[100, 150, 99]`` or settings with fewer than ``expected_runs``.
    """
    issues: list[str] = []
    if len(times) != expected_runs:
        issues.append(f"expected {expected_runs} runs, got {len(times)}")
    if not times:
        return issues
    if any(t <= 0 for t in times):
        issues.append(f"non-positive runtime in {list(times)}")
        return issues
    median = statistics.median(times)
    max_dev = max(abs(t - median) / median for t in times)
    if max_dev > max_rel_deviation:
        issues.append(
            f"high variance: max deviation from median is {max_dev:.1%} "
            f"(limit {max_rel_deviation:.1%}) for {list(times)}"
        )
    return issues


def validate_runs_csv(
    csv_path: str | Path,
    *,
    expected_runs: int = EXPECTED_RUNS_PER_SETTING,
    max_rel_deviation: float = DEFAULT_MAX_REL_DEVIATION,
) -> list[str]:
    """Validate every ``(test, cores, fraction)`` group in ``csv_path``."""
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        missing = [col for col in REQUIRED_COLUMNS if col not in fieldnames]
        if missing:
            return [f"missing required column(s): {', '.join(missing)}"]
        grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for row in reader:
            key = (row["test"], row["cores"], row["fraction"])
            grouped[key].append(float(row["total_ms"]))

    if not grouped:
        return ["runs.csv contains no data rows"]

    errors: list[str] = []
    for key, times in sorted(grouped.items()):
        for issue in validate_setting_times(
            times, expected_runs=expected_runs, max_rel_deviation=max_rel_deviation
        ):
            errors.append(f"{key[0]} {key[1]} (fraction={key[2]}): {issue}")
    return errors


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs",
        default="results/e8_scaling/runs.csv",
        help="Path to E8 runs.csv (default: results/e8_scaling/runs.csv)",
    )
    parser.add_argument(
        "--max-rel-deviation",
        type=float,
        default=DEFAULT_MAX_REL_DEVIATION,
        help="Maximum allowed relative deviation from median (default: 0.25)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    errors = validate_runs_csv(args.runs, max_rel_deviation=args.max_rel_deviation)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1
    print(f"OK: {args.runs} passed validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
