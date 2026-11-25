"""Tests for scripts/cluster/validate_runs.py (#32)."""

from __future__ import annotations

import csv
from pathlib import Path

from scripts.cluster.validate_runs import main, validate_runs_csv, validate_setting_times


def test_validate_setting_times_accepts_consistent_triplicate() -> None:
    assert validate_setting_times([100, 102, 98]) == []


def test_validate_setting_times_flags_high_variance() -> None:
    issues = validate_setting_times([100, 150, 99])
    assert len(issues) == 1
    assert "high variance" in issues[0]


def test_validate_setting_times_flags_missing_run() -> None:
    issues = validate_setting_times([100, 102])
    assert len(issues) == 1
    assert "expected 3 runs, got 2" in issues[0]


def test_validate_runs_csv_roundtrip(tmp_path: Path) -> None:
    good_csv = tmp_path / "good.csv"
    with good_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["test", "cores", "fraction", "run", "total_ms"])
        for run, ms in enumerate((100000, 102000, 98000), start=1):
            writer.writerow(["A", "4", "0.25", run, ms])
    assert validate_runs_csv(good_csv) == []
    assert main(["--runs", str(good_csv)]) == 0

    bad_csv = tmp_path / "bad.csv"
    with bad_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["test", "cores", "fraction", "run", "total_ms"])
        for run, ms in enumerate((100000, 150000, 99000), start=1):
            writer.writerow(["A", "4", "0.25", run, ms])
    assert len(validate_runs_csv(bad_csv)) == 1
    assert main(["--runs", str(bad_csv)]) == 1
