"""Tests for the E8 speedup analysis (issue #15)."""

import csv
import json
from pathlib import Path

import pytest

from src.scaling import analyze
from src.scaling.eventlogs import COLUMNS


def _row(test, cores, run, total_s, stages=None, fraction=0.25, slowest_s=10.0):
    row = dict.fromkeys(COLUMNS, 0)
    row.update(
        test=test, cores=cores, fraction=fraction, run=run, total_ms=int(total_s * 1000),
        shuffle_write_bytes=int(2e9), slowest_task_ms=int(slowest_s * 1000), median_task_ms=1500,
        stage_ms_json=json.dumps({str(k): int(v * 1000) for k, v in (stages or {1: 1.0}).items()}),
    )  # fmt: skip
    return {k: str(v) for k, v in row.items()}


def _amdahl_rows(share=0.2, t1=100.0):
    """Three runs per core count that follow Amdahl's law exactly (middle run)."""
    rows = []
    for cores in (1, 2, 4, 8):
        time = t1 * (share + (1 - share) / cores)
        rows += [_row("A", str(cores), run, time + run - 2, {1: time}) for run in (1, 2, 3)]
    return rows


def _write(path: Path, rows) -> Path:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_summary_takes_the_median_of_the_runs():
    rows = [_row("A", "2", 1, 12), _row("A", "2", 2, 10), _row("A", "2", 3, 20)]
    (row,) = analyze.summarise(rows)
    assert (row["runs"], row["median_s"], row["min_s"], row["max_s"]) == (3, 12.0, 10.0, 20.0)
    assert row["shuffle_write_gb"] == 2.0
    assert row["same_runs_as"] == ""


def test_copied_runs_are_marked_and_real_repeats_are_not():
    a = [_row("A", "8", run, 10 + run) for run in (1, 2, 3)]
    copy = [dict(r, test="B", cores="B:8") for r in a]
    other = [_row("B", "A:8", run, 20 + run) for run in (1, 2, 3)]
    marks = {
        (r["test"], r["cores"]): r["same_runs_as"] for r in analyze.summarise(a + copy + other)
    }
    assert marks == {("A", "8"): "", ("B", "A:8"): "", ("B", "B:8"): "A 8"}


def test_core_scaling_recovers_the_serial_share():
    scaling = analyze.core_scaling(analyze.summarise(_amdahl_rows(share=0.2)))
    assert scaling["cores"] == [1, 2, 4, 8]
    assert scaling["serial_share"] == pytest.approx(0.2, abs=0.01)
    assert scaling["speedup"][-1] == pytest.approx(1 / (0.2 + 0.8 / 8), rel=0.01)
    assert scaling["fitted_speedup"] == pytest.approx(scaling["speedup"], rel=0.01)
    assert scaling["efficiency"][0] == 1.0


def test_core_scaling_needs_two_core_counts():
    with pytest.raises(ValueError, match="two core counts"):
        analyze.core_scaling(analyze.summarise([_row("A", "2", 1, 5)]))


def test_stage_medians_dominant_stages_and_pauses():
    rows = [_row("A", "8", run, 100, {1: 50, 2: 1, 3: 30 + run, 4: 0.5}) for run in (1, 2, 3)]
    rows += [_row("B", "8+8", 1, 90, {1: 40, 2: 11, 3: 20, 4: 10})]
    medians = analyze.stage_medians(rows)
    one = medians[("A", "8", 0.25)]
    assert one[3] == 32.0
    assert analyze.dominant_stages(one, count=2) == [1, 3]
    ids, extra = analyze.pause_stages(medians[("B", "8+8", 0.25)], one)
    assert ids == [2, 4]
    assert extra == pytest.approx((11 - 1) + (10 - 0.5))


def test_main_writes_the_tables_and_plots_without_test_c(tmp_path, capsys):
    rows = _amdahl_rows() + [_row("B", "8+8", run, 20 + run, {1: 20}) for run in (1, 2, 3)]
    runs = _write(tmp_path / "runs.csv", rows)
    out = tmp_path / "analysis"
    assert analyze.main(["--runs", str(runs), "--data-runs", str(tmp_path / "no.csv"),
                         "--out", str(out)]) == 0  # fmt: skip
    for name in ("summary.csv", "stages.csv", "speedup_cores.png", "second_machine.png"):
        assert (out / name).stat().st_size > 0, name
    assert not (out / "data_size.png").exists()
    printed = capsys.readouterr().out
    assert "test C is skipped" in printed
    assert "serial share" in printed


def test_main_plots_test_c_when_its_runs_exist(tmp_path):
    runs = _write(tmp_path / "runs.csv", _amdahl_rows())
    data = _write(
        tmp_path / "data.csv",
        [_row("C", "8+8", 1, 10, fraction=0.1), _row("C", "8+8", 1, 80, fraction=1.0)],
    )
    out = tmp_path / "analysis"
    assert analyze.main(["--runs", str(runs), "--data-runs", str(data), "--out", str(out)]) == 0
    assert (out / "data_size.png").stat().st_size > 0


def test_empty_runs_file_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="no runs"):
        analyze.load_runs(_write(tmp_path / "runs.csv", []))
