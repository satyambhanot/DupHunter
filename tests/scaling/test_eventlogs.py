"""Tests for the event-log parser (issue #15), using a trimmed real Spark event log."""

import csv
import json
from pathlib import Path

import pytest

from src.scaling import eventlogs

FIXTURE = Path(__file__).parent / "fixtures" / "eventlog_sample.jsonl"


@pytest.fixture
def summary():
    with FIXTURE.open() as handle:
        return eventlogs.parse_log(handle)


def test_total_time_is_application_end_minus_start(summary):
    assert summary["total_ms"] == 2960


def test_one_row_per_stage(summary):
    assert [s["id"] for s in summary["stages"]] == [0, 2, 4, 7]
    assert [s["ms"] for s in summary["stages"]] == [489, 45, 34, 28]


def test_task_times_and_stragglers(summary):
    assert summary["n_tasks"] == 5
    assert summary["slowest_task_ms"] == 135 and summary["median_task_ms"] == 40


def test_shuffle_volume_and_peak_memory(summary):
    assert summary["shuffle_read_bytes"] == 1206 and summary["shuffle_write_bytes"] == 748
    assert summary["peak_execution_memory"] == 8650736


def test_settings_come_from_the_application_name(summary):
    row = eventlogs.run_row(summary)
    assert (row["test"], row["cores"], row["fraction"], row["run"]) == ("A", "2", 0.25, 1)
    assert json.loads(row["stage_ms_json"]) == {"0": 489, "2": 45, "4": 34, "7": 28}


def test_names_round_trip_through_the_command_builder():
    from src.scaling import commands

    for test, cores in [("A", "4"), ("B", "4+4"), ("B", "A:8"), ("C", "8+8")]:
        name = commands.log_name(test, cores, 0.5, 2)
        assert eventlogs.parse_name(name) == {
            "test": test,
            "cores": cores,
            "fraction": 0.5,
            "run": 2,
        }


def test_other_application_names_are_rejected():
    with pytest.raises(ValueError, match="not an E8"):
        eventlogs.parse_name("my-app")


def test_unfinished_run_is_reported():
    lines = [line for line in FIXTURE.read_text().splitlines() if "ApplicationEnd" not in line]
    with pytest.raises(ValueError, match="did not finish"):
        eventlogs.parse_log(lines)


def test_collect_and_write_runs(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "app-1").write_text(FIXTURE.read_text())
    (logs / ".app-1.crc").write_text("ignored")
    rows = eventlogs.collect_runs([logs])
    assert len(rows) == 1
    out = tmp_path / "runs.csv"
    eventlogs.write_runs(rows, out)
    with out.open() as handle:
        written = list(csv.DictReader(handle))
    assert list(written[0]) == eventlogs.COLUMNS and written[0]["total_ms"] == "2960"


def test_unfinished_log_names_the_file(tmp_path):
    bad = tmp_path / "app-2.inprogress"
    bad.write_text(FIXTURE.read_text().splitlines()[0] + "\n")
    with pytest.raises(ValueError, match="app-2.inprogress"):
        eventlogs.collect_runs([bad])
