"""Tests for the E8 command builder and the run script (issue #15)."""

import subprocess
import sys
from pathlib import Path

import pytest

from src.scaling import commands, run_scaling

ROOT = Path(__file__).resolve().parents[2]


def _flags(command):
    i = command.index("--executor-cores")
    return command[i : i + 4]


def test_test_a_uses_one_executor_with_n_cores():
    command = commands.build_command("A", "4", 0.25)
    assert _flags(command) == ["--executor-cores", "4", "--total-executor-cores", "4"]


def test_four_plus_four_has_two_executors_of_four_cores():
    command = commands.build_command("B", "4+4", 0.25)
    assert _flags(command) == ["--executor-cores", "4", "--total-executor-cores", "8"]


def test_eight_plus_eight_has_two_executors_of_eight_cores():
    command = commands.build_command("B", "8+8", 0.25)
    assert _flags(command) == ["--executor-cores", "8", "--total-executor-cores", "16"]


def test_laptop_prefix_is_a_label_only():
    assert commands.parse_cores("A:8") == (8, 8)
    assert commands.parse_cores("B:8") == (8, 8)


@pytest.mark.parametrize("spec", ["", "0", "x", "4+8", "4+", "C:8"])
def test_bad_cores_are_rejected(spec):
    with pytest.raises(ValueError):
        commands.parse_cores(spec)


def test_log_name():
    assert commands.log_name("A", "2", 0.25, 3) == "e8_A_2_0.25_run3"
    assert commands.log_name("B", "4+4", 1.0, 1) == "e8_B_4+4_1_run1"
    assert commands.log_name("B", "A:8", 0.25, 2) == "e8_B_A-8_0.25_run2"


def test_log_name_rejects_unknown_test_and_bad_fraction():
    with pytest.raises(ValueError):
        commands.log_name("D", "2", 0.25, 1)
    with pytest.raises(ValueError):
        commands.log_name("A", "2", 0, 1)


def test_command_enables_the_event_log_and_passes_the_fraction():
    command = commands.build_command(
        "C", "8+8", 0.5, run=2, master="spark://a:7077", app="job.py", app_args=["--x"]
    )
    assert command[:3] == ["spark-submit", "--master", "spark://a:7077"]
    assert "spark.eventLog.enabled=true" in command
    assert "spark.eventLog.dir=hdfs:///spark-logs" in command
    assert command[command.index("--name") + 1] == "e8_C_8+8_0.5_run2"
    assert command[-3:] == ["--fraction", "0.5", "--x"]


def test_plans_cover_the_proposal_tables():
    assert [c for c, _ in commands.PLAN["A"]] == ["1", "2", "4", "8"]
    assert [f for _, f in commands.PLAN["C"]] == [0.1, 0.25, 0.5, 1.0]
    assert {c for c, _ in commands.PLAN["B"]} == {"A:8", "B:8", "4+4", "8+8"}


def test_whole_plan_runs_each_setting_three_times():
    args = run_scaling.parse_args(["--test", "A"])
    assert len(run_scaling.commands_for(args)) == 4 * 3


def test_single_setting_with_runs():
    args = run_scaling.parse_args(["--test", "A", "--cores", "2", "--runs", "2"])
    assert len(run_scaling.commands_for(args)) == 2


def test_dry_run_prints_and_starts_nothing(capsys, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("must not start Spark")

    monkeypatch.setattr(run_scaling.subprocess, "run", boom)
    assert run_scaling.main(["--test", "A", "--cores", "2", "--dry-run", "--runs", "1"]) == 0
    assert "--executor-cores 2" in capsys.readouterr().out


def test_shell_wrapper_dry_run():
    result = subprocess.run(
        [str(ROOT / "scripts" / "run_scaling.sh"), "--test", "A", "--cores", "2"]
        + ["--dry-run", "--runs", "1"],
        capture_output=True, text=True, env={"PATH": "/usr/bin:/bin", "PYTHON": sys.executable},
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert "spark-submit" in result.stdout
