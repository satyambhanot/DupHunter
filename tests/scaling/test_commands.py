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
    assert command[command.index("--fraction") + 1] == "0.5"
    assert command[-1] == "--x"  # extra application arguments come last


def test_default_application_exists_and_is_the_core_pipeline():
    assert commands.DEFAULT_APP == "scripts/cluster/stages.py"
    assert (ROOT / commands.DEFAULT_APP).is_file()


def test_master_is_passed_on_to_the_application():
    # stages.py builds its own session from --master (default local[*]), which would win otherwise
    command = commands.build_command("A", "2", 0.25, master="spark://a:7077")
    app = command.index(commands.DEFAULT_APP)
    assert command[app + 1 :].count("--master") == 1
    rest = command[app + 1 :]
    assert rest[rest.index("--master") + 1] == "spark://a:7077"


def test_py_files_are_shipped_before_the_application_only_when_given():
    assert "--py-files" not in commands.build_command("A", "2", 0.25)
    command = commands.build_command("A", "2", 0.25, py_files="/tmp/src.zip")
    assert command[command.index("--py-files") + 1] == "/tmp/src.zip"
    assert command.index("--py-files") < command.index(commands.DEFAULT_APP)


def test_each_run_has_its_own_outputs_and_metrics_file():
    first = commands.build_command("A", "2", 0.25, run=1)
    second = commands.build_command("A", "2", 0.25, run=2)
    root = first[first.index("--output-root") + 1]
    assert root == "hdfs:///data/e8_scaling/e8_A_2_0.25_run1"
    assert second[second.index("--output-root") + 1] != root
    assert first[first.index("--metrics-out") + 1] == (
        "results/e8_scaling/run_metrics/e8_A_2_0.25_run1.csv"
    )


def test_only_the_chosen_setting_runs_unless_told_otherwise():
    command = commands.build_command("A", "2", 0.25)
    assert command[command.index("--setting") + 1] == "chosen"
    assert "--setting" not in commands.build_command("A", "2", 0.25, setting=None)


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


def test_dry_run_ships_a_zip_of_src(capsys):
    assert run_scaling.main(["--test", "A", "--cores", "2", "--dry-run", "--runs", "1"]) == 0
    out = capsys.readouterr().out
    assert "--py-files" in out
    assert "src.zip" in out
    assert "scripts/cluster/stages.py" in out


def test_zero_runs_is_rejected():
    with pytest.raises(SystemExit):
        run_scaling.main(["--test", "A", "--runs", "0", "--dry-run"])
