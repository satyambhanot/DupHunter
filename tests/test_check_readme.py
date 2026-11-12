"""Tests for scripts/check_readme.py (issue #22)."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_readme", ROOT / "scripts" / "check_readme.py")
check_readme = importlib.util.module_from_spec(spec)
sys.modules["check_readme"] = check_readme
spec.loader.exec_module(check_readme)


def _repo(tmp_path, readme_text, module_text=None):
    (tmp_path / "src" / "demo").mkdir(parents=True)
    (tmp_path / "README.md").write_text(readme_text)
    if module_text is not None:
        (tmp_path / "src" / "demo" / "tool.py").write_text(module_text)
    return tmp_path


HELPFUL = "import argparse\nargparse.ArgumentParser().parse_args()\n"


def test_commands_are_found_with_their_line_numbers():
    text = "intro\n```bash\npython -m src.etl.parse_posts --help\npython3 -m src.demo.tool x\n```"
    assert check_readme.documented_modules(text) == [
        (3, "src.etl.parse_posts"),
        (4, "src.demo.tool"),
    ]


def test_a_working_command_passes(tmp_path):
    root = _repo(tmp_path, "python -m src.demo.tool --help\n", HELPFUL)
    assert check_readme.check(root, run_help=True) == []


def test_a_missing_module_is_reported_with_its_line(tmp_path):
    root = _repo(tmp_path, "intro\npython -m src.nothing --help\n")
    problems = check_readme.check(root, run_help=False)
    assert len(problems) == 1
    assert "README.md:2" in problems[0] and "src.nothing" in problems[0]


def test_a_module_whose_help_fails_is_reported(tmp_path):
    root = _repo(tmp_path, "python -m src.demo.tool\n", "raise SystemExit(3)\n")
    problems = check_readme.check(root, run_help=True)
    assert len(problems) == 1 and "exited 3" in problems[0]


def test_main_exits_one_on_a_broken_command(tmp_path, monkeypatch):
    root = _repo(tmp_path, "python -m src.nothing\n")
    monkeypatch.setattr(check_readme, "ROOT", root)
    assert check_readme.main(["--no-help"]) == 1


def test_modules_with_code_need_the_section(tmp_path):
    root = _repo(tmp_path, "x", "pass\n")
    assert check_readme.modules_without_section(root) == ["src/demo"]
    (root / "src" / "demo" / "README.md").write_text("## Our code vs. libraries\nall ours")
    assert check_readme.modules_without_section(root) == []


def test_the_real_readmes_are_in_order():
    """Every documented command exists and answers --help; every code module has its section."""
    assert check_readme.check(ROOT, run_help=True) == []
    assert check_readme.modules_without_section(ROOT) == []
