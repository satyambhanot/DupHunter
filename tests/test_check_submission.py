"""Tests for scripts/check_submission.py (issue #25)."""

import importlib.util
import io
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest
from pypdf import PdfWriter

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "check_submission", ROOT / "scripts" / "check_submission.py"
)
cs = importlib.util.module_from_spec(spec)
sys.modules["check_submission"] = cs
spec.loader.exec_module(cs)


def _pdf(path, pages):
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=200, height=200)
    with open(path, "wb") as handle:
        writer.write(handle)


# -------------------------------------------------------------------- report pages
@pytest.mark.parametrize(
    ("pages", "ok"), [(9, False), (10, True), (12, True), (15, True), (16, False)]
)
def test_page_count_limits(tmp_path, pages, ok):
    _pdf(tmp_path / "r.pdf", pages)
    passed, message = cs.pdf_pages(tmp_path / "r.pdf")
    assert passed is ok
    assert str(pages) in message


def test_references_are_not_counted():
    texts = ["intro"] + ["text"] * 12 + ["References\n[1] a"] + ["[2] b"] * 3
    assert cs.body_page_count(texts) == 13
    assert cs.body_page_count(["a", "b", "c"]) == 3


def test_a_mention_of_references_in_running_text_does_not_end_the_body():
    texts = ["intro"] + ["we list the references at the end"] * 4
    assert cs.body_page_count(texts) == 5


def test_a_missing_report_fails_and_names_the_path(tmp_path):
    ok, message = cs.pdf_pages(tmp_path / "nope.pdf")
    assert not ok and "nope.pdf" in message


def test_an_unreadable_pdf_is_a_failure_not_a_crash(tmp_path):
    (tmp_path / "bad.pdf").write_text("not a pdf")
    ok, message = cs.pdf_pages(tmp_path / "bad.pdf")
    assert not ok and "cannot read" in message


# ----------------------------------------------------------------------- files
def test_large_files_are_named_and_small_ones_pass(tmp_path):
    (tmp_path / "big.bin").write_bytes(b"x" * 60_000_000)
    (tmp_path / "small.bin").write_bytes(b"x" * 10_000_000)
    ok, message = cs.no_large_files(tmp_path)
    assert not ok and "big.bin" in message and "small.bin" not in message
    (tmp_path / "big.bin").unlink()
    assert cs.no_large_files(tmp_path)[0]


def test_git_and_virtualenv_folders_are_skipped_outside_a_repo(tmp_path):
    for folder in (".git", ".venv"):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "pack").write_bytes(b"x" * 60_000_000)
    assert cs.no_large_files(tmp_path)[0]


def test_only_tracked_files_count_inside_a_repo(tmp_path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "ok.txt").write_text("x")
    (tmp_path / "local.bin").write_bytes(b"x" * 60_000_000)  # untracked: local data, not hand-in
    subprocess.run(["git", "add", "ok.txt"], cwd=tmp_path, check=True)
    assert cs.no_large_files(tmp_path)[0]
    subprocess.run(["git", "add", "local.bin"], cwd=tmp_path, check=True)
    assert not cs.no_large_files(tmp_path)[0]


def test_required_paths_name_the_missing_one(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    ok, message = cs.required_paths(tmp_path, ["a.txt", "docs/report/main.pdf"])
    assert not ok and "docs/report/main.pdf" in message and "a.txt" not in message
    assert cs.required_paths(tmp_path, ["a.txt"])[0]


# ----------------------------------------------------------------------- links
class _Response(io.BytesIO):
    def __init__(self, status):
        super().__init__()
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _opener(statuses):
    def opener(request, timeout=0):
        status = statuses[request.full_url]
        if status == 404:
            raise urllib.error.HTTPError(request.full_url, 404, "not found", {}, None)
        if status == 0:
            raise urllib.error.URLError("no route")
        return _Response(status)

    return opener


def test_one_good_and_one_broken_link():
    statuses = {"https://ok.example": 200, "https://gone.example": 404}
    ok, message = cs.links_resolve(
        ["https://ok.example", "https://gone.example"], _opener(statuses)
    )
    assert not ok and "gone.example" in message and "404" in message and "ok.example" not in message


def test_all_good_links_pass():
    assert cs.links_resolve(["https://ok.example"], _opener({"https://ok.example": 200}))[0]


def test_unreachable_link_fails():
    ok, message = cs.links_resolve(["https://down.example"], _opener({"https://down.example": 0}))
    assert not ok and "down.example" in message


# ------------------------------------------------------------------- checklist
def _checklist(items, where="somewhere", extra=""):
    rows = [f"| {i} | {item} | {where} | planned |" for i, item in enumerate(items, start=1)]
    return "\n".join(["| # | Item | Where | Status |", "|---|---|---|---|", *rows]) + extra


def test_a_complete_checklist_passes():
    assert cs.checklist_mapped(_checklist(cs.CHECKLIST_ITEMS))[0]


def test_a_missing_item_is_named():
    ok, message = cs.checklist_mapped(_checklist(cs.CHECKLIST_ITEMS[:-1]))
    assert not ok and "Final report submitted" in message


def test_an_item_without_a_location_or_with_todo_fails():
    assert not cs.checklist_mapped(_checklist(cs.CHECKLIST_ITEMS, where=""))[0]
    assert not cs.checklist_mapped(_checklist(cs.CHECKLIST_ITEMS, where="TODO"))[0]


def test_the_real_checklist_is_complete():
    ok, message = cs.check_checklist(ROOT)
    assert ok, message


# ------------------------------------------------------------------------ main
def test_main_exits_one_when_any_check_fails(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cs, "ROOT", tmp_path)
    assert cs.main(["--skip-links"]) == 1
    out = capsys.readouterr().out
    assert "FAIL  report pages" in out and "FAIL  required files" in out


def test_main_exits_zero_when_everything_passes(tmp_path, monkeypatch):
    monkeypatch.setattr(cs, "ROOT", tmp_path)
    for relative in cs.REQUIRED_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
    _pdf(tmp_path / "docs" / "report" / "main.pdf", 12)
    (tmp_path / cs.CHECKLIST).write_text(_checklist(cs.CHECKLIST_ITEMS))
    assert cs.main(["--skip-links"]) == 0


@pytest.mark.parametrize(
    ("heading", "ends_body"),
    [
        ("References", True),
        ("10 References", True),
        ("Bibliography", True),
        ("Appendix B", True),
        ("References to earlier work are discussed in section 3.", False),
        ("Appendix tables are described below and continue", False),
    ],
)
def test_only_a_heading_line_ends_the_body(heading, ends_body):
    texts = ["intro"] + ["text"] * 11 + [f"{heading}\n[1] a"]
    assert cs.body_page_count(texts) == (12 if ends_body else 13)
