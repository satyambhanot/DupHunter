"""Safety checks for a public repository: no data, no large files and no student IDs in git."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_SUFFIXES = {".7z", ".parquet", ".xml", ".zip", ".gz"}
UCID = re.compile(r"\b303\d{5}\b")
BINARY_SUFFIXES = {".pdf", ".png", ".jpg"}
LIMIT_BYTES = 1_000_000


def tracked_files(root: Path = ROOT) -> list[Path]:
    """Every file git tracks in ``root``."""
    out = subprocess.run(
        ["git", "ls-files"], cwd=root, capture_output=True, text=True, check=True
    ).stdout
    return [root / line for line in out.splitlines()]


def find_data_files(files: list[Path], root: Path = ROOT) -> list[str]:
    """Files that look like data: a data suffix, or anything under ``data/`` except its README."""
    bad = []
    for path in files:
        relative = path.relative_to(root)
        in_data = relative.parts[0] == "data" and relative != Path("data/README.md")
        if path.suffix in DATA_SUFFIXES or in_data:
            bad.append(str(relative))
    return bad


def find_large_files(files: list[Path], root: Path = ROOT, limit: int = LIMIT_BYTES) -> list[str]:
    """Files larger than ``limit`` bytes."""
    return [str(p.relative_to(root)) for p in files if p.is_file() and p.stat().st_size > limit]


def find_ucids(files: list[Path], root: Path = ROOT) -> list[str]:
    """Text files that contain a student ID (a UCID such as 303xxxxx)."""
    hits = []
    for path in files:
        if not path.is_file() or path.suffix in BINARY_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if UCID.search(text):
            hits.append(str(path.relative_to(root)))
    return hits


# ---------------------------------------------------------------- the repository itself
def test_no_data_files_tracked():
    bad = find_data_files(tracked_files())
    assert not bad, f"Data files must not be committed: {bad}"


def test_no_large_files_tracked():
    bad = find_large_files(tracked_files())
    assert not bad, f"Files over {LIMIT_BYTES:,} bytes must not be committed: {bad}"


def test_no_ucids_tracked():
    hits = find_ucids(tracked_files())
    assert not hits, f"Student IDs must not be committed: {hits}"


# ----------------------------------------------------------------- the checks themselves
def test_a_large_file_is_found_and_named(tmp_path):
    big = tmp_path / "results" / "big.bin"
    big.parent.mkdir()
    big.write_bytes(b"x" * 2_000_000)
    small = tmp_path / "small.txt"
    small.write_text("ok")
    assert find_large_files([big, small], tmp_path) == ["results/big.bin"]


def test_a_file_under_data_is_flagged_but_its_readme_is_not(tmp_path):
    (tmp_path / "data").mkdir()
    files = []
    for name in ["data/README.md", "data/raw/posts.csv", "src/x.py", "dump.7z"]:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
        files.append(path)
    assert find_data_files(files, tmp_path) == ["data/raw/posts.csv", "dump.7z"]


def test_a_ucid_in_a_text_file_is_found(tmp_path):
    fake_id = "303" + "12345"  # built here so this file contains no ID itself
    notes = tmp_path / "notes.md"
    notes.write_text(f"Someone, {fake_id}")
    clean = tmp_path / "clean.md"
    clean.write_text("no ids here, only 12345 and 3033629")
    pdf = tmp_path / "slides.pdf"
    pdf.write_bytes(fake_id.encode())
    assert find_ucids([notes, clean, pdf], tmp_path) == ["notes.md"]
