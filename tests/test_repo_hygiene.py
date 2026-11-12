"""Safety checks for a public repository: no data files and no student IDs in git."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_SUFFIXES = {".7z", ".parquet", ".xml", ".zip", ".gz"}
UCID = re.compile(r"\b303\d{5}\b")
BINARY_SUFFIXES = {".pdf", ".png", ".jpg"}


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    return [ROOT / line for line in out.splitlines()]


def test_no_data_files_tracked():
    bad = [str(p.relative_to(ROOT)) for p in tracked_files() if p.suffix in DATA_SUFFIXES]
    assert not bad, f"Data files must not be committed: {bad}"


def test_no_ucids_tracked():
    hits = []
    for path in tracked_files():
        if not path.is_file() or path.suffix in BINARY_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if UCID.search(text):
            hits.append(str(path.relative_to(ROOT)))
    assert not hits, f"Student IDs must not be committed: {hits}"
