#!/usr/bin/env python3
"""Check the final hand-in: the guideline's Section 16 checklist as checks a script can run.

Usage (from the repository root)::

    python scripts/check_submission.py                     # every check
    python scripts/check_submission.py --skip-links        # offline
    python scripts/check_submission.py --report docs/report/main.pdf --url https://github.com/<owner>/<repo>

Each check is a small function that returns ``(ok, message)``; ``main`` runs them all, prints one
line per check and exits with 1 if any failed. A new check is a new function in ``run_checks``;
the others are untouched.
"""

import argparse
import re
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIN_PAGES, MAX_PAGES = 10, 15
MAX_FILE_MB = 50
DEFAULT_REPORT = "docs/report/main.pdf"
DEFAULT_URLS = ["https://github.com/satyambhanot/DupHunter"]
CHECKLIST = "docs/submission_checklist.md"
REQUIRED_PATHS = [
    "README.md",
    "requirements.txt",
    "docs/report/main.pdf",
    "docs/presentation/deck.pdf",
    "docs/CONTRACTS.md",
    "results/dataset_stats.md",
    "results/metrics.csv",
    CHECKLIST,
]
# The 17 items of Section 16 of the project guidelines.
CHECKLIST_ITEMS = [
    "Problem clearly defined",
    "Motivation and application described",
    "Dataset identified and described",
    "Data preprocessing explained",
    "Selected algorithms explained",
    "Technical and system design details provided",
    "Proposed approach implemented",
    "Meaningful experiments conducted",
    "Appropriate evaluation metrics reported",
    "Relevant methods or baselines compared",
    "Computational cost and scalability analyzed",
    "Limitations discussed",
    "Individual contributions reported",
    "Appropriate references included",
    "Source code and run instructions included or linked",
    "Presentation prepared",
    "Final report submitted in the required format",
]
_END_OF_BODY = re.compile(
    r"^\s*(references|bibliography|appendix|appendices)\b", re.IGNORECASE | re.MULTILINE
)

Result = tuple[bool, str]


def body_page_count(page_texts: list[str]) -> int:
    """Pages before the first page that starts the references or an appendix (all pages if none)."""
    for number, text in enumerate(page_texts):
        # a heading line, not a mention in the running text; the first page is never the end
        if number > 0 and _END_OF_BODY.search(text[:300]):
            return number
    return len(page_texts)


def pdf_page_texts(path: Path) -> list[str]:
    """The text of every page of a PDF."""
    from pypdf import PdfReader

    return [page.extract_text() or "" for page in PdfReader(str(path)).pages]


def pdf_pages(path: Path, lo: int = MIN_PAGES, hi: int = MAX_PAGES) -> Result:
    """The report body (without references and appendices) has between ``lo`` and ``hi`` pages."""
    if not path.exists():
        return False, f"report PDF {path} is missing"
    try:
        body = body_page_count(pdf_page_texts(path))
    except Exception as error:  # an unreadable PDF is a failed check, not a crash
        return False, f"cannot read {path}: {error}"
    if not lo <= body <= hi:
        return False, f"report body has {body} pages, expected {lo} to {hi} (references excluded)"
    return True, f"report body has {body} pages"


def hand_in_files(root: Path) -> list[Path]:
    """The files that go into the hand-in: what git tracks, or every file if ``root`` is not a repo.

    Untracked files (local data, virtual environments) are not part of the hand-in, so they are
    skipped.
    """
    try:
        out = subprocess.run(
            ["git", "ls-files"], cwd=root, capture_output=True, text=True, check=True
        ).stdout
        return [root / line for line in out.splitlines() if (root / line).is_file()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        skipped = {".git", ".venv", "venv", "__pycache__"}
        return [
            p
            for p in root.rglob("*")
            if p.is_file() and not skipped.intersection(p.relative_to(root).parts)
        ]


def no_large_files(root: Path, limit_mb: float = MAX_FILE_MB) -> Result:
    """No file of the hand-in is larger than ``limit_mb`` megabytes."""
    limit = limit_mb * 1_000_000
    big = [
        f"{p.relative_to(root)} ({p.stat().st_size / 1e6:.0f} MB)"
        for p in hand_in_files(root)
        if p.stat().st_size > limit
    ]
    if big:
        return False, f"files over {limit_mb:g} MB: " + ", ".join(big)
    return True, f"no file over {limit_mb:g} MB"


def required_paths(root: Path, paths: Iterable[str] = REQUIRED_PATHS) -> Result:
    """Every required file exists."""
    missing = [p for p in paths if not (root / p).exists()]
    if missing:
        return False, "missing: " + ", ".join(missing)
    return True, "all required files exist"


def links_resolve(urls: Iterable[str], opener: Callable = urllib.request.urlopen) -> Result:
    """Every URL answers with HTTP 200. ``opener`` is replaceable so tests need no network."""
    bad = []
    for url in urls:
        try:
            with opener(urllib.request.Request(url, method="HEAD"), timeout=15) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            bad.append(f"{url} ({error})")
            continue
        if status != 200:
            bad.append(f"{url} (HTTP {status})")
    if bad:
        return False, "links that do not resolve: " + ", ".join(bad)
    return True, "all links resolve"


def checklist_mapped(text: str) -> Result:
    """Each of the 17 items appears with a non-empty 'where' cell and nothing marked TODO."""
    problems = []
    for item in CHECKLIST_ITEMS:
        row = next((line for line in text.splitlines() if item.lower() in line.lower()), None)
        if row is None:
            problems.append(f"'{item}' is not in the checklist")
            continue
        cells = [c.strip() for c in row.strip().strip("|").split("|")]
        if len(cells) < 3 or not cells[2] or "todo" in row.lower():
            problems.append(f"'{item}' has no location yet")
    if problems:
        return False, "; ".join(problems)
    return True, "all 17 checklist items point to a location"


def check_checklist(root: Path) -> Result:
    """The checklist file exists and maps every item."""
    path = root / CHECKLIST
    if not path.exists():
        return False, f"{CHECKLIST} is missing"
    return checklist_mapped(path.read_text())


def run_checks(
    root: Path, report: str, urls: list[str], skip_links: bool = False
) -> list[tuple[str, Result]]:
    """Run every check and return ``(name, (ok, message))`` in a fixed order."""
    checks: list[tuple[str, Callable[[], Result]]] = [
        ("report pages", lambda: pdf_pages(root / report)),
        ("file sizes", lambda: no_large_files(root)),
        ("required files", lambda: required_paths(root)),
        ("checklist", lambda: check_checklist(root)),
    ]
    if not skip_links:
        checks.append(("links", lambda: links_resolve(urls)))
    return [(name, check()) for name, check in checks]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--report", default=DEFAULT_REPORT, help="the report PDF")
    parser.add_argument("--url", action="append", help="a link that must resolve (repeatable)")
    parser.add_argument("--skip-links", action="store_true", help="do not call the network")
    args = parser.parse_args(argv)
    results = run_checks(ROOT, args.report, args.url or DEFAULT_URLS, args.skip_links)
    for name, (ok, message) in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {message}")
    failed = sum(not ok for _, (ok, _) in results)
    print("ready to submit" if not failed else f"{failed} check(s) failed", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
