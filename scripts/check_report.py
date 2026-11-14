"""Static checks for the DupHunter LaTeX report (#24)."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path

_INPUT_RE = re.compile(r"\\(?:input|include)\{([^}]+)\}")
_CITE_RE = re.compile(r"\\cite[pt]?\{([^}]+)\}")
_BIB_KEY_RE = re.compile(r"@\w+\s*\{\s*([^,\s]+)\s*,")
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_\\.-])\d+(?:,\d{3})*(?:\.\d+)?%?")
_STRIP_MATH_RE = re.compile(r"\$[^$]*\$")
_STRIP_CMD_ARG_RE = re.compile(
    r"\\(?:section|subsection|paragraph|label|ref|cite[pt]?|input|include|includegraphics|graphicspath|usepackage|documentclass|author|date|title)\*?(?:\[[^\]]*\])?\{[^}]*\}"
)


def extract_inputs(tex_text: str, *, include_conditional: bool = True) -> list[str]:
    """Return ``\\input`` / ``\\include`` targets found in ``tex_text``."""
    targets: list[str] = []
    for line in tex_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("%"):
            continue
        if not include_conditional and stripped.startswith(r"\IfFileExists"):
            continue
        for match in _INPUT_RE.finditer(line):
            targets.append(match.group(1).strip())
    return targets


def _strip_comments(tex_text: str) -> str:
    """Strip unescaped ``%`` LaTeX comments from ``tex_text``."""
    cleaned_lines = [re.split(r"(?<!\\)%", line, maxsplit=1)[0] for line in tex_text.splitlines()]
    return "\n".join(cleaned_lines)


def extract_citations(tex_text: str) -> set[str]:
    """Return all citation keys referenced via ``\\cite`` commands."""
    keys: set[str] = set()
    for match in _CITE_RE.finditer(_strip_comments(tex_text)):
        for raw_key in match.group(1).split(","):
            key = raw_key.strip()
            if key:
                keys.add(key)
    return keys


def extract_bib_keys(bib_text: str) -> set[str]:
    """Return all entry keys defined in a BibTeX file."""
    return {match.group(1).strip() for match in _BIB_KEY_RE.finditer(bib_text)}


def resolve_tex_input(base_dir: Path, target: str) -> Path:
    """Resolve a LaTeX ``\\input`` target relative to ``base_dir``."""
    candidate = base_dir / target
    if candidate.exists():
        return candidate
    if not target.endswith(".tex"):
        with_ext = base_dir / f"{target}.tex"
        if with_ext.exists():
            return with_ext
    return candidate


def find_missing_inputs(main_tex: Path) -> list[str]:
    """Return any ``\\input`` / ``\\include`` targets in ``main_tex`` that do not exist."""
    text = main_tex.read_text(encoding="utf-8")
    missing: list[str] = []
    for target in extract_inputs(text, include_conditional=False):
        resolved = resolve_tex_input(main_tex.parent, target)
        if not resolved.exists():
            missing.append(target)
    return missing


def find_missing_citations(tex_files: Iterable[Path], bib_path: Path) -> list[str]:
    """Return sorted citation keys used in ``tex_files`` that are missing from ``bib_path``."""
    bib_keys = extract_bib_keys(bib_path.read_text(encoding="utf-8"))
    cited: set[str] = set()
    for tex_path in tex_files:
        cited.update(extract_citations(tex_path.read_text(encoding="utf-8")))
    return sorted(cited - bib_keys)


def find_unsourced_numbers(tex_files: Iterable[Path]) -> list[tuple[Path, int, str]]:
    """Return lines with numeric values lacking a ``% source:`` or ``% TODO`` tag."""
    findings: list[tuple[Path, int, str]] = []
    for tex_path in tex_files:
        lines = tex_path.read_text(encoding="utf-8").splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped or stripped.startswith("%"):
                continue
            window = " ".join(lines[max(0, idx - 1) : min(len(lines), idx + 2)])
            if "% source:" in window or "% TODO" in window:
                continue
            cleaned = _STRIP_CMD_ARG_RE.sub(" ", _STRIP_MATH_RE.sub(" ", line))
            if _NUMBER_RE.search(cleaned):
                findings.append((tex_path, idx + 1, stripped))
    return findings


def collect_report_tex_files(main_tex: Path) -> list[Path]:
    """Return ``main_tex`` plus all existing ``\\input`` / ``\\include`` fragments."""
    files = [main_tex]
    text = main_tex.read_text(encoding="utf-8")
    for target in extract_inputs(text):
        resolved = resolve_tex_input(main_tex.parent, target)
        if resolved.exists() and resolved not in files:
            files.append(resolved)
    return files


def run_checks(
    main_tex: Path,
    bib_path: Path,
    *,
    strict_sources: bool = False,
) -> tuple[bool, list[str]]:
    """Run report checks and return ``(ok, messages)``."""
    messages: list[str] = []
    ok = True

    if not main_tex.exists():
        return False, [f"Missing main report file: {main_tex}"]
    if not bib_path.exists():
        return False, [f"Missing bibliography file: {bib_path}"]

    missing_inputs = find_missing_inputs(main_tex)
    if missing_inputs:
        ok = False
        messages.append(f"Missing \\input files: {missing_inputs}")

    tex_files = collect_report_tex_files(main_tex)
    missing_cites = find_missing_citations(tex_files, bib_path)
    if missing_cites:
        ok = False
        messages.append(f"Missing BibTeX keys in {bib_path.name}: {missing_cites}")

    unsourced = find_unsourced_numbers(tex_files)
    if unsourced:
        msg = f"{len(unsourced)} line(s) with numbers lack '% source:' or '% TODO'"
        messages.append(msg)
        if strict_sources:
            ok = False

    if ok and not messages:
        messages.append("Report checks passed.")
    return ok, messages


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for report checks."""
    parser = argparse.ArgumentParser(description="Validate DupHunter report inputs and citations.")
    parser.add_argument("--main", type=Path, default=Path("docs/report/main.tex"))
    parser.add_argument("--bib", type=Path, default=Path("docs/report/refs.bib"))
    parser.add_argument(
        "--strict-sources",
        action="store_true",
        help="Fail if any numeric line lacks a '% source:' or '% TODO' comment.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for ``scripts/check_report.py``."""
    args = parse_args(argv)
    ok, messages = run_checks(args.main, args.bib, strict_sources=args.strict_sources)
    for msg in messages:
        print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
