#!/usr/bin/env python3
"""Check that the commands in the READMEs work: every ``python -m src.<module>`` must exist.

Usage (from the repository root)::

    python scripts/check_readme.py            # find the modules and run each with --help
    python scripts/check_readme.py --no-help  # only check that the modules exist

Exit code 0 when every documented module exists and answers ``--help``, 1 otherwise. The problems
are printed with the README and line they come from.
"""

import argparse
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMMAND = re.compile(r"python3? -m (src(?:\.\w+)+)")
SECTION = re.compile(r"our code vs\.? libraries", re.IGNORECASE)


def readmes(root: Path = ROOT) -> list[Path]:
    """The root README and every ``src/<module>/README.md``."""
    return [root / "README.md", *sorted((root / "src").glob("*/README.md"))]


def documented_modules(text: str) -> list[tuple[int, str]]:
    """``(line number, module)`` for every ``python -m src...`` command in a README."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        found.extend((number, module) for module in COMMAND.findall(line))
    return found


def check(root: Path = ROOT, run_help: bool = True) -> list[str]:
    """Return the problems found; an empty list means the READMEs are in order."""
    problems = []
    sys.path.insert(0, str(root))
    try:
        for readme in readmes(root):
            if not readme.exists():
                continue
            for number, module in documented_modules(readme.read_text()):
                where = f"{readme.relative_to(root)}:{number}"
                try:
                    exists = importlib.util.find_spec(module) is not None
                except ModuleNotFoundError:
                    exists = False
                if not exists:
                    problems.append(f"{where}: module {module} does not exist")
                elif run_help:
                    result = subprocess.run(
                        [sys.executable, "-m", module, "--help"],
                        cwd=root, capture_output=True, text=True,
                    )  # fmt: skip
                    if result.returncode != 0:
                        problems.append(
                            f"{where}: `python -m {module} --help` exited {result.returncode}"
                        )
    finally:
        sys.path.remove(str(root))
    return problems


def modules_without_section(root: Path = ROOT) -> list[str]:
    """Source folders with Python files whose README has no 'Our code vs. libraries' section."""
    missing = []
    for folder in sorted((root / "src").glob("*/")):
        if not list(folder.glob("*.py")):
            continue
        readme = folder / "README.md"
        if not readme.exists() or not SECTION.search(readme.read_text()):
            missing.append(str(folder.relative_to(root)))
    return missing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--no-help", action="store_true", help="only check that the modules exist")
    args = parser.parse_args(argv)
    problems = check(ROOT, run_help=not args.no_help)
    problems += [
        f"{m}/README.md: no 'Our code vs. libraries' section" for m in modules_without_section()
    ]
    for problem in problems:
        print(problem)
    print(
        "READMEs are in order" if not problems else f"{len(problems)} problem(s)", file=sys.stderr
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
