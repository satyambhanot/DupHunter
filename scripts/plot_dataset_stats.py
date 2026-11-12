#!/usr/bin/env python3
"""Plot the questions per year of one site from ``results/dataset_stats.md`` (profile output).

Usage (from the repository root)::

    python scripts/plot_dataset_stats.py --site "Super User" \\
        --out results/dataset/questions_per_year.png
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def questions_per_year(markdown: str, site: str) -> dict[int, int]:
    """The ``Year | Questions`` table of ``site`` in the profile file, as ``{year: count}``.

    Raises:
        KeyError: if the site has no section in the file.
    """
    match = re.search(
        rf"<!-- site:{re.escape(site)} -->(.*?)<!-- end:{re.escape(site)} -->", markdown, re.S
    )
    if not match:
        raise KeyError(f"no section for {site!r} in the statistics file")
    rows = re.findall(r"^\|\s*(\d{4})\s*\|\s*([\d,]+)\s*\|\s*$", match.group(1), re.M)
    return {int(year): int(count.replace(",", "")) for year, count in rows}


def plot(per_year: dict[int, int], site: str, path: Path) -> None:
    """Draw the bars and save the figure (a drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.bar([str(y) for y in per_year], list(per_year.values()), color="#3b6ea5")
    ax.set_ylabel("Questions")
    ax.set_title(f"{site}: questions per year")
    plt.setp(ax.get_xticklabels(), rotation=60, ha="right")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stats", default=str(ROOT / "results" / "dataset_stats.md"))
    parser.add_argument("--site", required=True, help="section title in the statistics file")
    parser.add_argument("--out", required=True, help="PNG to write")
    args = parser.parse_args(argv)
    per_year = questions_per_year(Path(args.stats).read_text(), args.site)
    plot(per_year, args.site, Path(args.out))
    print(f"{len(per_year)} years plotted to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
