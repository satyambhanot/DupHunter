"""Error analysis: which duplicates does the system miss, and why?

The input is the per-pair table of ``src/evaluation/evaluate.py`` (contract C8), one row per
answer-key pair: ``dup_id, original_id, found, rank, exact_jaccard, tags, body_len, year``. It is
small (a few hundred thousand rows), so everything here is plain pandas. A pair is a *hit* when the
original is among the first ``k`` candidates of the newer question.

Theory says a pair with Jaccard similarity ``s`` becomes an LSH candidate with probability
``1 - (1 - s**r)**b``. Misses at low ``s`` are therefore expected; misses at high ``s`` are not.

Example (run from the repository root)::

    python -m src.evaluation.errors --per-pair data/per_pair --out results/error_analysis \\
        --bands 32 --rows 4
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

MIN_PAIRS = 20  # bins with fewer pairs are flagged: their recall is too noisy
TOP_K = 10


def lsh_probability(s: float, b: int, r: int) -> float:
    """Probability that a pair with Jaccard similarity ``s`` meets in one of ``b`` bands."""
    return 1.0 - (1.0 - s**r) ** b


HINTS = {
    "exact_jaccard": "run evaluate.py with --exact (the exact Jaccard of the pairs)",
    "tags": "run evaluate.py with --posts-text",
    "body_len": "run evaluate.py with --posts-text",
}


def _require(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        raise KeyError(f"the per-pair table has no column {column!r}")


def _require_values(df: pd.DataFrame, column: str) -> None:
    """The column exists and holds something: an all-null column means its input was not given."""
    _require(df, column)
    if df[column].isna().all():
        hint = HINTS.get(column, "check the input of evaluate.py")
        raise ValueError(f"column {column!r} is empty in the per-pair table; {hint}")


def hits(df: pd.DataFrame, k: int = TOP_K) -> pd.Series:
    """Boolean series: the original is among the first ``k`` candidates."""
    _require(df, "rank")
    return df["rank"].notna() & (df["rank"] <= k)


def _summarise(label, hit: pd.Series, min_pairs: int) -> dict:
    n = int(len(hit))
    return {
        "bin": label,
        "n_pairs": n,
        "recall": float(hit.mean()) if n else float("nan"),
        "low_n": n < min_pairs,
    }


def recall_by(
    df: pd.DataFrame,
    column: str,
    bins=None,
    k: int = TOP_K,
    min_pairs: int = MIN_PAIRS,
) -> pd.DataFrame:
    """Recall@k broken down by the values of one column.

    Args:
        df: the per-pair table.
        column: the column to break down by.
        bins: for a numeric column, the bin edges (the last bin includes its right edge); without
            ``bins`` every distinct value is a group (for example the year).
        k: a pair is a hit when its original is within the first ``k`` candidates.
        min_pairs: bins with fewer pairs get ``low_n = True``.

    Returns:
        One row per bin: ``bin``, ``n_pairs``, ``recall`` (``NaN`` for an empty bin) and ``low_n``.
        Pairs with a missing value in ``column`` are left out.
    """
    _require_values(df, column)
    hit = hits(df, k)
    values = df[column]
    if bins is None:
        rows = [
            _summarise(value, hit[values == value], min_pairs)
            for value in sorted(values.dropna().unique())
        ]
        return pd.DataFrame(rows, columns=["bin", "n_pairs", "recall", "low_n"])
    edges = list(bins)
    rows = []
    for i, (low, high) in enumerate(zip(edges[:-1], edges[1:], strict=True)):
        last = i == len(edges) - 2
        inside = (values >= low) & ((values <= high) if last else (values < high))
        rows.append(_summarise(f"{low:g}-{high:g}", hit[inside], min_pairs))
    return pd.DataFrame(rows, columns=["bin", "n_pairs", "recall", "low_n"])


def recall_by_similarity(
    df: pd.DataFrame, k: int = TOP_K, width: float = 0.1, b: int | None = None, r: int | None = None
) -> pd.DataFrame:
    """Recall by exact Jaccard similarity in bins of ``width``; with ``b`` and ``r`` it adds the
    ``expected`` probability of the LSH S-curve at the middle of each bin."""
    edges = np.round(np.arange(0.0, 1.0 + width / 2, width), 10)
    table = recall_by(df, "exact_jaccard", edges, k)
    if b is not None and r is not None:
        middles = (edges[:-1] + edges[1:]) / 2
        table["expected"] = [lsh_probability(s, b, r) for s in middles]
    return table


def recall_by_tag(df: pd.DataFrame, top: int = 20, k: int = TOP_K, min_pairs: int = MIN_PAIRS):
    """Recall for the ``top`` most frequent tags; a pair counts once for every tag it has."""
    _require_values(df, "tags")
    exploded = df.assign(tag=df["tags"].map(lambda tags: list(tags) if tags is not None else []))
    exploded = exploded.explode("tag").dropna(subset=["tag"])
    common = exploded["tag"].value_counts().head(top).index
    rows = []
    for tag in common:
        part = exploded[exploded["tag"] == tag]
        rows.append(_summarise(tag, hits(part, k), min_pairs))
    return pd.DataFrame(rows, columns=["bin", "n_pairs", "recall", "low_n"])


def recall_by_length(
    df: pd.DataFrame, edges=(0, 200, 500, 1000, 2000, 5000, 10**9), k: int = TOP_K
) -> pd.DataFrame:
    """Recall by the length of the newer question's body (characters)."""
    return recall_by(df, "body_len", edges, k)


def recall_by_year(df: pd.DataFrame, k: int = TOP_K) -> pd.DataFrame:
    """Recall by the year of the newer question, oldest first."""
    return recall_by(df, "year", None, k)


def sample_misses(df: pd.DataFrame, n: int = 20, k: int = TOP_K) -> pd.DataFrame:
    """The first ``n`` missed pairs ordered by ``dup_id``, to read by eye and group by reason."""
    return df[~hits(df, k)].sort_values(["dup_id", "original_id"]).head(n)


def plot_recall(table: pd.DataFrame, title: str, path: str | Path) -> None:
    """Draw recall per bin as bars, with the pair count above each bar (a drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    labels = [str(label) for label in table["bin"]]
    bars = ax.bar(
        labels, table["recall"].fillna(0.0), color=np.where(table["low_n"], "#bbbbbb", "#3b6ea5")
    )
    for bar, n in zip(bars, table["n_pairs"], strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"n={n}",
            ha="center",
            va="bottom",
            fontsize=8,
        )
    if "expected" in table:
        ax.plot(labels, table["expected"], "o--", color="#c0392b", label="LSH theory")
        ax.legend()
    ax.set_ylim(0, 1.1)
    ax.set_ylabel(f"Recall@{TOP_K}")
    ax.set_title(f"{title} (grey bars: fewer than {MIN_PAIRS} pairs)")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--per-pair", required=True, help="per-pair table (Parquet or CSV, contract C8)"
    )
    parser.add_argument(
        "--out", default="results/error_analysis", help="directory for tables and plots"
    )
    parser.add_argument("--bands", type=int, help="LSH bands b, to draw the theory curve")
    parser.add_argument("--rows", type=int, help="LSH rows per band r, to draw the theory curve")
    parser.add_argument("-k", type=int, default=TOP_K, help="a hit is a rank of at most k")
    parser.add_argument("--top-tags", type=int, default=20)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write the four breakdown tables, their plots and a sample of misses."""
    args = parse_args(argv)
    path = Path(args.per_pair)
    df = pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    builders = {
        "similarity": lambda: recall_by_similarity(df, args.k, 0.1, args.bands, args.rows),
        "tags": lambda: recall_by_tag(df, args.top_tags, args.k),
        "length": lambda: recall_by_length(df, k=args.k),
        "year": lambda: recall_by_year(df, args.k),
    }
    for name, build in builders.items():
        try:
            table = build()
        except ValueError as error:  # an input that evaluate.py was not given: skip this breakdown
            print(f"skipped the breakdown by {name}: {error}", file=sys.stderr)
            continue
        table.to_csv(out / f"recall_by_{name}.csv", index=False)
        plot_recall(table, f"Recall by {name}", out / f"recall_by_{name}.png")
    sample_misses(df, 20, args.k).to_csv(out / "misses.csv", index=False)
    print(
        f"overall recall@{args.k}: {hits(df, args.k).mean():.3f} over {len(df)} pairs",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
