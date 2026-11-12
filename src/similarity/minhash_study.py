"""E2: how long should a MinHash signature be? Accuracy against theory, and cost.

The share of equal positions of two signatures estimates the Jaccard similarity ``J`` of the two
sets, with standard deviation ``sqrt(J (1 - J) / n)`` for a signature of length ``n``. Two parts:

- **Accuracy.** For n = 32, 64, 128, 256 (the first n of one family of 256 hash functions, so the
  lengths are comparable) the estimate is compared with the exact Jaccard on (a) labelled
  duplicate and random pairs from the ``pairs_e1`` export and (b) synthetic set pairs at known
  similarities 0.1 to 0.9, where the spread of the estimate can be set against the formula.
- **Cost.** Time, size and peak memory of computing the signatures of a sample of questions."""

import argparse
import math
import random
import sys
import time
import tracemalloc
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.similarity.minhash import compute_signature
from src.similarity.representation_study import jaccard, posts_of, shingles_for

LENGTHS = (32, 64, 128, 256)
SEED = 42
DEFAULT_OPTION = {"name": "word1_title", "representation": "word", "k": 1, "fields": "title"}
SYNTHETIC_LEVELS = tuple(round(0.1 * i, 1) for i in range(1, 10))


def theoretical_std(j: float, n: int) -> float:
    """Standard deviation of the MinHash estimate of similarity ``j`` with ``n`` hashes."""
    return math.sqrt(j * (1.0 - j) / n)


def estimate(sig_a: np.ndarray, sig_b: np.ndarray) -> float:
    """Share of equal positions of two signatures (the Jaccard estimate)."""
    return float(np.mean(sig_a == sig_b))


def mean_absolute_error(estimates: list[float], exact: list[float]) -> float:
    """Mean of ``|estimate - exact|``."""
    return float(np.mean(np.abs(np.asarray(estimates) - np.asarray(exact))))


def synthetic_pairs(
    level: float, count: int, size: int = 40, seed: int = 0
) -> list[tuple[set, set]]:
    """``count`` pairs of ``size``-element integer sets with Jaccard ``level`` (up to rounding)."""
    rng = random.Random(seed)
    shared = round(2 * size * level / (1 + level))  # |A ∩ B| for |A| = |B| = size
    pairs = []
    for _ in range(count):
        universe = rng.sample(range(1, 2**30), 2 * size - shared)
        a = set(universe[:size])
        b = set(universe[:shared]) | set(universe[size : 2 * size - shared])
        pairs.append((a, b))
    return pairs


def accuracy_rows(
    pairs: list[tuple[str, set, set]], lengths=LENGTHS, seed: int = SEED
) -> pd.DataFrame:
    """One row per pair and length: the exact Jaccard and the estimate from the first ``n`` hashes.

    Args:
        pairs: ``(kind, set_a, set_b)``; ``kind`` is a label such as ``duplicate`` or ``0.3``.
        lengths: signature lengths; one family of ``max(lengths)`` functions is shared by all.
        seed: seed of the hash family.
    """
    longest = max(lengths)
    rows = []
    for kind, a, b in pairs:
        exact = jaccard(a, b)
        full_a = compute_signature(sorted(a), longest, seed)
        full_b = compute_signature(sorted(b), longest, seed)
        for n in lengths:
            rows.append(
                {"kind": kind, "n": n, "exact": exact, "estimate": estimate(full_a[:n], full_b[:n])}
            )
    return pd.DataFrame(rows)


def summarise_accuracy(rows: pd.DataFrame) -> pd.DataFrame:
    """Per kind and length: mean absolute error, bias, measured and theoretical spread."""
    out = []
    for (kind, n), part in rows.groupby(["kind", "n"], sort=False):
        error = part["estimate"] - part["exact"]
        mean_exact = float(part["exact"].mean())
        out.append(
            {
                "kind": kind,
                "n": n,
                "pairs": len(part),
                "mean_exact": mean_exact,
                "mae": float(error.abs().mean()),
                "bias": float(error.mean()),
                "std": float(error.std(ddof=0)),
                "theory_std": theoretical_std(mean_exact, n),
            }
        )
    return pd.DataFrame(out)


def cost_row(n: int, sets: list[list[int]], seed: int = SEED) -> dict:
    """Time, size and peak memory of the signatures of every set for one length ``n``.

    ``peak_mem_mb`` is the peak of Python memory while the signatures are computed (the working
    matrix of one question and the signatures kept so far); ``sig_bytes`` is the size of the
    signatures as 8-byte integers.
    """
    tracemalloc.start()
    started = time.perf_counter()
    signatures = [compute_signature(values, n, seed) for values in sets]
    seconds = time.perf_counter() - started
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return {
        "n": n,
        "questions": len(sets),
        "time_s": seconds,
        "sig_bytes": int(sum(s.nbytes for s in signatures)),
        "peak_mem_mb": peak / 1e6,
    }


def shingle_sets(spark: SparkSession, posts: DataFrame, option: dict) -> dict[int, set]:
    """``{post_id: set of shingle hashes}`` for the posts under one representation option."""
    shingled = shingles_for(posts, option).collect()
    return {row.post_id: set(row.shingles) for row in shingled}


def real_pairs(
    spark: SparkSession, pairs: DataFrame, per_kind: int, option: dict
) -> list[tuple[str, set, set]]:
    """Duplicate and random pairs of the ``pairs_e1`` export as ``(kind, set_a, set_b)``."""
    chosen = (
        pairs.filter("is_dup").limit(per_kind).union(pairs.filter(~F.col("is_dup")).limit(per_kind))
    )
    sets = shingle_sets(spark, posts_of(chosen), option)
    return [
        ("duplicate" if row.is_dup else "random", sets[row.post_id_a], sets[row.post_id_b])
        for row in chosen.collect()
    ]


def plot_accuracy(summary: pd.DataFrame, path: str | Path) -> None:
    """Spread of the estimate against ``sqrt(J(1-J)/n)`` on synthetic pairs (drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    synthetic = summary[summary["kind"].str.match(r"^0\.\d+$")]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    for n, part in synthetic.groupby("n"):
        axes[0].plot(part["mean_exact"], part["std"], "o-", label=f"measured, n={n}")
        axes[0].plot(part["mean_exact"], part["theory_std"], "--", color="grey", linewidth=0.8)
    axes[0].set_xlabel("exact Jaccard")
    axes[0].set_ylabel("standard deviation of the estimate")
    axes[0].set_title("Spread: measured (dots) and theory (dashed)")
    axes[0].legend(fontsize=7)
    real = summary[summary["kind"].isin(["duplicate", "random"])]
    for kind, part in real.groupby("kind"):
        axes[1].plot(part["n"], part["mae"], "o-", label=f"{kind} pairs")
    axes[1].set_xscale("log", base=2)
    axes[1].set_xlabel("signature length n")
    axes[1].set_ylabel("mean absolute error")
    axes[1].set_title("Error on labelled pairs")
    axes[1].legend()
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_cost(cost: pd.DataFrame, path: str | Path) -> None:
    """Time and size against ``n`` (drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(5, 3.5))
    ax.plot(cost["n"], cost["time_s"], "o-", color="#3b6ea5")
    ax.set_xscale("log", base=2)
    ax.set_xlabel("signature length n")
    ax.set_ylabel("time for the sample (s)")
    ax2 = ax.twinx()
    ax2.plot(cost["n"], cost["sig_bytes"] / 1e6, "s--", color="#c0392b")
    ax2.set_ylabel("signature size (MB)")
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--pairs", required=True, help="the pairs_e1 export (Parquet)")
    parser.add_argument(
        "--posts", required=True, help="posts for the cost part: Parquet with post_id, title, body"
    )
    parser.add_argument("--out", default="results/e2_signature")
    parser.add_argument(
        "--per-kind", type=int, default=1000, help="duplicate and random pairs each"
    )
    parser.add_argument(
        "--synthetic", type=int, default=200, help="synthetic pairs per similarity level"
    )
    parser.add_argument("--cost-questions", type=int, default=5000)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run both parts and write the tables and the plots."""
    args = parse_args(argv)
    spark = SparkSession.builder.master(args.master).appName("duphunter-e2").getOrCreate()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    pairs = real_pairs(spark, spark.read.parquet(args.pairs), args.per_kind, DEFAULT_OPTION)
    for level in SYNTHETIC_LEVELS:
        pairs += [
            (str(level), a, b)
            for a, b in synthetic_pairs(level, args.synthetic, seed=int(level * 10))
        ]
    summary = summarise_accuracy(accuracy_rows(pairs))
    summary.to_csv(out / "accuracy.csv", index=False, float_format="%.4f")
    plot_accuracy(summary, out / "accuracy.png")

    posts = (
        spark.read.parquet(args.posts).select("post_id", "title", "body").limit(args.cost_questions)
    )
    sets = [sorted(s) for s in shingle_sets(spark, posts, DEFAULT_OPTION).values()]
    cost = pd.DataFrame([cost_row(n, sets) for n in LENGTHS])
    cost.to_csv(out / "cost.csv", index=False, float_format="%.4f")
    plot_cost(cost, out / "cost.png")
    print(summary.round(4).to_string(index=False), file=sys.stderr)
    print(cost.round(3).to_string(index=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
