"""E3 on real questions: choose the LSH band/row setting from measured and predicted S-curves.

Theory says a pair with Jaccard similarity ``s`` meets in at least one of ``b`` bands of ``r`` rows
with probability ``1 - (1 - s**r)**b`` (the S-curve). Two measurements on a sample of Super User
questions:

- **Measured S-curve.** For labelled duplicate pairs and random pairs, the exact Jaccard similarity
  (from the shingle sets) and whether the two MinHash signatures collide in some band. The share
  that collide, per similarity bin, is set against the formula.
- **Trade-off.** For every setting, LSH is run on the whole sample and the candidates are scored
  against the answer key: Recall@1, @5, @10, Precision@10, MAP, NDCG, the reduction ratio, the
  number of candidate pairs and the run time.

The text representation is the E1 choice (word 1-grams of the title) and the signature length is
the E2 choice (128)."""

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from src.evaluation import evaluate
from src.similarity.lsh import LSHConfig, find_candidates, s_curve
from src.similarity.minhash import MinHashConfig, build_signatures
from src.similarity.representation_study import jaccard, posts_of, shingles_for

OPTION = {"name": "word1_title", "representation": "word", "k": 1, "fields": "title"}
N = 128
SETTINGS = ((64, 2), (32, 4), (16, 8))
BIN_WIDTH = 0.1


def collides(sig_a: np.ndarray, sig_b: np.ndarray, b: int, r: int) -> bool:
    """Whether two signatures are equal in all ``r`` rows of at least one band."""
    if b * r != len(sig_a):
        raise ValueError(f"b * r = {b * r} does not match the signature length {len(sig_a)}")
    matches = (np.asarray(sig_a) == np.asarray(sig_b)).reshape(b, r)
    return bool(matches.all(axis=1).any())


def measured_curve(
    exact: list[float], hit: list[bool], b: int, r: int, width: float = BIN_WIDTH
) -> pd.DataFrame:
    """Share of pairs that collide per exact-similarity bin, and the theory at the bin middle.

    Columns: ``low``, ``high``, ``pairs``, ``measured`` (``NaN`` for an empty bin), ``predicted``.
    The last bin includes similarity 1.0.
    """
    exact_values, hits = np.asarray(exact), np.asarray(hit, dtype=bool)
    edges = np.round(np.arange(0.0, 1.0 + width / 2, width), 10)
    rows = []
    for i, (low, high) in enumerate(zip(edges[:-1], edges[1:], strict=True)):
        last = i == len(edges) - 2
        inside = (exact_values >= low) & ((exact_values <= high) if last else (exact_values < high))
        rows.append(
            {
                "low": low,
                "high": high,
                "pairs": int(inside.sum()),
                "measured": float(hits[inside].mean()) if inside.any() else float("nan"),
                "predicted": s_curve((low + high) / 2, b, r),
            }
        )
    return pd.DataFrame(rows)


def curve_error(curve: pd.DataFrame, min_pairs: int = 20) -> float:
    """Mean absolute difference of measured and predicted over bins with enough pairs."""
    usable = curve[curve["pairs"] >= min_pairs]
    return float((usable["measured"] - usable["predicted"]).abs().mean())


def plot_curves(curves: dict[tuple[int, int], pd.DataFrame], path: str | Path) -> None:
    """Measured points over the predicted S-curve for each setting (drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(
        1, len(curves), figsize=(4.2 * len(curves), 3.6), sharey=True, squeeze=False
    )
    grid = np.linspace(0, 1, 101)
    for ax, ((b, r), curve) in zip(axes[0], curves.items(), strict=True):
        ax.plot(grid, [s_curve(s, b, r) for s in grid], color="#999999", label="theory")
        usable = curve[curve["pairs"] >= 20]
        ax.plot(
            (usable["low"] + usable["high"]) / 2,
            usable["measured"],
            "o",
            color="#c0392b",
            label="measured",
        )
        ax.set_title(f"b={b}, r={r}", fontsize=10)
        ax.set_xlabel("exact Jaccard")
    axes[0][0].set_ylabel("share of pairs that collide")
    axes[0][0].legend()
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def build_sample(spark: SparkSession, pairs, posts, extra: int):
    """The questions of the pairs plus ``extra`` more, as ``(post_id, title, body)``."""
    base = posts_of(pairs)
    more = posts.select("post_id", "title", "body").join(
        base.select("post_id"), "post_id", "left_anti"
    )
    return base.union(more.limit(extra)).cache()


def run(spark: SparkSession, args: argparse.Namespace) -> tuple[pd.DataFrame, dict]:
    """Run both measurements; return the trade-off table and the S-curves per setting."""
    pairs = spark.read.parquet(args.pairs)
    sample = build_sample(spark, pairs, spark.read.parquet(args.posts), args.extra)
    n_sample = sample.count()
    shingles = shingles_for(sample, OPTION).cache()
    signatures = build_signatures(shingles, MinHashConfig(n=N, seed=42)).cache()
    sets = {row.post_id: set(row.shingles) for row in shingles.collect()}
    sigs = {row.post_id: np.asarray(row.signature) for row in signatures.collect()}

    key = spark.read.parquet(args.answer_key)
    ids = sample.select("post_id")
    key = key.join(ids.withColumnRenamed("post_id", "dup_id"), "dup_id", "left_semi")
    key = key.join(
        ids.withColumnRenamed("post_id", "original_id"), "original_id", "left_semi"
    ).cache()

    exact_pairs = pairs.select("post_id_a", "post_id_b").collect()
    exact = [jaccard(sets[r.post_id_a], sets[r.post_id_b]) for r in exact_pairs]
    curves, rows = {}, []
    for b, r in SETTINGS:
        hit = [collides(sigs[p.post_id_a], sigs[p.post_id_b], b, r) for p in exact_pairs]
        curves[(b, r)] = measured_curve(exact, hit, b, r)
        started = time.perf_counter()
        candidates = find_candidates(signatures, LSHConfig(b=b, r=r)).cache()
        n_candidates = candidates.count()
        seconds = time.perf_counter() - started
        metrics = evaluate.evaluate(candidates, key, n_questions=n_sample)
        candidates.unpersist()
        rows.append(
            {
                "b": b,
                "r": r,
                "threshold": (1 / b) ** (1 / r),
                "candidates": n_candidates,
                "seconds": seconds,
                "curve_error": curve_error(curves[(b, r)]),
                **{
                    k: metrics[k]
                    for k in ("recall@1", "recall@5", "recall@10", "precision@10", "map", "ndcg@10")
                },
                "reduction_ratio": metrics["reduction_ratio"],
                "queries": metrics["n_queries"],
                "questions": n_sample,
            }
        )
    return pd.DataFrame(rows), curves


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--pairs", required=True, help="the pairs_e1 export (Parquet)")
    parser.add_argument("--posts", required=True, help="more questions for the sample (Parquet)")
    parser.add_argument("--answer-key", required=True, help="the answer key (Parquet, contract C6)")
    parser.add_argument(
        "--extra", type=int, default=2000, help="questions added to those of the pairs"
    )
    parser.add_argument("--out", default="results/e3_lsh_real")
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run E3 on the real sample and write the tables and plots."""
    args = parse_args(argv)
    spark = (
        SparkSession.builder.master(args.master).appName("duphunter-e3")
        .config("spark.sql.shuffle.partitions", 16).getOrCreate()
    )  # fmt: skip
    table, curves = run(spark, args)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "tradeoff.csv", index=False, float_format="%.4f")
    for (b, r), curve in curves.items():
        curve.to_csv(out / f"s_curve_b{b}_r{r}.csv", index=False, float_format="%.4f")
    plot_curves(curves, out / "s_curves.png")
    print(table.round(4).to_string(index=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
