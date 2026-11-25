"""Reproducible E5 benchmark of the two exact engines: ``python -m src.baselines.benchmark``.

For every sample size it takes the questions with the lowest ``post_id`` (so the samples are
nested), builds the E1 shingles (word 1-grams of the title) and runs both engines.

What is timed: building the pairs of one engine and materialising them once with ``count()`` on a
cached result. Building the shingles happens before the clock starts, and the digest that checks
that both engines agree is computed from the cached pairs after it stops.
"""

from __future__ import annotations

import argparse
import csv
import time
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.baselines.exact import _plot_timings, exact_jaccard_pairs
from src.similarity.shingles import ShingleConfig, build_shingles

ENGINES = ("sparse_blocks", "spark_join")
COLUMNS = ("sample_size", "engine", "block_size", "threshold", "elapsed_ms", "n_candidates",
           "digest")  # fmt: skip


def title_shingles(posts: DataFrame, sample_size: int) -> DataFrame:
    """E1 shingles (word 1-grams of the title) of the ``sample_size`` lowest post ids."""
    sample = posts.orderBy("post_id").limit(sample_size)
    shingled = build_shingles(sample.withColumn("body", F.lit("")), ShingleConfig(k=1))
    columns = ["post_id", "shingles"]
    if "created" in posts.columns:
        shingled = shingled.join(sample.select("post_id", "created"), "post_id")
        columns.append("created")
    return shingled.select(*columns)


def pairs_digest(pairs: DataFrame) -> str:
    """An order-independent fingerprint of C4 pairs: count, then sum, minimum and maximum hash.

    The score is rounded to 12 decimals, so the digest compares pairs and their similarity.
    """
    hashed = pairs.select(F.xxhash64("dup_id", "cand_id", F.round("est_sim", 12)).alias("h"))
    row = hashed.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("h").cast("decimal(38,0)")).alias("total"),
        F.min("h").alias("low"),
        F.max("h").alias("high"),
    ).collect()[0]
    return f"{row['n']}:{row['total']}:{row['low']}:{row['high']}"


def run_benchmark(
    posts: DataFrame,
    sample_sizes: Sequence[int],
    threshold: float = 0.1,
    block_size: int = 10_000,
) -> list[dict[str, object]]:
    """Time both engines on every sample size and check that they return the same pairs.

    Raises:
        RuntimeError: if the two engines disagree on any sample.
    """
    rows: list[dict[str, object]] = []
    for size in sample_sizes:
        if size < 1:
            raise ValueError("sample sizes must be positive")
        shingles = title_shingles(posts, size).cache()
        actual_size = shingles.count()
        digests = {}
        for engine in ENGINES:
            started = time.perf_counter()
            pairs = exact_jaccard_pairs(
                shingles, threshold=threshold, engine=engine, block_size=block_size
            ).cache()
            n_pairs = pairs.count()
            elapsed_ms = round((time.perf_counter() - started) * 1000)
            digests[engine] = pairs_digest(pairs)
            pairs.unpersist()
            rows.append(
                {
                    "sample_size": actual_size,
                    "engine": engine,
                    "block_size": block_size,
                    "threshold": threshold,
                    "elapsed_ms": elapsed_ms,
                    "n_candidates": n_pairs,
                    "digest": digests[engine],
                }
            )
        shingles.unpersist()
        if len(set(digests.values())) != 1:
            raise RuntimeError(f"the engines disagree on the sample of {size}: {digests}")
    return rows


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--posts", required=True, help="C1 posts_text Parquet (post_id, title, body)"
    )
    parser.add_argument("--sample-sizes", type=int, nargs="+", default=[10_000, 50_000])
    parser.add_argument("--threshold", type=float, default=0.1)
    parser.add_argument("--block-size", type=int, default=10_000)
    parser.add_argument("--out", required=True, help="directory for runs.csv and runtime.png")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.master(args.master).appName("duphunter-e5-exact").getOrCreate()
    try:
        rows = run_benchmark(
            spark.read.parquet(args.posts), args.sample_sizes, args.threshold, args.block_size
        )
    finally:
        spark.stop()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "runs.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    _plot_timings(out / "runs.csv", out / "runtime.png")
    for row in rows:
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
