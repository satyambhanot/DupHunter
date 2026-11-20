"""Compute exact Jaccard pairs for the E5 baseline.

The sparse-block engine joins inverted shingle postings and aggregates their
overlap. The Spark-join engine compares each pair of question arrays directly.
Both return Contract C4 rows, with the exact Jaccard score in ``est_sim``.
"""

from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, ByteType, IntegerType, LongType, ShortType

_INTEGER_TYPES = (ByteType, ShortType, IntegerType, LongType)
_ENGINE_ALIASES = {
    "blocks": "sparse_blocks",
    "sparse_blocks": "sparse_blocks",
    "join": "spark_join",
    "spark_join": "spark_join",
}
_ENGINES = tuple(_ENGINE_ALIASES)
_TIMING_COLUMNS = (
    "sample_size",
    "engine",
    "block_size",
    "threshold",
    "elapsed_ms",
    "n_candidates",
)


def _prepare_posts(shingles_df: DataFrame) -> tuple[DataFrame, bool]:
    """Validate C2 input and normalize each question's shingle set."""
    missing = {"post_id", "shingles"}.difference(shingles_df.columns)
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {sorted(missing)}")

    if not isinstance(shingles_df.schema["post_id"].dataType, _INTEGER_TYPES):
        raise ValueError("post_id must use an integer Spark type")
    shingle_type = shingles_df.schema["shingles"].dataType
    if not isinstance(shingle_type, ArrayType):
        raise ValueError("shingles must be an array column")

    has_created = "created" in shingles_df.columns
    empty_array = F.array().cast(shingle_type)
    columns = [
        F.col("post_id"),
        F.coalesce(F.array_distinct(F.col("shingles")), empty_array).alias("_shingles"),
    ]
    if has_created:
        columns.append(F.col("created").alias("_created"))
    return shingles_df.select(*columns), has_created


def _shared_shingles_by_blocks(posts: DataFrame, block_size: int) -> DataFrame:
    """Count shared shingles after assigning each candidate pair to an ID block."""
    postings = posts.select(
        F.col("post_id"),
        F.floor(F.col("post_id") / F.lit(block_size)).cast("long").alias("_block"),
        F.explode("_shingles").alias("_shingle"),
    )
    left = postings.alias("left")
    right = postings.alias("right")
    matches = left.join(
        right,
        (F.col("left._shingle") == F.col("right._shingle"))
        & (F.col("left.post_id") < F.col("right.post_id")),
        "inner",
    ).select(
        F.col("left.post_id").alias("_left_id"),
        F.col("right.post_id").alias("_right_id"),
        F.col("left._block").alias("_left_block"),
        F.col("right._block").alias("_right_block"),
        F.col("left._shingle").alias("_shingle"),
    )
    # _prepare_posts applies array_distinct before exploding, so each question
    # contributes one posting per shingle. Every matching posting pair therefore
    # contributes one distinct member to the pair's intersection.
    return matches.groupBy("_left_block", "_right_block", "_left_id", "_right_id").agg(
        F.count("_shingle").alias("_intersection")
    )


def _shared_shingles_by_join(posts: DataFrame) -> DataFrame:
    """Count shared shingles with a direct Spark self-join on question IDs."""
    left = posts.alias("left")
    right = posts.alias("right")
    intersection = F.array_intersect(F.col("left._shingles"), F.col("right._shingles"))
    return (
        left.join(right, F.col("left.post_id") < F.col("right.post_id"), "inner")
        .select(
            F.col("left.post_id").alias("_left_id"),
            F.col("right.post_id").alias("_right_id"),
            F.size(intersection).alias("_intersection"),
        )
        .filter(F.col("_intersection") > 0)
    )


def _score_pairs(
    shared: DataFrame,
    posts: DataFrame,
    threshold: float,
    has_created: bool,
) -> DataFrame:
    """Add C4 pair direction and exact Jaccard scores to shared-shingle counts."""
    left_sizes = posts.select(
        F.col("post_id").alias("_left_id"),
        F.size("_shingles").alias("_left_size"),
        *([F.col("_created").alias("_left_created")] if has_created else []),
    )
    right_sizes = posts.select(
        F.col("post_id").alias("_right_id"),
        F.size("_shingles").alias("_right_size"),
        *([F.col("_created").alias("_right_created")] if has_created else []),
    )
    pairs = shared.join(left_sizes, "_left_id").join(right_sizes, "_right_id")

    if has_created:
        left_is_newer = (F.col("_left_created") > F.col("_right_created")) | (
            (F.col("_left_created") == F.col("_right_created"))
            & (F.col("_left_id") > F.col("_right_id"))
        )
    else:
        left_is_newer = F.col("_left_id") > F.col("_right_id")

    union_size = F.col("_left_size") + F.col("_right_size") - F.col("_intersection")
    return pairs.select(
        F.when(left_is_newer, F.col("_left_id")).otherwise(F.col("_right_id")).alias("dup_id"),
        F.when(left_is_newer, F.col("_right_id")).otherwise(F.col("_left_id")).alias("cand_id"),
        (F.col("_intersection") / union_size).cast("double").alias("est_sim"),
    ).filter(F.col("est_sim") >= F.lit(threshold))


def exact_jaccard_pairs(
    shingles_df: DataFrame,
    threshold: float,
    engine: str = "blocks",
    block_size: int = 10_000,
) -> DataFrame:
    """Return exact Jaccard pairs that meet ``threshold``.

    Args:
        shingles_df: C2 DataFrame with integer ``post_id``, array ``shingles``,
            and optional ``created`` columns.
        threshold: Minimum exact Jaccard similarity, inclusive, from 0 to 1.
        engine: ``"blocks"`` (or ``"sparse_blocks"``) joins inverted postings;
            ``"join"`` (or ``"spark_join"``) compares question arrays directly.
        block_size: Positive question-ID range used to organize pairs with the
            sparse-block engine. Changing it does not change the pairs or scores.

    Returns:
        A DataFrame matching Contract C4: ``dup_id`` (newer question),
        ``cand_id`` (older question), and ``est_sim`` (exact Jaccard similarity).

    Raises:
        ValueError: If the threshold, engine, block size, or input schema is invalid.
    """
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
        raise ValueError("threshold must be between 0 and 1")
    if engine not in _ENGINES:
        raise ValueError(f"engine must be one of {_ENGINES}, got {engine!r}")
    if block_size < 1:
        raise ValueError("block_size must be greater than zero")

    posts, has_created = _prepare_posts(shingles_df)
    engine = _ENGINE_ALIASES[engine]
    if engine == "sparse_blocks":
        shared = _shared_shingles_by_blocks(posts, block_size)
    else:
        shared = _shared_shingles_by_join(posts)
    return _score_pairs(shared, posts, threshold, has_created)


def _write_timing(
    path: Path,
    sample_size: int,
    engine: str,
    block_size: int,
    threshold: float,
    elapsed_ms: int,
    n_candidates: int,
) -> None:
    """Append one completed run to a CSV timing log."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_TIMING_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "sample_size": sample_size,
                "engine": engine,
                "block_size": block_size,
                "threshold": threshold,
                "elapsed_ms": elapsed_ms,
                "n_candidates": n_candidates,
            }
        )


def _plot_timings(timing_path: Path, output_path: Path) -> None:
    """Plot elapsed time against nested sample size from a timing CSV."""
    import matplotlib.pyplot as plt

    rows: dict[tuple[str, str], list[tuple[int, int]]] = {}
    with timing_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row["engine"], row["block_size"])
            rows.setdefault(key, []).append((int(row["sample_size"]), int(row["elapsed_ms"])))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    for (engine, block_size), values in sorted(rows.items()):
        values.sort()
        plt.plot(
            [size for size, _ in values],
            [elapsed for _, elapsed in values],
            marker="o",
            label=f"{engine}, block size {block_size}",
        )
    plt.xlabel("Questions in nested sample")
    plt.ylabel("Elapsed time (ms)")
    plt.title("Exact Jaccard baseline runtime")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse options for running the exact-baseline experiment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="C2 shingles Parquet path")
    parser.add_argument("--output", required=True, help="C4 exact-pairs Parquet path")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument("--threshold", type=float, default=0.1)
    parser.add_argument("--engine", choices=_ENGINES, default="sparse_blocks")
    parser.add_argument("--block-size", type=int, default=10_000)
    parser.add_argument(
        "--sample-size",
        type=int,
        help="take the lowest post IDs for a deterministic nested sample",
    )
    parser.add_argument("--timing-output", type=Path, help="append run metrics to this CSV")
    parser.add_argument("--plot-output", type=Path, help="save a plot from --timing-output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one exact-baseline sample and optionally record and plot its runtime."""
    args = parse_args(argv)
    if args.sample_size is not None and args.sample_size < 1:
        raise ValueError("sample_size must be greater than zero")
    if args.plot_output is not None and args.timing_output is None:
        raise ValueError("--plot-output requires --timing-output")

    spark = (
        SparkSession.builder.master(args.master).appName("duphunter-exact-baseline").getOrCreate()
    )
    try:
        posts = spark.read.parquet(args.input)
        if args.sample_size is not None:
            posts = posts.orderBy("post_id").limit(args.sample_size)
        sample_size = posts.count()

        started = time.perf_counter()
        pairs = exact_jaccard_pairs(
            posts,
            threshold=args.threshold,
            engine=args.engine,
            block_size=args.block_size,
        )
        n_candidates = pairs.count()
        pairs.write.mode("overwrite").parquet(args.output)
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        print(
            f"{args.engine}: {sample_size:,} questions, {n_candidates:,} pairs, {elapsed_ms:,} ms"
        )

        if args.timing_output is not None:
            _write_timing(
                args.timing_output,
                sample_size,
                args.engine,
                args.block_size,
                args.threshold,
                elapsed_ms,
                n_candidates,
            )
        if args.plot_output is not None:
            _plot_timings(args.timing_output, args.plot_output)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
