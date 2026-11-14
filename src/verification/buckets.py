"""Bucket-size profiling and big-bucket mitigation comparison for E7 (#30)."""

from __future__ import annotations

import argparse
import csv
import math
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.evaluation.evaluate import evaluate
from src.similarity.lsh import LSHConfig, find_candidates, pass_through_policy
from src.similarity.lsh import size_limit as _lsh_size_limit
from src.similarity.minhash import MinHashConfig, build_signatures
from src.similarity.shingles import ShingleConfig, build_shingles

DEFAULT_STRATEGIES: tuple[str, ...] = ("none", "size_limit", "drop_common_shingles")
COMPARISON_COLUMNS: tuple[str, ...] = (
    "strategy",
    "bucket_limit",
    "drop_common_share",
    "time_sec",
    "slowest_task_sec",
    "median_task_sec",
    "shuffle_bytes",
    "memory_bytes",
    "n_candidates",
    "recall@10",
    "lost_answer_pairs",
    "total_answer_pairs",
    "lost_answer_share",
)


@dataclass(frozen=True)
class BucketProfile:
    """Summary statistics for LSH bucket sizes and implied candidate pairs."""

    num_buckets: int
    total_items: int
    min_size: int
    max_size: int
    mean_size: float
    median_size: float
    p95_size: float
    p99_size: float
    total_pairs: int
    largest_bucket_pairs: int
    largest_bucket_share: float

    def to_dict(self) -> dict[str, int | float]:
        """Return profile fields as a plain dictionary."""
        return asdict(self)


def _quantile_from_sorted_counts(
    sorted_counts: list[tuple[int, int]],
    total_buckets: int,
    q: float,
) -> float:
    """Return linear-interpolated quantile ``q`` (0..1) from sorted ``(size, count)`` pairs."""
    if total_buckets == 0:
        return 0.0
    if total_buckets == 1:
        return float(sorted_counts[0][0])

    pos = q * (total_buckets - 1)
    lo_idx = int(math.floor(pos))
    hi_idx = int(math.ceil(pos))
    frac = pos - lo_idx

    def _value_at(target_idx: int) -> int:
        seen = 0
        for size, freq in sorted_counts:
            seen += freq
            if target_idx < seen:
                return size
        return sorted_counts[-1][0]

    lo_val = _value_at(lo_idx)
    if lo_idx == hi_idx:
        return float(lo_val)
    hi_val = _value_at(hi_idx)
    return float(lo_val + frac * (hi_val - lo_val))


def _profile_from_histogram(counts: dict[int, int]) -> BucketProfile:
    """Compute a ``BucketProfile`` from a ``{bucket_size: num_buckets}`` histogram."""
    filtered = [(int(size), int(freq)) for size, freq in counts.items() if freq > 0]
    if not filtered:
        return BucketProfile(
            num_buckets=0,
            total_items=0,
            min_size=0,
            max_size=0,
            mean_size=0.0,
            median_size=0.0,
            p95_size=0.0,
            p99_size=0.0,
            total_pairs=0,
            largest_bucket_pairs=0,
            largest_bucket_share=0.0,
        )

    sorted_counts = sorted(filtered, key=lambda pair: pair[0])
    num_buckets = sum(freq for _, freq in sorted_counts)
    total_items = sum(size * freq for size, freq in sorted_counts)
    min_size = sorted_counts[0][0]
    max_size = sorted_counts[-1][0]
    total_pairs = sum((size * (size - 1) // 2) * freq for size, freq in sorted_counts)
    largest_bucket_pairs = max_size * (max_size - 1) // 2
    largest_bucket_share = (largest_bucket_pairs / total_pairs) if total_pairs > 0 else 0.0

    return BucketProfile(
        num_buckets=num_buckets,
        total_items=total_items,
        min_size=min_size,
        max_size=max_size,
        mean_size=total_items / num_buckets,
        median_size=_quantile_from_sorted_counts(sorted_counts, num_buckets, 0.50),
        p95_size=_quantile_from_sorted_counts(sorted_counts, num_buckets, 0.95),
        p99_size=_quantile_from_sorted_counts(sorted_counts, num_buckets, 0.99),
        total_pairs=total_pairs,
        largest_bucket_pairs=largest_bucket_pairs,
        largest_bucket_share=largest_bucket_share,
    )


def profile_bucket_sizes(sizes: Iterable[int]) -> BucketProfile:
    """Profile an iterable of bucket sizes."""
    counts: Counter[int] = Counter()
    for size in sizes:
        if size < 0:
            raise ValueError(f"Bucket size must be non-negative, got {size}")
        counts[int(size)] += 1
    return _profile_from_histogram(counts)


def profile_buckets(
    df: DataFrame,
    band_col: str = "band",
    key_col: str = "bucket_key",
) -> BucketProfile:
    """Profile bucket sizes of a Contract Hook 1 bucket DataFrame."""
    hist_rows = (
        df.groupBy(band_col, key_col)
        .agg(F.count(F.lit(1)).alias("_bucket_size"))
        .groupBy("_bucket_size")
        .agg(F.count(F.lit(1)).alias("_num_buckets"))
        .collect()
    )
    counts = {int(row["_bucket_size"]): int(row["_num_buckets"]) for row in hist_rows}
    return _profile_from_histogram(counts)


def size_limit(max_size: int = 1000) -> Callable[[DataFrame], DataFrame]:
    """Return a Contract Hook 1 policy dropping buckets with more than ``max_size`` items."""
    if max_size < 1:
        raise ValueError(f"max_size must be at least 1, got {max_size}")
    return _lsh_size_limit(max_size)


def count_lost_answer_pairs(
    candidates: DataFrame,
    answer_key: DataFrame,
) -> tuple[int, int, float]:
    """Return ``(lost_pairs, total_pairs, lost_share)`` for missing answer-key pairs."""
    total_pairs = answer_key.count()
    if total_pairs == 0:
        return 0, 0, 0.0

    cand_edges = candidates.select(
        F.least(F.col("dup_id"), F.col("cand_id")).alias("_u"),
        F.greatest(F.col("dup_id"), F.col("cand_id")).alias("_v"),
    ).distinct()
    key_edges = answer_key.select(
        F.least(F.col("dup_id"), F.col("original_id")).alias("_u"),
        F.greatest(F.col("dup_id"), F.col("original_id")).alias("_v"),
    )
    found_pairs = key_edges.join(cand_edges, ["_u", "_v"], "left_semi").count()
    lost_pairs = total_pairs - found_pairs
    return lost_pairs, total_pairs, lost_pairs / total_pairs


def _build_strategy_spec(
    name: str,
    bucket_limit: int,
    drop_common_share: float,
) -> tuple[float | None, Callable[[DataFrame], DataFrame]]:
    """Return ``(drop_common_share, bucket_policy)`` for a named E7 strategy."""
    if name == "none":
        return None, pass_through_policy
    if name == "size_limit":
        return None, size_limit(bucket_limit)
    if name == "drop_common_shingles":
        return drop_common_share, pass_through_policy
    raise ValueError(f"Unknown E7 strategy {name!r}; expected one of {DEFAULT_STRATEGIES}")


def _write_comparison_csv(path: str | Path, rows: Sequence[dict[str, object]]) -> None:
    """Write E7 strategy comparison rows to ``path``."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(COMPARISON_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col) for col in COMPARISON_COLUMNS})


def compare_bucket_strategies(
    posts: DataFrame,
    answer_key: DataFrame,
    strategies: Sequence[str] = DEFAULT_STRATEGIES,
    bucket_limit: int = 1000,
    drop_common_share: float = 0.05,
    shingle_k: int = 5,
    num_perm: int = 120,
    bands: int = 30,
    rows_per_band: int = 4,
    output_path: str | Path | None = None,
) -> list[dict[str, object]]:
    """Run and compare E7 big-bucket strategies on candidate count, Recall@10, and lost pairs."""
    n_questions = posts.count()
    results: list[dict[str, object]] = []

    for strategy in strategies:
        share_cfg, policy = _build_strategy_spec(strategy, bucket_limit, drop_common_share)
        shingle_cfg = ShingleConfig(k=shingle_k, drop_common_share=share_cfg)
        mh_cfg = MinHashConfig(n=num_perm)
        lsh_cfg = LSHConfig(b=bands, r=rows_per_band, n=num_perm)

        t0 = time.perf_counter()
        shingled = build_shingles(posts, config=shingle_cfg)
        sigs = build_signatures(shingled, config=mh_cfg)
        extra_cols = [c for c in ("created", "group") if c in posts.columns]
        if extra_cols:
            sigs = sigs.join(posts.select("post_id", *extra_cols), "post_id", "left")
        candidates = find_candidates(sigs, config=lsh_cfg, bucket_policy=policy)
        metrics = evaluate(candidates, answer_key, n_questions=n_questions)
        lost_pairs, total_pairs, lost_share = count_lost_answer_pairs(candidates, answer_key)
        elapsed = time.perf_counter() - t0

        results.append(
            {
                "strategy": strategy,
                "bucket_limit": bucket_limit if strategy == "size_limit" else None,
                "drop_common_share": share_cfg,
                "time_sec": round(elapsed, 4),
                "slowest_task_sec": None,
                "median_task_sec": None,
                "shuffle_bytes": None,
                "memory_bytes": None,
                "n_candidates": int(metrics["n_candidates"]),
                "recall@10": round(float(metrics["recall@10"]), 6),
                "lost_answer_pairs": lost_pairs,
                "total_answer_pairs": total_pairs,
                "lost_answer_share": round(lost_share, 6),
            }
        )

    if output_path is not None:
        _write_comparison_csv(output_path, results)

    return results


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for E7 big-bucket comparison."""
    parser = argparse.ArgumentParser(description="Profile and compare E7 big-bucket strategies.")
    parser.add_argument("--posts", required=True, help="Parquet path for C1 posts.")
    parser.add_argument("--answer-key", required=True, help="Parquet path for C6 answer key.")
    parser.add_argument(
        "--output",
        default="results/e7_buckets/bucket_comparison.csv",
        help="Output CSV path for strategy comparison.",
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=list(DEFAULT_STRATEGIES),
        choices=list(DEFAULT_STRATEGIES),
        help="Strategies to compare.",
    )
    parser.add_argument("--bucket-limit", type=int, default=1000, help="Max bucket size.")
    parser.add_argument(
        "--drop-common-share",
        type=float,
        default=0.05,
        help="Shingle document-frequency cutoff for drop_common_shingles.",
    )
    parser.add_argument("--shingle-k", type=int, default=5, help="Shingle k-gram length.")
    parser.add_argument("--num-perm", type=int, default=120, help="MinHash signature length.")
    parser.add_argument("--bands", type=int, default=30, help="LSH band count.")
    parser.add_argument("--rows", type=int, default=4, help="LSH rows per band.")
    parser.add_argument("--master", default=None, help="Optional Spark master URL.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for E7 big-bucket strategy comparison."""
    args = parse_args(argv)
    builder = SparkSession.builder.appName("DupHunter-E7-Buckets")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    try:
        posts = spark.read.parquet(args.posts)
        answer_key = spark.read.parquet(args.answer_key)
        compare_bucket_strategies(
            posts=posts,
            answer_key=answer_key,
            strategies=args.strategies,
            bucket_limit=args.bucket_limit,
            drop_common_share=args.drop_common_share,
            shingle_k=args.shingle_k,
            num_perm=args.num_perm,
            bands=args.bands,
            rows_per_band=args.rows,
            output_path=args.output,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
