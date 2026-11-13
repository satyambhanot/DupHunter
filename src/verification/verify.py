"""Exact Jaccard verification and ranking comparison CLI (Experiment E4, #29).

Reads C4 candidate pairs ``(dup_id, cand_id, est_sim)`` and C2 shingles
``(post_id, shingles)``, re-scores with exact Jaccard, outputs C5 ranked
candidates ``(dup_id, cand_id, rank, score, method)``, and compares the
``estimated`` vs ``exact_jaccard`` rankings.

Example (run from repository root)::

    python -m src.verification.verify --input data/candidates --shingles data/shingles \\
        --output results/e4_verification/ranking_comparison.csv --method both --master local[*]
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.verification.rankers import RANKERS


def _read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read Parquet with a PyArrow fallback for Windows without ``winutils.exe``."""
    try:
        return spark.read.parquet(path)
    except Py4JJavaError:
        local = Path(path)
        if not local.exists():
            raise
        table = pq.read_table(path)
        return spark.createDataFrame(table.to_pylist())


def verify_candidates(
    candidates: DataFrame,
    shingles: DataFrame | None = None,
    method: str = "exact_jaccard",
    top_k: int = 10,
    min_jaccard: float | None = None,
    posts: DataFrame | None = None,
) -> DataFrame:
    """Rank C4 candidates using the named strategy in ``RANKERS``."""
    if method not in RANKERS:
        raise ValueError(f"Unknown ranking method '{method}'. Available: {sorted(RANKERS)}")
    if method == "tfidf":
        if posts is None and (
            shingles is None
            or not ({"title", "body"} <= set(shingles.columns) or "text" in shingles.columns)
        ):
            raise ValueError("method='tfidf' requires C1 posts text DataFrame (--posts)")
        return RANKERS[method].rank(
            candidates=candidates,
            shingles=shingles,
            top_k=top_k,
            min_jaccard=min_jaccard,
            posts=posts,
        )
    return RANKERS[method].rank(
        candidates=candidates,
        shingles=shingles,
        top_k=top_k,
        min_jaccard=min_jaccard,
    )


def _rank_metrics(ranked: DataFrame, answer_key: DataFrame | None) -> dict[str, float]:
    """Compute recall@1, recall@5, recall@10, and MRR when an answer key is supplied."""
    if answer_key is None:
        return {}
    key = answer_key.select(
        F.col("dup_id"),
        F.col("original_id").alias("cand_id"),
    ).distinct()
    n_queries = key.select("dup_id").distinct().count()
    if n_queries == 0:
        return {"recall_at_1": 0.0, "recall_at_5": 0.0, "recall_at_10": 0.0, "mrr": 0.0}

    hits = ranked.join(key, ["dup_id", "cand_id"], "inner")
    best_per_query = hits.groupBy("dup_id").agg(F.min("rank").alias("best_rank")).collect()

    r1 = sum(1 for row in best_per_query if row["best_rank"] <= 1) / n_queries
    r5 = sum(1 for row in best_per_query if row["best_rank"] <= 5) / n_queries
    r10 = sum(1 for row in best_per_query if row["best_rank"] <= 10) / n_queries
    mrr = sum(1.0 / row["best_rank"] for row in best_per_query) / n_queries
    return {
        "recall_at_1": round(r1, 6),
        "recall_at_5": round(r5, 6),
        "recall_at_10": round(r10, 6),
        "mrr": round(mrr, 6),
    }


def compare_rankings(
    candidates: DataFrame,
    shingles: DataFrame,
    top_k: int = 10,
    min_jaccard: float | None = None,
    answer_key: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame, list[dict[str, object]]]:
    """Run both ``estimated`` and ``exact_jaccard`` rankers and summarize differences."""
    est_df = verify_candidates(
        candidates, shingles=shingles, method="estimated", top_k=top_k, min_jaccard=min_jaccard
    ).cache()
    exact_df = verify_candidates(
        candidates, shingles=shingles, method="exact_jaccard", top_k=top_k, min_jaccard=min_jaccard
    ).cache()

    est_top1 = est_df.filter(F.col("rank") == 1).select("dup_id", F.col("cand_id").alias("e_top1"))
    exact_top1 = exact_df.filter(F.col("rank") == 1).select(
        "dup_id", F.col("cand_id").alias("x_top1")
    )
    top1_joined = est_top1.join(exact_top1, "dup_id", "inner")
    n_top1_queries = top1_joined.count()
    top1_agree = (
        top1_joined.filter(F.col("e_top1") == F.col("x_top1")).count() / n_top1_queries
        if n_top1_queries > 0
        else 0.0
    )

    rank_join = est_df.select("dup_id", "cand_id", F.col("rank").alias("e_rank")).join(
        exact_df.select("dup_id", "cand_id", F.col("rank").alias("x_rank")),
        ["dup_id", "cand_id"],
        "inner",
    )
    shift_row = rank_join.agg(
        F.avg(F.abs(F.col("e_rank") - F.col("x_rank"))).alias("mean_shift")
    ).collect()[0]
    mean_shift = float(shift_row["mean_shift"]) if shift_row["mean_shift"] is not None else 0.0

    rows: list[dict[str, object]] = []
    for method_name, df in [("estimated", est_df), ("exact_jaccard", exact_df)]:
        stats = df.agg(
            F.countDistinct("dup_id").alias("n_queries"),
            F.count("*").alias("n_ranked"),
            F.avg("score").alias("mean_score"),
        ).collect()[0]
        row: dict[str, object] = {
            "method": method_name,
            "n_queries": int(stats["n_queries"] or 0),
            "n_ranked": int(stats["n_ranked"] or 0),
            "mean_score": round(float(stats["mean_score"] or 0.0), 6),
            "top1_agreement_with_exact": round(
                1.0 if method_name == "exact_jaccard" else top1_agree, 6
            ),
            "mean_abs_rank_shift": round(0.0 if method_name == "exact_jaccard" else mean_shift, 6),
        }
        row.update(_rank_metrics(df, answer_key))
        rows.append(row)

    return est_df, exact_df, rows


def write_comparison_csv(rows: list[dict[str, object]], output_path: str | Path) -> Path:
    """Write ranking comparison summary rows to CSV."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out.write_text("", encoding="utf-8")
        return out
    fieldnames = list(rows[0].keys())
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for exact Jaccard verification."""
    parser = argparse.ArgumentParser(description="E4 exact Jaccard verification and ranking")
    parser.add_argument(
        "--input",
        "--candidates",
        dest="input",
        required=True,
        help="Path to C4 candidates Parquet (dup_id, cand_id, est_sim)",
    )
    parser.add_argument(
        "--shingles",
        required=True,
        help="Path to C2 shingles Parquet (post_id, shingles)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path (.csv for comparison table, or Parquet dir for C5 output)",
    )
    parser.add_argument(
        "--method",
        default="both",
        choices=[*sorted(RANKERS), "both"],
        help="Ranking strategy to run ('estimated', 'exact_jaccard', or 'both')",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Maximum ranked candidates to keep per question (default: 10)",
    )
    parser.add_argument(
        "--min-jaccard",
        type=float,
        default=None,
        help="Optional minimum exact Jaccard threshold to retain candidates",
    )
    parser.add_argument(
        "--answer-key",
        default=None,
        help="Optional C6 answer key Parquet for Recall@k and MRR comparison",
    )
    parser.add_argument(
        "--posts",
        default=None,
        help="Path to C1 posts_text Parquet (required when --method tfidf)",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    args = parser.parse_args(argv)
    if args.method == "tfidf" and not args.posts:
        parser.error("--posts is required when --method tfidf is used")
    return args


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for E4 verification."""
    args = parse_args(argv)
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = builder.master(args.master).appName("DupHunter-E4-Verification").getOrCreate()
    try:
        candidates = _read_parquet(spark, args.input)
        shingles = _read_parquet(spark, args.shingles)
        posts = _read_parquet(spark, args.posts) if args.posts else None
        answer_key = _read_parquet(spark, args.answer_key) if args.answer_key else None

        if args.method == "both" or args.output.lower().endswith(".csv"):
            _, exact_df, rows = compare_rankings(
                candidates=candidates,
                shingles=shingles,
                top_k=args.top_k,
                min_jaccard=args.min_jaccard,
                answer_key=answer_key,
            )
            csv_path = (
                Path(args.output)
                if args.output.lower().endswith(".csv")
                else Path(args.output) / "ranking_comparison.csv"
            )
            write_comparison_csv(rows, csv_path)
            if not args.output.lower().endswith(".csv"):
                exact_df.write.mode("overwrite").parquet(str(Path(args.output) / "ranked_c5"))
        else:
            ranked = verify_candidates(
                candidates=candidates,
                shingles=shingles,
                method=args.method,
                top_k=args.top_k,
                min_jaccard=args.min_jaccard,
                posts=posts,
            )
            ranked.write.mode("overwrite").parquet(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
