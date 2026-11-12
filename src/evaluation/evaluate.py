"""Evaluate candidate pairs against the answer key: one row of metrics per run.

Inputs (contracts in docs/CONTRACTS.md):

- candidates: C4 ``(dup_id, cand_id, est_sim)`` or C5 ``(dup_id, cand_id, rank, score, method)``.
  C4 is ranked by ``est_sim`` (highest first), ties by ``cand_id`` ascending.
- answer key: C6 ``(dup_id, original_id, created)``.

Outputs: one row appended to ``results/metrics.csv`` (C7) and, optionally, a per-pair table (C8).

Example (run from the repository root)::

    python -m src.evaluation.evaluate --candidates data/candidates --answer-key data/answer_key \\
        --out results/metrics.csv --experiment E3 --settings '{"b": 32, "r": 4}' \\
        --n-questions 517000
"""

import argparse
import csv
import json
import math
import sys
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.evaluation.metrics import METRICS, mean_metrics, reduction_ratio

COLUMNS = ["experiment", "settings_json", *METRICS, "reduction_ratio", "n_candidates"]
DEFAULT_MAX_RANK = 100


def rank_candidates(candidates: DataFrame) -> DataFrame:
    """Return ``(dup_id, cand_id, rank)``; rank 1 is the best candidate of its question."""
    if "rank" in candidates.columns:
        return candidates.select("dup_id", "cand_id", F.col("rank").cast("long").alias("rank"))
    order = Window.partitionBy("dup_id").orderBy(F.col("est_sim").desc(), F.col("cand_id").asc())
    return candidates.select("dup_id", "cand_id", F.row_number().over(order).alias("rank"))


def evaluate(
    candidates: DataFrame,
    answer_key: DataFrame,
    n_questions: int | None = None,
    max_rank: int = DEFAULT_MAX_RANK,
) -> dict:
    """Compute the metrics of one run.

    Args:
        candidates: C4 or C5 candidate pairs for any number of questions.
        answer_key: C6 duplicate pairs; only its questions are evaluated.
        n_questions: number of questions searched, for the reduction ratio (``NaN`` if missing).
        max_rank: candidates ranked below this are ignored (they only affect MAP, slightly).

    Returns:
        The metric values (see ``METRICS``), ``reduction_ratio``, ``n_candidates`` and ``n_queries``
        (answer-key questions evaluated). Questions with no candidates count as zero.
    """
    n_candidates = candidates.count()
    relevant = answer_key.groupBy("dup_id").agg(F.collect_set("original_id").alias("relevant"))
    # Filter first: only the questions of the answer key are ranked, which keeps the window small.
    in_scope = candidates.join(relevant.select("dup_id"), "dup_id", "left_semi")
    ranked = rank_candidates(in_scope).filter(F.col("rank") <= max_rank)
    lists = ranked.groupBy("dup_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", "cand_id"))).alias("items")
    )
    rows = relevant.join(lists, "dup_id", "left").collect()
    pairs = [
        ([item["cand_id"] for item in (row["items"] or [])], set(row["relevant"])) for row in rows
    ]
    result = mean_metrics(pairs)
    result["reduction_ratio"] = (
        reduction_ratio(n_questions, n_candidates) if n_questions is not None else math.nan
    )
    result["n_candidates"] = n_candidates
    result["n_queries"] = len(pairs)
    return result


def per_pair_table(
    candidates: DataFrame,
    answer_key: DataFrame,
    posts_text: DataFrame | None = None,
    exact: DataFrame | None = None,
    max_rank: int = DEFAULT_MAX_RANK,
) -> DataFrame:
    """One row per answer-key pair for the error analysis (contract C8).

    Columns: ``dup_id, original_id, found, rank, exact_jaccard, tags, body_len, year``.
    ``rank`` is the rank of the original among the candidates of ``dup_id`` (null when it was
    not found). ``tags``, ``body_len`` and ``year`` describe the newer question and come from
    ``posts_text``; ``exact_jaccard`` comes from ``exact`` (``dup_id, cand_id, jaccard``).
    Missing inputs give null columns.
    """
    in_scope = candidates.join(answer_key.select("dup_id").distinct(), "dup_id", "left_semi")
    ranked = rank_candidates(in_scope).filter(F.col("rank") <= max_rank)
    table = answer_key.select("dup_id", "original_id", "created").join(
        ranked.withColumnRenamed("cand_id", "original_id"), ["dup_id", "original_id"], "left"
    )
    table = table.withColumn("found", F.col("rank").isNotNull())
    if exact is not None:
        table = table.join(
            exact.select(
                "dup_id",
                F.col("cand_id").alias("original_id"),
                F.col("jaccard").alias("exact_jaccard"),
            ),
            ["dup_id", "original_id"],
            "left",
        )
    else:
        table = table.withColumn("exact_jaccard", F.lit(None).cast("double"))
    if posts_text is not None:
        info = posts_text.select(
            F.col("post_id").alias("dup_id"), "tags", F.length("body").alias("body_len")
        )
        table = table.join(info, "dup_id", "left")
    else:
        table = table.withColumn("tags", F.lit(None).cast("array<string>")).withColumn(
            "body_len", F.lit(None).cast("int")
        )
    return table.select(
        "dup_id", "original_id", "found", "rank", "exact_jaccard", "tags", "body_len",
        F.year("created").alias("year"),
    )  # fmt: skip


def append_metrics(path: str | Path, experiment: str, settings: dict, metrics: dict) -> None:
    """Append one row to the metrics CSV (C7), writing the header if the file is new."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "experiment": experiment,
        "settings_json": json.dumps(settings, sort_keys=True),
        **{name: metrics[name] for name in [*METRICS, "reduction_ratio", "n_candidates"]},
    }
    new_file = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--candidates", required=True, help="candidates Parquet (C4 or C5)")
    parser.add_argument("--answer-key", required=True, help="answer key Parquet (C6)")
    parser.add_argument("--out", default="results/metrics.csv", help="metrics CSV to append to")
    parser.add_argument("--experiment", required=True, help="experiment id, for example E3")
    parser.add_argument("--settings", default="{}", help="JSON with the settings of this run")
    parser.add_argument(
        "--n-questions", type=int, help="questions searched, for the reduction ratio"
    )
    parser.add_argument(
        "--max-rank", type=int, default=DEFAULT_MAX_RANK, help="ignore ranks below this"
    )
    parser.add_argument("--per-pair-out", help="write the per-pair table (C8) here as Parquet")
    parser.add_argument("--posts-text", help="posts_text Parquet, for tags and body length in C8")
    parser.add_argument("--exact", help="exact Jaccard Parquet (dup_id, cand_id, jaccard), for C8")
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Evaluate one run, append its metrics row and optionally write the per-pair table."""
    from src.etl.spark_session import get_spark

    args = parse_args(argv)
    settings = json.loads(args.settings)
    spark = get_spark(args.master, "duphunter-evaluate")
    candidates = spark.read.parquet(args.candidates)
    answer_key = spark.read.parquet(args.answer_key)
    metrics = evaluate(candidates, answer_key, args.n_questions, args.max_rank)
    append_metrics(args.out, args.experiment, settings, metrics)
    if args.per_pair_out:
        table = per_pair_table(
            candidates,
            answer_key,
            spark.read.parquet(args.posts_text) if args.posts_text else None,
            spark.read.parquet(args.exact) if args.exact else None,
            args.max_rank,
        )
        table.write.mode("overwrite").parquet(args.per_pair_out)
    print(metrics, file=sys.stderr)
    print("Precision is a lower bound: some true duplicates were never labelled.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
