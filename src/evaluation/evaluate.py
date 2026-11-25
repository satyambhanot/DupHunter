"""Evaluate candidate pairs against the answer key: one row of metrics per run.

Inputs (contracts in docs/CONTRACTS.md):

- candidates: C4 ``(dup_id, cand_id, est_sim)`` or C5 ``(dup_id, cand_id, rank, score, method)``.
  C4 is ranked by ``est_sim`` (highest first), ties by ``cand_id`` ascending.
- answer key: C6 ``(dup_id, original_id, created)``.

Outputs: one row appended to ``results/metrics.csv`` (C7) and, optionally, a per-pair table (C8).

On a sample, pass ``--universe`` (a Parquet with ``post_id`` of the sampled questions). Without it
every answer-key pair with a question outside the sample counts as a miss, and recall is too low.

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

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.evaluation.metrics import METRICS, mean_metrics, reduction_ratio
from src.verification.rankers import compute_exact_jaccard

COLUMNS = ["experiment", "settings_json", *METRICS, "reduction_ratio", "n_candidates"]
DEFAULT_MAX_RANK = 100


def rank_candidates(candidates: DataFrame) -> DataFrame:
    """Return ``(dup_id, cand_id, rank)``; rank 1 is the best candidate of its question."""
    if "rank" in candidates.columns:
        return candidates.select("dup_id", "cand_id", F.col("rank").cast("long").alias("rank"))
    order = Window.partitionBy("dup_id").orderBy(F.col("est_sim").desc(), F.col("cand_id").asc())
    return candidates.select("dup_id", "cand_id", F.row_number().over(order).alias("rank"))


def restrict_answer_key(answer_key: DataFrame, universe: DataFrame) -> DataFrame:
    """Keep the answer-key pairs whose ``dup_id`` and ``original_id`` are both in ``universe``.

    Args:
        answer_key: C6 duplicate pairs.
        universe: any DataFrame with a ``post_id`` column: the questions that were searched.
    """
    ids = universe.select("post_id").distinct()
    return answer_key.join(ids.withColumnRenamed("post_id", "dup_id"), "dup_id", "left_semi").join(
        ids.withColumnRenamed("post_id", "original_id"), "original_id", "left_semi"
    )


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


def exact_for_pairs(answer_key: DataFrame, shingles: DataFrame) -> DataFrame:
    """Exact Jaccard similarity of every answer-key pair, from C2 shingles.

    Returns ``(dup_id, cand_id, jaccard)``, the ``exact`` input of ``per_pair_table``. It covers
    the labelled pairs whether or not LSH found them, so recall can be broken down by similarity.
    A pair with a question that has no shingle row is left out (its similarity stays empty).
    """
    pairs = answer_key.select(
        "dup_id", F.col("original_id").alias("cand_id"), F.lit(0.0).alias("est_sim")
    )
    return compute_exact_jaccard(pairs, shingles).select(
        "dup_id", "cand_id", F.col("exact_jaccard").alias("jaccard")
    )


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
        "--n-questions",
        type=int,
        help="questions searched, for the reduction ratio (default: the --universe size)",
    )
    parser.add_argument(
        "--universe",
        help="Parquet with post_id of the sampled questions: only answer-key pairs inside it count",
    )
    parser.add_argument(
        "--max-rank", type=int, default=DEFAULT_MAX_RANK, help="ignore ranks below this"
    )
    parser.add_argument("--per-pair-out", help="write the per-pair table (C8) here as Parquet")
    parser.add_argument("--posts-text", help="posts_text Parquet, for tags and body length in C8")
    similarity = parser.add_mutually_exclusive_group()
    similarity.add_argument(
        "--exact", help="exact Jaccard Parquet (dup_id, cand_id, jaccard), for C8"
    )
    similarity.add_argument(
        "--shingles",
        help="C2 shingles Parquet: compute the exact Jaccard of the answer-key pairs, for C8",
    )
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Evaluate one run, append its metrics row and optionally write the per-pair table."""
    args = parse_args(argv)
    settings = json.loads(args.settings)
    spark = SparkSession.builder.master(args.master).appName("duphunter-evaluate").getOrCreate()
    candidates = spark.read.parquet(args.candidates)
    answer_key = spark.read.parquet(args.answer_key)
    n_questions = args.n_questions
    if args.universe:
        universe = spark.read.parquet(args.universe)
        answer_key = restrict_answer_key(answer_key, universe)
        if n_questions is None:
            n_questions = universe.select("post_id").distinct().count()
    metrics = evaluate(candidates, answer_key, n_questions, args.max_rank)
    append_metrics(args.out, args.experiment, settings, metrics)
    if args.per_pair_out:
        exact = spark.read.parquet(args.exact) if args.exact else None
        if args.shingles:
            exact = exact_for_pairs(answer_key, spark.read.parquet(args.shingles))
        table = per_pair_table(
            candidates,
            answer_key,
            spark.read.parquet(args.posts_text) if args.posts_text else None,
            exact,
            args.max_rank,
        )
        table.write.mode("overwrite").parquet(args.per_pair_out)
    print(metrics, file=sys.stderr)
    print("Precision is a lower bound: some true duplicates were never labelled.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
