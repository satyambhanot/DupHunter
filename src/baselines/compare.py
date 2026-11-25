"""E5 on one shared sample: exact search, our LSH and Spark MinHashLSH.

``python -m src.baselines.compare`` draws one deterministic sample, saves its post ids, builds the
E1 shingles (word 1-grams of the title) once, and runs every method on exactly those shingles with
the same answer key (the labelled pairs whose two questions are both in the sample). So the
differences between rows come from the methods, not from the data.

The sample is ``--pairs`` labelled pairs (both questions of each) plus ``--extra`` other
questions, chosen by a hash of the seed and the ids, so it does not depend on row order.

For every method the table has the candidate count, Recall@10, Precision@10 and MAP against the
labels, the share of labelled pairs that are candidates at any rank, and for each ``--levels``
value the share of the exact pairs with at least that Jaccard similarity that the method also
returns. ``time_s``
is one materialisation of the method's candidates; for Spark it includes building the vectors and
fitting the model, and for ours it excludes the MinHash signatures, timed in the notes.
"""

from __future__ import annotations

import argparse
import csv
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.baselines.exact import exact_jaccard_pairs
from src.baselines.spark_baseline import spark_minhash_pairs
from src.evaluation.evaluate import evaluate, restrict_answer_key
from src.similarity.lsh import LSHConfig, find_candidates
from src.similarity.minhash import MinHashConfig, build_signatures
from src.similarity.shingles import ShingleConfig, build_shingles

LSH_SETTINGS = ((64, 2), (32, 4), (16, 8))
SPARK_TABLES = (3, 5, 10)
LEVELS = (0.2, 0.4, 0.6)


def sample_ids(
    posts: DataFrame, answer_key: DataFrame, n_pairs: int, n_extra: int, seed: int = 42
) -> DataFrame:
    """The ``post_id`` values of the shared sample (see the module text)."""
    key = restrict_answer_key(answer_key, posts)
    pairs = key.select("dup_id", "original_id").distinct()
    chosen = pairs.orderBy(
        F.xxhash64(F.lit(seed), F.col("dup_id"), F.col("original_id")), "dup_id", "original_id"
    ).limit(n_pairs)
    paired = (
        chosen.select(F.col("dup_id").alias("post_id"))
        .union(chosen.select(F.col("original_id").alias("post_id")))
        .distinct()
    )
    extra = (
        posts.select("post_id")
        .join(paired, "post_id", "left_anti")
        .orderBy(F.xxhash64(F.lit(seed), F.col("post_id")), "post_id")
        .limit(n_extra)
    )
    return paired.union(extra)


def _found_share(candidates: DataFrame, wanted: DataFrame) -> float:
    """Share of the ``(dup_id, cand_id)`` rows of ``wanted`` that are also candidates."""
    total = wanted.count()
    if total == 0:
        return float("nan")
    return wanted.join(candidates, ["dup_id", "cand_id"], "left_semi").count() / total


def compare_methods(
    shingles: DataFrame,
    answer_key: DataFrame,
    *,
    exact_threshold: float = 0.1,
    spark_threshold: float = 0.6,
    levels: Sequence[float] = LEVELS,
    lsh_settings: Sequence[tuple[int, int]] = LSH_SETTINGS,
    spark_tables: Sequence[int] = SPARK_TABLES,
    num_perm: int = 128,
    seed: int = 42,
) -> tuple[list[dict[str, object]], dict[str, float]]:
    """Run every method on ``shingles`` (``post_id``, ``shingles``, ``created``).

    ``answer_key`` must already be restricted to these questions. Returns one row per method and
    setting, and notes with the sample size and the time taken by our MinHash signatures.

    Raises:
        ValueError: if a level is below ``exact_threshold``, where the exact pairs are unknown.
    """
    if any(level < exact_threshold for level in levels):
        raise ValueError("every level must be at least the exact threshold")
    shingles = shingles.cache()
    n_questions = shingles.count()
    labelled = answer_key.select("dup_id", F.col("original_id").alias("cand_id")).distinct()

    started = time.perf_counter()
    signatures = (
        build_signatures(shingles, MinHashConfig(n=num_perm, seed=seed))
        .join(shingles.select("post_id", "created"), "post_id")
        .cache()
    )
    signatures.count()
    notes = {
        "n_questions": n_questions,
        "n_labelled_pairs": labelled.count(),
        "signature_time_s": round(time.perf_counter() - started, 3),
    }

    methods: list[tuple[str, str, Callable[[], DataFrame]]] = [
        (
            "exact",
            f"J >= {exact_threshold:g}",
            lambda: exact_jaccard_pairs(shingles, exact_threshold, engine="spark_join"),
        )
    ]
    for b, r in lsh_settings:
        config = LSHConfig(b=b, r=r)
        sigs = signatures
        if b * r != num_perm:
            sigs = signatures.withColumn("signature", F.slice("signature", 1, b * r))
        methods.append(
            ("our_lsh", f"b={b} r={r}", lambda s=sigs, c=config: find_candidates(s, config=c))
        )
    for tables in spark_tables:
        methods.append(
            (
                "spark_minhash",
                f"{tables} tables, J >= {spark_threshold:g}",
                lambda t=tables: spark_minhash_pairs(shingles, spark_threshold, t, seed)[0],
            )
        )

    rows: list[dict[str, object]] = []
    exact_pairs: dict[float, DataFrame] = {}
    exact_result: DataFrame | None = None
    for method, setting, build in methods:
        started = time.perf_counter()
        candidates = build().cache()
        n_candidates = candidates.count()
        elapsed = time.perf_counter() - started
        if method == "exact":
            exact_result = candidates
            exact_pairs = {
                level: exact_result.filter(F.col("est_sim") >= level - 1e-9).select(
                    "dup_id", "cand_id"
                )
                for level in levels
            }
        metrics = evaluate(candidates, answer_key, n_questions=n_questions)
        pairs = candidates.select("dup_id", "cand_id")
        row: dict[str, object] = {
            "method": method,
            "setting": setting,
            "n_candidates": n_candidates,
            "recall@10": round(float(metrics["recall@10"]), 4),
            "precision@10": round(float(metrics["precision@10"]), 4),
            "map": round(float(metrics["map"]), 4),
            "labelled_pairs_found": round(_found_share(pairs, labelled), 4),
        }
        for level in levels:
            row[f"exact_pairs_found_J>={level:g}"] = round(
                _found_share(pairs, exact_pairs[level]), 4
            )
        row["time_s"] = round(elapsed, 3)
        rows.append(row)
        if method != "exact":
            candidates.unpersist()
    if exact_result is not None:
        exact_result.unpersist()
    signatures.unpersist()
    return rows, notes


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--posts", required=True, help="C1 posts_text Parquet")
    parser.add_argument("--answer-key", required=True, help="C6 answer key Parquet")
    parser.add_argument("--out", required=True, help="directory for the table and the sample ids")
    parser.add_argument("--pairs", type=int, default=2000, help="labelled pairs in the sample")
    parser.add_argument("--extra", type=int, default=6000, help="other questions in the sample")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--exact-threshold", type=float, default=0.1)
    parser.add_argument("--spark-threshold", type=float, default=0.6)
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.master(args.master).appName("duphunter-e5-compare").getOrCreate()
    try:
        posts = spark.read.parquet(args.posts)
        answer_key = spark.read.parquet(args.answer_key)
        ids = sample_ids(posts, answer_key, args.pairs, args.extra, args.seed).cache()
        sample = posts.join(ids, "post_id", "left_semi")
        shingles = build_shingles(sample.withColumn("body", F.lit("")), ShingleConfig(k=1)).join(
            sample.select("post_id", "created"), "post_id"
        )
        rows, notes = compare_methods(
            shingles.select("post_id", "shingles", "created"),
            restrict_answer_key(answer_key, ids),
            exact_threshold=args.exact_threshold,
            spark_threshold=args.spark_threshold,
            seed=args.seed,
        )
        sample_list = sorted(row.post_id for row in ids.collect())
    finally:
        spark.stop()

    (out / "sample_ids.csv").write_text(
        "post_id\n" + "".join(f"{post_id}\n" for post_id in sample_list), encoding="utf-8"
    )
    with (out / "shared_sample.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(notes)
    for row in rows:
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
