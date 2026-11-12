"""Small files for the team, built from the full tables (never committed to git).

- ``python_tags``: id and tags of every ``python`` question (for SON, about 30 MB on
  Stack Overflow).
- ``python_sample``: a sample of questions that contains every member of the duplicate pairs.
- ``pairs_e1``: duplicate pairs and random pairs with title and body, for the text study.

All choices are deterministic: rows are ordered by a hash of their id and the seed from
``configs/sample.json``, so the same inputs and seed give the same files.

Example (run from the repository root)::

    python -m src.etl.exports --parquet-root data/parquet/superuser \\
        --answer-key data/answer_key/superuser --output data/exports/superuser
"""

import argparse
import json
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.etl.spark_session import get_spark

PYTHON_TAG = "python"
DEFAULT_CONFIG = {"seed": 0, "sample_size": 50_000, "dup_pairs": 2_000, "random_pairs": 2_000}


def _by_hash(df: DataFrame, columns: list[str], seed: int) -> DataFrame:
    """Order rows by a seeded hash of ``columns``; ties are broken by the columns themselves."""
    return df.orderBy(F.xxhash64(*columns, F.lit(seed)), *columns)


def export_python_tags(posts_meta: DataFrame, tag: str | None = PYTHON_TAG) -> DataFrame:
    """Return ``(post_id, tags)`` of every question tagged ``tag`` (all questions if empty).

    Super User has only 3,212 ``python`` questions, too few for the exports; for development there,
    pass an empty tag to use all questions.
    """
    questions = posts_meta.filter(F.col("post_type") == 1)
    if tag:
        questions = questions.filter(F.array_contains("tags", tag))
    return questions.select("post_id", "tags")


def make_sample(questions: DataFrame, pairs: DataFrame, n: int, seed: int) -> DataFrame:
    """Return ``n`` questions (fewer if there are fewer): all pair members plus random ones.

    Args:
        questions: question rows with ``post_id`` (and any other columns, which are kept).
        pairs: the answer key with ``dup_id`` and ``original_id``.
        n: sample size.
        seed: seed of the hash order used to pick the random questions.

    Raises:
        ValueError: if the pair members alone are more than ``n``.
    """
    member_ids = (
        pairs.select(F.col("dup_id").alias("post_id"))
        .union(pairs.select(F.col("original_id").alias("post_id")))
        .distinct()
    )
    members = questions.join(member_ids, "post_id", "left_semi")
    member_count = members.count()
    if member_count > n:
        raise ValueError(f"the pair members ({member_count}) do not fit in a sample of {n}")
    others = questions.join(member_ids, "post_id", "left_anti")
    fill = _by_hash(others, ["post_id"], seed).limit(n - member_count)
    return members.unionByName(fill)


def make_pairs(
    answer_key: DataFrame,
    questions: DataFrame,
    dup_pairs: int,
    random_pairs: int,
    seed: int,
) -> DataFrame:
    """Return duplicate pairs and random pairs with their text.

    Columns: ``post_id_a``, ``post_id_b``, ``title_a``, ``body_a``, ``title_b``, ``body_b``,
    ``is_dup``. Duplicate pairs come from the answer key (``a`` is the newer question); random
    pairs join two random questions and never repeat a known duplicate pair.
    """
    text = questions.select("post_id", "title", "body")
    first = text.select(*[F.col(c).alias(f"{c}_a") for c in text.columns])
    second = text.select(*[F.col(c).alias(f"{c}_b") for c in text.columns])

    keyed = answer_key.select(
        F.col("dup_id").alias("post_id_a"), F.col("original_id").alias("post_id_b")
    )
    ids = text.select("post_id")
    keyed = keyed.join(
        ids.select(F.col("post_id").alias("post_id_a")), "post_id_a", "left_semi"
    ).join(ids.select(F.col("post_id").alias("post_id_b")), "post_id_b", "left_semi")
    dups = _by_hash(keyed, ["post_id_a", "post_id_b"], seed).limit(dup_pairs)

    ranked = _by_hash(text.select("post_id"), ["post_id"], seed + 1).limit(2 * random_pairs)
    numbered = ranked.withColumn(
        "n", F.row_number().over(Window.orderBy(F.xxhash64("post_id", F.lit(seed + 1)), "post_id"))
    )
    left = numbered.filter(F.col("n") % 2 == 1).select(
        F.col("post_id").alias("post_id_a"), (F.col("n") + 1).alias("n")
    )
    right = numbered.select(F.col("post_id").alias("post_id_b"), F.col("n"))
    randoms = left.join(right, "n").select("post_id_a", "post_id_b")
    known = keyed.union(
        keyed.select(F.col("post_id_b").alias("post_id_a"), F.col("post_id_a").alias("post_id_b"))
    )
    randoms = randoms.join(known, ["post_id_a", "post_id_b"], "left_anti")

    both = dups.withColumn("is_dup", F.lit(True)).unionByName(
        randoms.withColumn("is_dup", F.lit(False))
    )
    return (
        both.join(first, "post_id_a")
        .join(second, "post_id_b")
        .select("post_id_a", "post_id_b", "title_a", "body_a", "title_b", "body_b", "is_dup")
    )


def load_config(path: str | None) -> dict:
    """Read ``configs/sample.json``; missing keys fall back to the defaults."""
    config = dict(DEFAULT_CONFIG)
    if path:
        with open(path) as handle:
            config.update(json.load(handle))
    return config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--parquet-root", required=True, help="site root holding posts_meta and posts_text"
    )
    parser.add_argument("--answer-key", required=True, help="answer key Parquet (contract C6)")
    parser.add_argument("--output", required=True, help="directory for the three exports")
    parser.add_argument("--config", default="configs/sample.json", help="seed and sizes")
    parser.add_argument(
        "--tag",
        default=PYTHON_TAG,
        help="keep questions with this tag; empty keeps all (Super User: only 3,212 python)",
    )
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write ``python_tags``, ``python_sample`` and ``pairs_e1`` under the output directory."""
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(args.master, "duphunter-exports")
    root = args.parquet_root.rstrip("/")
    out = args.output.rstrip("/")
    key = spark.read.parquet(args.answer_key)
    questions = spark.read.parquet(f"{root}/posts_text")
    tagged = export_python_tags(spark.read.parquet(f"{root}/posts_meta"), args.tag)
    python_questions = questions.join(tagged.select("post_id"), "post_id", "left_semi")
    tagged.write.mode("overwrite").parquet(f"{out}/python_tags")
    make_sample(python_questions, key, config["sample_size"], config["seed"]).write.mode(
        "overwrite"
    ).parquet(f"{out}/python_sample")
    make_pairs(
        key, python_questions, config["dup_pairs"], config["random_pairs"], config["seed"]
    ).write.mode("overwrite").parquet(f"{out}/pairs_e1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
