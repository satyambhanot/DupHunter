"""Build the answer key: the known duplicate pairs (contract C6).

Stack Overflow marks a question as a duplicate of an older one with a ``post_links`` row of
link type 3.
The answer key keeps those pairs whose two questions are both in ``posts_text`` and always puts
the newer question first (``dup_id``) and the older one second (``original_id``).

Example (run from the repository root)::

    python -m src.etl.answer_key --parquet-root data/parquet/superuser \\
        --output data/answer_key/superuser
"""

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.schema import LINK_DUPLICATE
from src.etl.spark_session import get_spark


def _linked_questions(post_links: DataFrame, questions: DataFrame) -> DataFrame:
    """Duplicate links joined to the creation time of both questions (missing ones drop out)."""
    links = post_links.filter(F.col("link_type") == LINK_DUPLICATE).select(
        F.col("post_id").alias("a_id"), F.col("related_post_id").alias("b_id")
    )
    first = questions.select(F.col("post_id").alias("a_id"), F.col("created").alias("a_created"))
    second = questions.select(F.col("post_id").alias("b_id"), F.col("created").alias("b_created"))
    return links.join(first, "a_id").join(second, "b_id")


def build_answer_key(post_links: DataFrame, questions: DataFrame) -> DataFrame:
    """Return the answer key ``(dup_id, original_id, created)``.

    Args:
        post_links: the ``post_links`` table (C1).
        questions: a table with ``post_id`` and ``created`` of every question to consider, usually
            ``posts_text``. A link is dropped when either of its questions is missing here.

    Pairs are oriented by creation time: ``dup_id`` is the newer question. A link whose two
    questions share the same timestamp has no direction and is dropped. Each pair appears once.
    """
    linked = _linked_questions(post_links, questions).filter(
        F.col("a_created") != F.col("b_created")
    )
    a_is_newer = F.col("a_created") > F.col("b_created")
    return linked.select(
        F.when(a_is_newer, F.col("a_id")).otherwise(F.col("b_id")).alias("dup_id"),
        F.when(a_is_newer, F.col("b_id")).otherwise(F.col("a_id")).alias("original_id"),
        F.greatest("a_created", "b_created").alias("created"),
    ).dropDuplicates(["dup_id", "original_id"])


def answer_key_counts(post_links: DataFrame, questions: DataFrame) -> dict[str, int]:
    """Count what happened to the duplicate links while the answer key was built.

    Returns the number of duplicate links, how many were dropped because a question was missing, how
    many pointed from the older to the newer question (swapped), how many had equal timestamps
    (dropped), and the number of pairs kept.
    """
    total = post_links.filter(F.col("link_type") == LINK_DUPLICATE).count()
    linked = _linked_questions(post_links, questions)
    row = linked.agg(
        F.count("*").alias("both_found"),
        F.sum(F.when(F.col("a_created") < F.col("b_created"), 1).otherwise(0)).alias("swapped"),
        F.sum(F.when(F.col("a_created") == F.col("b_created"), 1).otherwise(0)).alias("equal_time"),
    ).first()
    return {
        "duplicate_links": total,
        "dropped_missing": total - row["both_found"],
        "swapped": int(row["swapped"] or 0),
        "dropped_equal_time": int(row["equal_time"] or 0),
        "pairs_kept": build_answer_key(post_links, questions).count(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--parquet-root", required=True, help="site root holding posts_text and post_links"
    )
    parser.add_argument("--output", required=True, help="where to write the answer key (Parquet)")
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write the answer key and print the link counts."""
    args = parse_args(argv)
    spark = get_spark(args.master, "duphunter-answer-key")
    root = args.parquet_root.rstrip("/")
    questions = spark.read.parquet(f"{root}/posts_text").select("post_id", "created")
    post_links = spark.read.parquet(f"{root}/post_links")
    build_answer_key(post_links, questions).write.mode("overwrite").parquet(args.output)
    print(answer_key_counts(post_links, questions), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
