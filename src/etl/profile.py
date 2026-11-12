"""Dataset statistics for the report (dataset section): sizes, missing values, text properties.

Example (run from the repository root)::

    python -m src.etl.profile --parquet-root data/parquet/superuser --site superuser \\
        --output results/dataset_stats.md

Every number in the dataset section of the report comes from the file this writes.
"""

import argparse
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.schema import ANSWER, LINK_DUPLICATE, LINK_LINKED, QUESTION
from src.etl.spark_session import get_spark

PYTHON_TAG = "python"


def compute_profile(posts_meta: DataFrame, posts_text: DataFrame, post_links: DataFrame) -> dict:
    """Compute the dataset statistics.

    Args:
        posts_meta: questions and answers without text (C1).
        posts_text: question title and body (C1); may be a subset such as the ``python`` questions.
        post_links: links between posts (C1).

    Returns:
        A dict with the counts of questions, answers, duplicate and linked links, python questions,
        questions per year, questions without an owner, and for ``posts_text`` the number of
        questions, empty bodies, the share of bodies with code and the mean body length.
    """
    meta = posts_meta.agg(
        F.sum(F.when(F.col("post_type") == QUESTION, 1).otherwise(0)).alias("questions"),
        F.sum(F.when(F.col("post_type") == ANSWER, 1).otherwise(0)).alias("answers"),
        F.sum(
            F.when(
                (F.col("post_type") == QUESTION) & F.array_contains("tags", PYTHON_TAG), 1
            ).otherwise(0)
        ).alias("python_questions"),
        F.sum(
            F.when((F.col("post_type") == QUESTION) & F.col("owner_user_id").isNull(), 1).otherwise(
                0
            )
        ).alias("missing_owners"),
    ).first()
    per_year = (
        posts_meta.filter(F.col("post_type") == QUESTION)
        .groupBy(F.year("created").alias("year"))
        .count()
        .orderBy("year")
        .collect()
    )
    links = post_links.agg(
        F.sum(F.when(F.col("link_type") == LINK_DUPLICATE, 1).otherwise(0)).alias(
            "duplicate_links"
        ),
        F.sum(F.when(F.col("link_type") == LINK_LINKED, 1).otherwise(0)).alias("linked_links"),
    ).first()
    empty_body = F.col("body").isNull() | (F.trim(F.col("body")) == "")
    text = posts_text.agg(
        F.count("*").alias("text_questions"),
        F.sum(F.when(empty_body, 1).otherwise(0)).alias("empty_bodies"),
        F.avg(F.when(F.col("body").rlike("(?i)<code"), 1.0).otherwise(0.0)).alias("code_share"),
        F.avg(F.length("body")).alias("mean_body_length"),
    ).first()
    return {
        "questions": int(meta["questions"] or 0),
        "answers": int(meta["answers"] or 0),
        "python_questions": int(meta["python_questions"] or 0),
        "missing_owners": int(meta["missing_owners"] or 0),
        "questions_per_year": {int(row["year"]): int(row["count"]) for row in per_year},
        "duplicate_links": int(links["duplicate_links"] or 0),
        "linked_links": int(links["linked_links"] or 0),
        "text_questions": int(text["text_questions"] or 0),
        "empty_bodies": int(text["empty_bodies"] or 0),
        "code_share": float(text["code_share"] or 0.0),
        "mean_body_length": float(text["mean_body_length"] or 0.0),
    }


def format_profile(profile: dict, site: str) -> str:
    """Render a profile as a Markdown section for one site."""
    lines = [
        f"<!-- site:{site} -->",
        f"## {site}",
        "",
        "| Statistic | Value |",
        "|---|---|",
        f"| Questions | {profile['questions']:,} |",
        f"| Answers | {profile['answers']:,} |",
        f"| `python` questions | {profile['python_questions']:,} |",
        f"| Questions without an owner | {profile['missing_owners']:,} |",
        f"| Duplicate links (type 3) | {profile['duplicate_links']:,} |",
        f"| Linked links (type 1) | {profile['linked_links']:,} |",
        f"| Questions with text | {profile['text_questions']:,} |",
        f"| Empty bodies | {profile['empty_bodies']:,} |",
        f"| Bodies with code | {profile['code_share']:.1%} |",
        f"| Mean body length (characters) | {profile['mean_body_length']:,.0f} |",
        "",
        "| Year | Questions |",
        "|---|---|",
        *[
            f"| {year} | {count:,} |"
            for year, count in sorted(profile["questions_per_year"].items())
        ],
        f"<!-- end:{site} -->",
        "",
    ]
    return "\n".join(lines)


def write_profile(profile: dict, site: str, path: str | Path) -> None:
    """Write or replace the section of ``site`` in the Markdown file at ``path``."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    section = format_profile(profile, site)
    text = path.read_text() if path.exists() else "# Dataset statistics\n\n"
    pattern = re.compile(
        rf"<!-- site:{re.escape(site)} -->.*?<!-- end:{re.escape(site)} -->\n?", re.S
    )
    if pattern.search(text):
        text = pattern.sub(lambda _: section, text)
    else:
        text = text.rstrip("\n") + "\n\n" + section
    path.write_text(text)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--parquet-root", required=True, help="site root holding the three tables")
    parser.add_argument("--site", required=True, help="site name used as the section title")
    parser.add_argument(
        "--output", default="results/dataset_stats.md", help="Markdown file to write"
    )
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Compute the profile of one site and write its section."""
    args = parse_args(argv)
    spark = get_spark(args.master, "duphunter-profile")
    root = args.parquet_root.rstrip("/")
    profile = compute_profile(
        spark.read.parquet(f"{root}/posts_meta"),
        spark.read.parquet(f"{root}/posts_text"),
        spark.read.parquet(f"{root}/post_links"),
    )
    write_profile(profile, args.site, args.output)
    print(profile, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
