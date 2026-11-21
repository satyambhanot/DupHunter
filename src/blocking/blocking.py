"""Tag-based blocking strategies (SON tagsets, single-tag, none) for LSH (E6, #17)."""

from __future__ import annotations

import argparse
import csv
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.evaluation.evaluate import evaluate, restrict_answer_key
from src.similarity.lsh import LSHConfig, find_candidates


@dataclass(frozen=True)
class FrequentTagset:
    """One frequent tag set loaded from SON output."""

    tags: frozenset[str]
    label: str
    size: int
    support: float


def load_frequent_tagsets(path: str | Path) -> list[FrequentTagset]:
    """Load frequent tagsets from a SON CSV (``tagset,size,count,support,...``)."""
    tagsets: list[FrequentTagset] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            tokens = frozenset(row["tagset"].split())
            if not tokens:
                continue
            tagsets.append(
                FrequentTagset(
                    tags=tokens,
                    label=row["tagset"].strip(),
                    size=int(row.get("size") or len(tokens)),
                    support=float(row["support"]),
                )
            )
    return sort_tagsets(tagsets)


def sort_tagsets(tagsets: Sequence[FrequentTagset]) -> list[FrequentTagset]:
    """Order tagsets by largest size first, then lowest support, then label."""
    return sorted(tagsets, key=lambda ts: (-ts.size, ts.support, ts.label))


def assign_son_groups(
    posts: DataFrame,
    tagsets: Sequence[FrequentTagset] | None = None,
    *,
    post_id_col: str = "post_id",
    tags_col: str = "tags",
    exclude_tags: Sequence[str] = (),
) -> DataFrame:
    """Assign each question to its largest frequent tagset (lowest support on tie), else other."""
    del exclude_tags
    ordered = sort_tagsets(tagsets or ())
    group_expr = F.lit("other")
    for ts in reversed(ordered):
        cond = F.lit(True)
        for tag in sorted(ts.tags):
            cond = cond & F.array_contains(F.col(tags_col), tag)
        group_expr = F.when(cond, F.lit(ts.label)).otherwise(group_expr)
    return posts.select(
        F.col(post_id_col).alias("post_id"),
        group_expr.alias("group"),
    )


DEFAULT_EXCLUDE_TAGS: tuple[str, ...] = ("python",)


def assign_single_tag_groups(
    posts: DataFrame,
    tagsets: Sequence[FrequentTagset] | None = None,
    *,
    post_id_col: str = "post_id",
    tags_col: str = "tags",
    exclude_tags: Sequence[str] = DEFAULT_EXCLUDE_TAGS,
) -> DataFrame:
    """Assign each question to its most frequent tag, or ``other`` when it has none left.

    Tags in ``exclude_tags`` are ignored. On the python subset every question carries
    ``python``, so grouping by it (or by the first tag, which is whatever the author typed first)
    would put most questions into one group and make this mode the same as no blocking.
    The most frequent tag is counted once over all questions; a tie goes to the tag whose
    name sorts last, so the result does not depend on the order of the tags.
    """
    del tagsets
    tags = F.coalesce(F.col(tags_col), F.array().cast("array<string>"))
    kept = F.filter(F.array_distinct(tags), lambda tag: ~tag.isin(*exclude_tags))
    prepared = posts.select(F.col(post_id_col).alias("post_id"), kept.alias("_tags"))
    exploded = prepared.select("post_id", F.explode("_tags").alias("_tag"))
    frequencies = exploded.groupBy("_tag").count()
    best = (
        exploded.join(F.broadcast(frequencies), "_tag")
        .groupBy("post_id")
        .agg(F.max(F.struct(F.col("count"), F.col("_tag"))).alias("_best"))
        .select("post_id", F.col("_best.`_tag`").alias("_group"))
    )
    return prepared.join(best, "post_id", "left").select(
        "post_id", F.coalesce(F.col("_group"), F.lit("other")).alias("group")
    )


def assign_no_groups(
    posts: DataFrame,
    tagsets: Sequence[FrequentTagset] | None = None,
    *,
    post_id_col: str = "post_id",
    tags_col: str = "tags",
    exclude_tags: Sequence[str] = (),
) -> DataFrame:
    """Assign all questions to a single ``all`` group (no blocking)."""
    del tagsets, tags_col, exclude_tags
    return posts.select(
        F.col(post_id_col).alias("post_id"),
        F.lit("all").alias("group"),
    )


STRATEGIES: dict[str, Callable[..., DataFrame]] = {
    "son": assign_son_groups,
    "single_tag": assign_single_tag_groups,
    "none": assign_no_groups,
}


def assign_groups(
    posts: DataFrame,
    mode: str = "son",
    tagsets: Sequence[FrequentTagset] | None = None,
    *,
    post_id_col: str = "post_id",
    tags_col: str = "tags",
    exclude_tags: Sequence[str] = DEFAULT_EXCLUDE_TAGS,
) -> DataFrame:
    """Assign Contract C9 ``(post_id, group)`` rows using the requested blocking mode.

    ``exclude_tags`` only matters for ``single_tag``. ``son`` needs ``tagsets``: without them
    every question would land in ``other``, so that is an error.
    """
    if mode not in STRATEGIES:
        raise ValueError(f"Unknown blocking mode {mode!r}; choose from {sorted(STRATEGIES)}")
    if mode == "son" and not tagsets:
        raise ValueError("mode 'son' needs the frequent tagsets (--tagsets), got none")
    return STRATEGIES[mode](
        posts,
        tagsets=tagsets,
        post_id_col=post_id_col,
        tags_col=tags_col,
        exclude_tags=exclude_tags,
    )


def compute_recall_cost(groups: DataFrame, answer_key: DataFrame) -> float:
    """Return the share of answer-key pairs whose questions fall into different groups.

    Only pairs whose two questions both appear in ``groups`` are counted, so scoring a sample
    does not count pairs outside it as split.
    """
    answer_key = restrict_answer_key(answer_key, groups)
    total = answer_key.count()
    if total == 0:
        return 0.0
    dup_groups = groups.select(
        F.col("post_id").alias("_dup_id"),
        F.col("group").alias("_dup_group"),
    )
    orig_groups = groups.select(
        F.col("post_id").alias("_orig_id"),
        F.col("group").alias("_orig_group"),
    )
    joined = answer_key.join(
        dup_groups, answer_key["dup_id"] == dup_groups["_dup_id"], "left"
    ).join(orig_groups, answer_key["original_id"] == orig_groups["_orig_id"], "left")
    split_count = joined.filter(F.col("_dup_group") != F.col("_orig_group")).count()
    return float(split_count) / float(total)


def compare_blocking(
    signatures: DataFrame,
    posts_tags: DataFrame,
    answer_key: DataFrame,
    tagsets: Sequence[FrequentTagset],
    *,
    modes: Sequence[str] = ("none", "single_tag", "son"),
    lsh_config: LSHConfig | None = None,
    n_questions: int | None = None,
    exclude_tags: Sequence[str] = DEFAULT_EXCLUDE_TAGS,
) -> list[dict[str, object]]:
    """Run LSH with each blocking mode and collect time, candidates, Recall@10, MAP, recall_cost.

    The answer key is first restricted to the questions in ``signatures``, so a sample is scored
    against its own pairs. ``shuffle_bytes`` is left empty: it comes from the Spark event log of a
    cluster run, not from this function.
    """
    cfg = lsh_config or LSHConfig(b=32, r=4, n=128)
    answer_key = restrict_answer_key(answer_key, signatures)
    total_q = n_questions if n_questions is not None else signatures.count()
    rows: list[dict[str, object]] = []

    for mode in modes:
        groups_df = assign_groups(
            posts_tags, mode=mode, tagsets=tagsets, exclude_tags=exclude_tags
        ).cache()
        cost = 0.0 if mode == "none" else compute_recall_cost(groups_df, answer_key)
        lsh_groups = None if mode == "none" else groups_df

        t0 = time.perf_counter()
        candidates = find_candidates(signatures, config=cfg, groups=lsh_groups).cache()
        n_candidates = candidates.count()
        elapsed = time.perf_counter() - t0

        metrics = evaluate(candidates, answer_key, n_questions=total_q)
        rows.append(
            {
                "mode": mode,
                "time_sec": round(elapsed, 4),
                "shuffle_bytes": "",
                "n_candidates": n_candidates,
                "recall@10": round(float(metrics["recall@10"]), 6),
                "map": round(float(metrics["map"]), 6),
                "recall_cost": round(cost, 6),
            }
        )
    return rows


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for tag-blocking assignment and comparison."""
    parser = argparse.ArgumentParser(description="E6 tag-blocking assignment and LSH comparison.")
    parser.add_argument("--posts-tags", required=True, help="Parquet with (post_id, tags)")
    parser.add_argument("--tagsets", help="SON frequent_tagsets.csv path (required for 'son' mode)")
    parser.add_argument("--signatures", help="MinHash signatures Parquet (C3) for LSH comparison")
    parser.add_argument("--answer-key", help="Answer key Parquet (C6) for evaluation")
    parser.add_argument("--mode", choices=sorted(STRATEGIES), default="son")
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=sorted(STRATEGIES),
        default=None,
        help="Modes to run with --output (default: none single_tag son)",
    )
    parser.add_argument("--groups-out", help="Write Contract C9 (post_id, group) Parquet here")
    parser.add_argument("--output", help="Write E6 comparison CSV here (runs all modes)")
    parser.add_argument(
        "--exclude-tag",
        action="append",
        default=None,
        help="Tag ignored by single_tag grouping (repeatable; default: python)",
    )
    parser.add_argument("-b", "--bands", type=int, default=32)
    parser.add_argument("-r", "--rows", type=int, default=4)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for assigning C9 groups or comparing blocking modes."""
    args = parse_args(argv)
    tagsets = load_frequent_tagsets(args.tagsets) if args.tagsets else []
    exclude_tags = tuple(args.exclude_tag) if args.exclude_tag else DEFAULT_EXCLUDE_TAGS
    if not tagsets and (args.output or (args.groups_out and args.mode == "son")):
        raise SystemExit("error: --tagsets is required for the 'son' mode and for --output")
    spark = SparkSession.builder.master(args.master).appName("duphunter-e6-blocking").getOrCreate()
    try:
        posts_tags = spark.read.parquet(args.posts_tags)
        if args.groups_out:
            groups_df = assign_groups(
                posts_tags, mode=args.mode, tagsets=tagsets, exclude_tags=exclude_tags
            )
            groups_df.write.mode("overwrite").parquet(args.groups_out)
        if args.output:
            if not args.signatures or not args.answer_key:
                raise ValueError("--signatures and --answer-key are required with --output")
            sigs = spark.read.parquet(args.signatures)
            ak = spark.read.parquet(args.answer_key)
            rows = compare_blocking(
                sigs,
                posts_tags,
                ak,
                tagsets,
                modes=tuple(args.modes) if args.modes else ("none", "single_tag", "son"),
                lsh_config=LSHConfig(b=args.bands, r=args.rows, n=args.bands * args.rows),
                exclude_tags=exclude_tags,
            )
            out = Path(args.output)
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
