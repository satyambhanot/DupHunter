"""E1: which text representation separates real duplicates from random pairs best?

Every later result depends on how a question becomes a set. Before tuning LSH we measure, with the
exact Jaccard similarity only (no MinHash), how similar labelled duplicates are and how well each
representation tells them apart from random pairs. Per option we report the Jaccard distribution of
duplicate and random pairs (median and quartiles), the separation (the ROC AUC of Jaccard for
"duplicate versus random"), the share of duplicates above 0.2 and 0.4 (what an LSH threshold could
find) and the cost (mean number of shingles per question).

Input: the ``pairs_e1`` export (``src.etl.exports``): ``post_id_a, post_id_b, title_a, body_a,
title_b, body_b, is_dup``. The options are read from ``configs/e1_representation.json``; a new
option is a new entry there, not new code.

Example (run from the repository root)::

    python -m src.similarity.representation_study --pairs data/exports/superuser/pairs_e1 \\
        --config configs/e1_representation.json --out results/e1_representation
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.similarity.shingles import ShingleConfig, build_shingles

THRESHOLDS = (0.2, 0.4)


def jaccard(a: set, b: set) -> float:
    """Jaccard similarity of two sets; 0.0 when both are empty."""
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def separation_auc(dup: list[float], rand: list[float]) -> float:
    """ROC AUC of a similarity for 'duplicate versus random' (Mann-Whitney, ties count half).

    1.0 means every duplicate pair is more similar than every random pair; 0.5 means no signal.
    """
    if not dup or not rand:
        raise ValueError("need at least one duplicate and one random pair")
    values = np.concatenate([dup, rand])
    order = values.argsort(kind="mergesort")
    ranks = np.empty(len(values))
    sorted_values = values[order]
    start = 0
    for end in range(1, len(values) + 1):  # average ranks over runs of equal values
        if end == len(values) or sorted_values[end] != sorted_values[start]:
            ranks[order[start:end]] = (start + 1 + end) / 2
            start = end
    rank_sum = ranks[: len(dup)].sum()
    return float((rank_sum - len(dup) * (len(dup) + 1) / 2) / (len(dup) * len(rand)))


def load_options(path: str | Path) -> list[dict]:
    """The options of the study from the JSON config."""
    with open(path) as handle:
        return json.load(handle)["options"]


def posts_of(pairs: DataFrame) -> DataFrame:
    """The distinct questions of the pairs as ``(post_id, title, body)``."""
    sides = [
        pairs.select(
            F.col(f"post_id_{s}").alias("post_id"),
            F.col(f"title_{s}").alias("title"),
            F.col(f"body_{s}").alias("body"),
        )
        for s in ("a", "b")
    ]
    return sides[0].union(sides[1]).dropDuplicates(["post_id"])


def shingles_for(posts: DataFrame, option: dict) -> DataFrame:
    """``(post_id, shingles)`` for one option, with its fields, code and common-shingle choices."""
    source = (
        posts
        if option.get("fields", "title+body") == "title+body"
        else posts.withColumn("body", F.lit(""))
    )
    config = ShingleConfig(
        representation=option["representation"],
        k=option["k"],
        drop_common_share=option.get("drop_common_share"),
    )
    shingled = build_shingles(source.select("post_id", "title", "body"), config)
    if option.get("include_code"):
        shingled = shingled.withColumn("shingles", F.array_union("shingles", "code_shingles"))
    return shingled.select("post_id", "shingles")


def pair_similarities(pairs: DataFrame, shingles: DataFrame) -> pd.DataFrame:
    """Exact Jaccard of every pair: ``is_dup``, ``jaccard`` and the shingle counts of both sides."""
    left = shingles.select(F.col("post_id").alias("post_id_a"), F.col("shingles").alias("sh_a"))
    right = shingles.select(F.col("post_id").alias("post_id_b"), F.col("shingles").alias("sh_b"))
    joined = (
        pairs.select("post_id_a", "post_id_b", "is_dup")
        .join(left, "post_id_a")
        .join(right, "post_id_b")
    )
    union = F.size(F.array_union("sh_a", "sh_b"))
    result = joined.select(
        "is_dup",
        F.when(union > 0, F.size(F.array_intersect("sh_a", "sh_b")) / union)
        .otherwise(0.0)
        .alias("jaccard"),
        F.size("sh_a").alias("n_a"),
        F.size("sh_b").alias("n_b"),
    )
    return result.toPandas()


def summarise(name: str, sims: pd.DataFrame) -> dict:
    """One row of the option table from the pair similarities of one option."""
    dup = sims.loc[sims["is_dup"], "jaccard"].to_numpy()
    rand = sims.loc[~sims["is_dup"], "jaccard"].to_numpy()
    q1, med, q3 = np.percentile(dup, [25, 50, 75])
    row = {
        "option": name,
        "median_dup": float(med),
        "q1_dup": float(q1),
        "q3_dup": float(q3),
        "median_rand": float(np.median(rand)),
        "auc": separation_auc(list(dup), list(rand)),
        "mean_shingles": float(pd.concat([sims["n_a"], sims["n_b"]]).mean()),
    }
    for threshold in THRESHOLDS:
        row[f"dup_share_ge_{threshold}"] = float((dup >= threshold).mean())
    return row


def run_study(
    spark: SparkSession, pairs: DataFrame, options: list[dict]
) -> tuple[pd.DataFrame, dict]:
    """Evaluate every option; return the option table and the pair similarities per option."""
    posts = posts_of(pairs).cache()
    rows, sims_by_option = [], {}
    for option in options:
        sims = pair_similarities(pairs, shingles_for(posts, option))
        sims_by_option[option["name"]] = sims
        rows.append(summarise(option["name"], sims))
    return pd.DataFrame(rows), sims_by_option


def plot_histograms(sims_by_option: dict, names: list[str], path: str | Path) -> None:
    """Histograms of the Jaccard of duplicate and random pairs for some options (drawing only)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(
        1, len(names), figsize=(4.5 * len(names), 3.5), sharey=True, squeeze=False
    )
    bins = np.linspace(0, 1, 21)
    for ax, name in zip(axes[0], names, strict=True):
        sims = sims_by_option[name]
        ax.hist(
            sims.loc[~sims["is_dup"], "jaccard"],
            bins=bins,
            alpha=0.6,
            label="random",
            color="#999999",
        )
        ax.hist(
            sims.loc[sims["is_dup"], "jaccard"],
            bins=bins,
            alpha=0.7,
            label="duplicate",
            color="#c0392b",
        )
        ax.set_yscale("log")
        ax.set_title(name, fontsize=9)
        ax.set_xlabel("exact Jaccard")
    axes[0][0].set_ylabel("pairs (log scale)")
    axes[0][0].legend()
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--pairs", required=True, help="the pairs_e1 export (Parquet)")
    parser.add_argument("--config", default="configs/e1_representation.json")
    parser.add_argument("--out", default="results/e1_representation")
    parser.add_argument("--plot", type=int, default=3, help="histograms for this many best options")
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the study, write ``options.csv`` and the histograms of the best options by AUC."""
    args = parse_args(argv)
    spark = SparkSession.builder.master(args.master).appName("duphunter-e1").getOrCreate()
    table, sims = run_study(spark, spark.read.parquet(args.pairs), load_options(args.config))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "options.csv", index=False, float_format="%.4f")
    best = list(table.sort_values("auc", ascending=False)["option"].head(args.plot))
    plot_histograms(sims, best, out / "best_options_histograms.png")
    print(table.round(3).to_string(index=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
