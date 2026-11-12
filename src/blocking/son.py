"""SON (Savasere, Omiecinski, and Navathe) frequent itemset mining on tag baskets.

This module implements:
1. `apriori`: A local, single-pass/in-memory Apriori frequent itemset algorithm
   for a collection of baskets.
2. `son`: A distributed two-pass SON algorithm over a Spark RDD of baskets,
   wrapping `apriori` in `mapPartitions` for local candidate generation (Pass 1)
   and validating candidate counts globally across the RDD (Pass 2).

Part of Experiment E6 (Blocking) under Issue #16.
"""

from __future__ import annotations

import argparse
import csv
import math
import time
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.rdd import RDD


def apriori(
    baskets: Iterable[Sequence[str]],
    min_count: int,
    max_size: int | None = None,
) -> set[frozenset[str]]:
    """Find frequent itemsets in a collection of baskets using the Apriori algorithm.

    Parameters
    ----------
    baskets : Iterable[Sequence[str]]
        Iterable of baskets, where each basket is a sequence of items (e.g. question tags).
    min_count : int
        Minimum occurrence count required for an itemset to be considered frequent.
    max_size : int | None, optional
        Maximum size (k) of itemsets to discover. If None, discovers up to the maximum
        possible itemset size.

    Returns
    -------
    set[frozenset[str]]
        Set of frequent itemsets, where each itemset is represented as a frozenset of strings.
    """

    basket_sets = [set(b) for b in baskets]
    if not basket_sets or min_count <= 0:
        return set()

    all_frequent: set[frozenset[str]] = set()

    # --- Pass 1: Count individual items ---
    item_counts: dict[str, int] = defaultdict(int)
    for b in basket_sets:
        for item in b:
            item_counts[item] += 1

    current_L: set[frozenset[str]] = {
        frozenset({item}) for item, count in item_counts.items() if count >= min_count
    }
    all_frequent.update(current_L)

    if max_size == 1 or not current_L:
        return all_frequent

    # --- Pass k >= 2: Level-by-level candidate generation and verification ---
    k = 2
    while current_L:
        if max_size is not None and k > max_size:
            break

        # Candidate generation: join pairs from current_L of size k-1
        current_L_list = list(current_L)
        candidates: set[frozenset[str]] = set()
        for i in range(len(current_L_list)):
            for j in range(i + 1, len(current_L_list)):
                union_set = current_L_list[i] | current_L_list[j]
                if len(union_set) == k:
                    # Apriori pruning property: all subsets of size k-1 must be frequent
                    all_subsets_frequent = True
                    for item in union_set:
                        subset = union_set - {item}
                        if subset not in current_L:
                            all_subsets_frequent = False
                            break

                    if all_subsets_frequent:
                        candidates.add(union_set)

        if not candidates:
            break

        # Count candidate occurrences across baskets
        candidate_counts: dict[frozenset[str], int] = defaultdict(int)
        for b in basket_sets:
            for c in candidates:
                if c.issubset(b):
                    candidate_counts[c] += 1

        # Keep candidates meeting minimum support
        current_L = {c for c, count in candidate_counts.items() if count >= min_count}
        all_frequent.update(current_L)
        k += 1

    return all_frequent


def son(
    rdd: RDD[Sequence[str]],
    s: float,
    max_size: int | None = None,
) -> list[tuple[frozenset[str], int]]:
    """Distributed two-pass SON algorithm for mining frequent itemsets over an RDD.

    Pass 1:
        Divides baskets across partitions. Runs local `apriori` on each partition
        using a scaled support threshold (s * partition_size). The union of frequent
        itemsets from all partitions forms the global candidate set.

    Pass 2:
        Counts occurrences of candidate itemsets globally across the full RDD,
        filtering out candidates whose total count is strictly less than s * total_baskets.

    Parameters
    ----------
    rdd : RDD[Sequence[str]]
        Spark RDD where each record is a basket of items (tags).
    s : float
        Support threshold fraction (0.0 < s <= 1.0).
    max_size : int | None, optional
        Maximum size (k) of itemsets to generate.

    Returns
    -------
    list[tuple[frozenset[str], int]]
        List of tuples `(frequent_itemset, count)` that satisfy the global support threshold.
    """
    if rdd is None:
        raise ValueError("rdd cannot be None")
    if s <= 0:
        return []

    total_baskets = rdd.count()
    if total_baskets == 0:
        return []

    global_min_count = max(1, math.ceil(s * total_baskets))

    # --- Pass 1: Local Apriori per partition to generate candidate itemsets ---
    def _pass1_local_apriori(
        partition: Iterable[Sequence[str]],
    ) -> Iterable[frozenset[str]]:
        partition_baskets = list(partition)
        if not partition_baskets:
            return []
        local_min_count = max(1, math.floor(s * len(partition_baskets)))
        return apriori(
            partition_baskets,
            min_count=local_min_count,
            max_size=max_size,
        )

    candidates_list: list[frozenset[str]] = (
        rdd.mapPartitions(_pass1_local_apriori).distinct().collect()
    )
    if not candidates_list:
        return []

    candidate_set = set(candidates_list)

    # --- Pass 2: Count global occurrences of candidates across all partitions ---
    def _pass2_count_candidates(
        partition: Iterable[Sequence[str]],
    ) -> Iterable[tuple[frozenset[str], int]]:
        counts: dict[frozenset[str], int] = defaultdict(int)
        for basket in partition:
            basket_set = set(basket)
            for candidate in candidate_set:
                if candidate.issubset(basket_set):
                    counts[candidate] += 1
        return counts.items()

    frequent_itemsets: list[tuple[frozenset[str], int]] = (
        rdd.mapPartitions(_pass2_count_candidates)
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda item_count: item_count[1] >= global_min_count)
        .collect()
    )

    frequent_itemsets.sort(key=lambda x: (len(x[0]), sorted(x[0])))
    return frequent_itemsets


def run_son(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    support: float,
    max_size: int | None = None,
) -> dict[str, float]:
    """Run SON on tag baskets from an input file and export results to CSV.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    input_path : str
        Path to input file containing tags (Parquet, CSV, or text).
    output_path : str
        Path to save output CSV (e.g. results/e6_blocking/frequent_tagsets.csv).
    support : float
        Support threshold fraction.
    max_size : int | None, optional
        Maximum size of frequent itemsets.

    Returns
    -------
    dict[str, float]
        Dictionary containing recorded run times (`son_time_sec`, `fpgrowth_time_sec`).
    """
    path = Path(input_path)
    if path.suffix.lower() == ".parquet" or (path.is_dir() and any(path.glob("*.parquet"))):
        try:
            df = spark.read.parquet(input_path)
            tag_col = "tags" if "tags" in df.columns else df.columns[0]
            rdd = df.select(tag_col).rdd.map(
                lambda row: (
                    list(row[0]) if isinstance(row[0], (list, tuple)) else str(row[0]).split()
                )
            )
        except Py4JJavaError:
            if not path.exists():
                raise
            # Fallback for Windows without winutils.exe/hadoop.dll (NativeIO$Windows.access0).
            table = pq.read_table(input_path)
            tag_col = "tags" if "tags" in table.column_names else table.column_names[0]
            baskets_list = [
                list(val) if isinstance(val, (list, tuple)) else str(val).split()
                for val in table[tag_col].to_pylist()
                if val is not None
            ]
            rdd = spark.sparkContext.parallelize(baskets_list)
    else:
        rdd = spark.sparkContext.textFile(input_path).map(
            lambda line: [t.strip() for t in line.replace(",", " ").split() if t.strip()]
        )

    rdd = rdd.filter(lambda b: len(b) > 0).cache()
    total_baskets = rdd.count()

    # Time custom SON
    t0 = time.perf_counter()
    son_results = son(rdd, s=support, max_size=max_size)
    son_time_sec = time.perf_counter() - t0

    # Time Spark's built-in FPGrowth for baseline runtime comparison
    fpgrowth_time_sec = 0.0
    try:
        items_df = rdd.map(lambda b: (list(set(b)),)).toDF(["items"])
        t1 = time.perf_counter()
        fp = FPGrowth(itemsCol="items", minSupport=support)
        model = fp.fit(items_df)
        _ = model.freqItemsets.collect()
        fpgrowth_time_sec = time.perf_counter() - t1
    except (ImportError, RuntimeError):
        fpgrowth_time_sec = -1.0

    # Write results to CSV
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["tagset", "size", "count", "support", "son_time_sec", "fpgrowth_time_sec"])
        for itemset, count in son_results:
            tagset_str = " ".join(sorted(itemset))
            sup_val = count / total_baskets if total_baskets > 0 else 0.0
            writer.writerow(
                [
                    tagset_str,
                    len(itemset),
                    count,
                    f"{sup_val:.6f}",
                    f"{son_time_sec:.4f}",
                    f"{fpgrowth_time_sec:.4f}",
                ]
            )

    return {"son_time_sec": son_time_sec, "fpgrowth_time_sec": fpgrowth_time_sec}


def parse_args(args: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for SON script."""
    parser = argparse.ArgumentParser(
        description="Mine frequent tag sets using two-pass SON algorithm."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input path to question tags (Parquet or text format)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help=(
            "Output CSV path for frequent tag sets (e.g. results/e6_blocking/frequent_tagsets.csv)"
        ),
    )
    parser.add_argument(
        "--support",
        type=float,
        default=0.01,
        help="Support threshold fraction s in (0.0, 1.0] (default: 0.01)",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=None,
        help="Maximum itemset size to discover (default: None, no limit)",
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    return parser.parse_args(args)


def main() -> None:
    """Entry point for running SON script from CLI."""
    args = parse_args()
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = builder.master(args.master).appName("DupHunter-SON-FrequentTagSets").getOrCreate()
    try:
        run_son(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            support=args.support,
            max_size=args.max_size,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
