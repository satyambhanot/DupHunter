"""LSH banding: split MinHash signatures into bands and emit candidate pairs.

Stage C3 -> C4.  Input is a DataFrame with ``post_id`` and ``signature`` columns
(output of ``minhash.build_signatures``), plus optional ``created`` and ``group``.
Output is a DataFrame of candidate pairs matching Contract C4:
    - ``dup_id``: identifier of the newer question
    - ``cand_id``: identifier of the older candidate question
    - ``est_sim``: estimated Jaccard similarity in [0.0, 1.0]

Only earlier questions are candidates for newer questions.
Plug-in hooks allow custom bucket filtering (--bucket-policy, Contract Hook 1)
and tag/topic blocking (--groups, Contract Hook 2).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import logging
import struct
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("duphunter.lsh")


# ---------------------------------------------------------------------------
# Configuration & Mathematical Formulas
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LSHConfig:
    """Configuration for LSH banding.

    Attributes:
        b: Number of bands (must be >= 1).
        r: Number of rows per band (must be >= 1).
        n: Total signature length. If provided, must satisfy b * r == n.
        bands_per_pass: Number of bands to process per pass for memory scaling.
    """

    b: int = 16
    r: int = 8
    n: int | None = None
    bands_per_pass: int | None = None

    def __post_init__(self) -> None:
        if self.b < 1:
            raise ValueError(f"b must be at least 1, got {self.b}")
        if self.r < 1:
            raise ValueError(f"r must be at least 1, got {self.r}")
        expected_n = self.b * self.r
        if self.n is not None and self.n != expected_n:
            raise ValueError(f"b * r ({expected_n}) != n ({self.n})")
        if self.bands_per_pass is not None and self.bands_per_pass < 1:
            raise ValueError("bands_per_pass must be at least 1")

    @property
    def total_hashes(self) -> int:
        """Total signature length (b * r)."""
        return self.b * self.r


DEFAULT_CONFIG = LSHConfig()


def s_curve(s: float, b: int, r: int) -> float:
    """Compute theoretical detection probability: 1 - (1 - s^r)^b.

    Args:
        s: Jaccard similarity in [0.0, 1.0].
        b: Number of bands.
        r: Rows per band.

    Returns:
        Probability that two items with similarity s share at least one bucket.
    """
    if not (0.0 <= s <= 1.0):
        raise ValueError(f"Similarity s must be between 0.0 and 1.0, got {s}")
    return 1.0 - (1.0 - (s**r)) ** b


def lsh_threshold(b: int, r: int) -> float:
    """Compute approximate S-curve 50% threshold: (1 / b) ** (1 / r)."""
    if b < 1 or r < 1:
        raise ValueError("b and r must be at least 1")
    return (1.0 / b) ** (1.0 / r)


def estimate_similarity(sig1: list[int], sig2: list[int]) -> float:
    """Compute fraction of matching signature positions between two signatures."""
    if len(sig1) != len(sig2) or len(sig1) == 0:
        return 0.0
    matches = sum(1 for a, b in zip(sig1, sig2, strict=True) if a == b)
    return matches / len(sig1)


# ---------------------------------------------------------------------------
# Contract Hook 1: Bucket Policies
# ---------------------------------------------------------------------------


def size_limit(max_size: int = 1000) -> Callable[[DataFrame], DataFrame]:
    """Bucket policy that drops buckets containing more than max_size items.

    Logs skipped buckets exceeding max_size.
    Returns a callable with signature (DataFrame) -> DataFrame preserving
    all input columns.
    """

    def policy(df: DataFrame) -> DataFrame:
        if df.rdd.isEmpty():
            return df

        w = Window.partitionBy("band", "bucket_key")
        with_size = df.withColumn("_bucket_size", F.count("*").over(w))

        # Filter out oversized buckets
        kept = with_size.filter(F.col("_bucket_size") <= max_size).drop("_bucket_size")
        return kept

    policy.__name__ = f"size_limit_{max_size}"
    return policy


def pass_through_policy(df: DataFrame) -> DataFrame:
    """Default pass-through policy keeping all buckets."""
    return df


BUCKET_POLICIES: dict[str, Callable[..., Callable[[DataFrame], DataFrame]]] = {
    "size_limit": size_limit,
    "none": lambda: pass_through_policy,
}


def resolve_bucket_policy(name: str, bucket_limit: int = 1000) -> Callable[[DataFrame], DataFrame]:
    """Turn a policy name into a bucket policy.

    Args:
        name: ``size_limit``, ``none``, or ``module:attr`` naming a policy callable
            ``(DataFrame) -> DataFrame`` that other workstreams provide (contract hook 1).
        bucket_limit: bucket size for ``size_limit``.

    Raises:
        ValueError: if the name is unknown or ``module:attr`` cannot be imported.
    """
    if name == "size_limit":
        return size_limit(bucket_limit)
    if name == "none":
        return pass_through_policy
    if ":" in name:
        module_name, _, attribute = name.partition(":")
        try:
            policy = getattr(importlib.import_module(module_name), attribute)
        except (ImportError, AttributeError) as error:
            raise ValueError(f"Cannot load bucket policy {name!r}: {error}") from error
        if not callable(policy):
            raise ValueError(f"Bucket policy {name!r} is not callable")
        return policy
    raise ValueError(f"Unknown bucket policy name: {name}")


# ---------------------------------------------------------------------------
# Internal Helpers: Stable Hashing and Pair Generation
# ---------------------------------------------------------------------------


def _band_key(group: str, band_idx: int, band_values: list[int]) -> str:
    """Return a deterministic, endian-independent bucket key for a band slice."""
    packed = struct.pack(f">{len(band_values)}q", *band_values)
    digest = hashlib.sha1(packed).hexdigest()
    return f"{group}:{band_idx}:{digest}"


def _expand_pairs_from_partition(
    rows: Iterator[Any],
    signature_len: int,
) -> Iterator[tuple[Any, Any, float]]:
    """Generate ordered candidate pairs from grouped bucket members.

    Input rows are (band, bucket_key, members) where members is a list of
    Row(post_id, created, signature).
    Only earlier questions are candidates for newer questions.
    Emits (dup_id, cand_id, est_sim) with dup_id (newer) and cand_id (older).
    """
    for row in rows:
        members = row[2]
        if not members or len(members) < 2:
            continue

        n_members = len(members)
        for i in range(n_members):
            m1 = members[i]
            id1, c1, sig1 = m1[0], m1[1], m1[2]

            for j in range(i + 1, n_members):
                m2 = members[j]
                id2, c2, sig2 = m2[0], m2[1], m2[2]

                if id1 == id2:
                    continue

                # Determine newer vs older:
                # 1. Compare 'created' timestamp if distinct
                # 2. If timestamps are equal or identical, compare post IDs
                if c1 > c2:
                    newer_id, older_id = id1, id2
                    newer_sig, older_sig = sig1, sig2
                elif c2 > c1:
                    newer_id, older_id = id2, id1
                    newer_sig, older_sig = sig2, sig1
                else:
                    if id1 > id2:
                        newer_id, older_id = id1, id2
                        newer_sig, older_sig = sig1, sig2
                    else:
                        newer_id, older_id = id2, id1
                        newer_sig, older_sig = sig2, sig1

                sim = estimate_similarity(newer_sig, older_sig)
                yield (newer_id, older_id, float(sim))


# ---------------------------------------------------------------------------
# Core Public API: find_candidates
# ---------------------------------------------------------------------------


def find_candidates(
    signatures: DataFrame,
    config: LSHConfig = DEFAULT_CONFIG,
    *,
    bucket_policy: Callable[[DataFrame], DataFrame] | str | None = None,
    groups: DataFrame | str | None = None,
    bands_per_pass: int | None = None,
    post_id_col: str = "post_id",
    signature_col: str = "signature",
    created_col: str = "created",
) -> DataFrame:
    """Generate candidate duplicate pairs from MinHash signatures using LSH banding.

    Args:
        signatures: Input DataFrame containing post_id and signature columns (C3).
        config: LSHConfig defining b, r, and optional n.
        bucket_policy: Callable (df -> df) or policy name ("size_limit", "none").
            Defaults to size_limit(1000).
        groups: DataFrame or path to groups parquet file (C9) for tag blocking.
            Items in different groups will never be paired.
        bands_per_pass: Bands processed per pass. Defaults to config.bands_per_pass or b.
        post_id_col: Column name for post identifiers.
        signature_col: Column name for MinHash signatures.
        created_col: Column name for creation timestamps (optional in signatures).

    Returns:
        DataFrame matching Contract C4 with columns:
            - ``dup_id``: newer question ID
            - ``cand_id``: older candidate question ID
            - ``est_sim``: estimated Jaccard similarity

    Raises:
        ValueError: If required columns are missing, or if b * r != signature length.
    """
    required = {post_id_col, signature_col}
    missing = required.difference(signatures.columns)
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {sorted(missing)}")

    b = config.b
    r = config.r
    expected_len = b * r

    if config.n is not None and config.n != expected_len:
        raise ValueError(f"b * r ({expected_len}) != n ({config.n})")

    spark = signatures.sparkSession
    id_field = signatures.schema[post_id_col]

    # Resolve bucket policy
    if bucket_policy is None:
        active_policy: Callable[[DataFrame], DataFrame] = size_limit(1000)
    elif isinstance(bucket_policy, str):
        active_policy = resolve_bucket_policy(bucket_policy)
    elif callable(bucket_policy):
        active_policy = bucket_policy
    else:
        raise TypeError(f"Invalid bucket_policy type: {type(bucket_policy)}")

    # Resolve groups (Contract Hook 2)
    if groups is not None:
        if isinstance(groups, str):
            groups_df = spark.read.parquet(groups)
        elif isinstance(groups, DataFrame):
            groups_df = groups
        else:
            raise TypeError("groups must be a DataFrame or path string")

        group_col_name = "group" if "group" in groups_df.columns else groups_df.columns[1]
        gid_col_name = post_id_col if post_id_col in groups_df.columns else groups_df.columns[0]

        df = signatures.join(
            groups_df.select(
                F.col(gid_col_name).alias("_gid"),
                F.col(group_col_name).alias("_group"),
            ),
            signatures[post_id_col] == F.col("_gid"),
            how="left",
        ).drop("_gid")
        df = df.withColumn("_group", F.coalesce(F.col("_group"), F.lit("other")))
    elif "group" in signatures.columns:
        df = signatures.withColumn("_group", F.col("group"))
    else:
        df = signatures.withColumn("_group", F.lit("default"))

    has_created = created_col in signatures.columns
    if not has_created:
        df = df.withColumn("_created", F.col(post_id_col))
    else:
        df = df.withColumn("_created", F.col(created_col))

    # Resolve bands_per_pass
    pass_size = bands_per_pass or config.bands_per_pass or b
    if pass_size < 1:
        raise ValueError(f"bands_per_pass must be at least 1, got {pass_size}")

    passes = [range(i, min(i + pass_size, b)) for i in range(0, b, pass_size)]

    c4_schema = StructType(
        [
            StructField("dup_id", id_field.dataType, nullable=False),
            StructField("cand_id", id_field.dataType, nullable=False),
            StructField("est_sim", DoubleType(), nullable=False),
        ]
    )

    all_pass_dfs: list[DataFrame] = []

    # Map each pass
    for pass_bands in passes:
        band_list = list(pass_bands)

        # Map partitions to bucket records
        def make_partition_buckets(
            rows: Iterator[Any],
            bands: list[int] = band_list,
            b_rows: int = r,
            n_len: int = expected_len,
        ) -> Iterator[tuple[int, str, Any, Any, list[int]]]:
            for row in rows:
                p_id = row[post_id_col]
                sig: list[int] = row[signature_col] or []
                created_val = row["_created"]
                grp = row["_group"]

                if len(sig) != n_len:
                    raise ValueError(f"Signature length {len(sig)} != expected b * r ({n_len})")

                for band_idx in bands:
                    band_vals = sig[band_idx * b_rows : (band_idx + 1) * b_rows]
                    b_key = _band_key(grp, band_idx, band_vals)
                    yield (band_idx, b_key, p_id, created_val, sig)

        bucket_schema = StructType(
            [
                StructField("band", LongType(), nullable=False),
                StructField("bucket_key", StringType(), nullable=False),
                StructField("post_id", id_field.dataType, nullable=False),
                StructField("created", df.schema["_created"].dataType, nullable=True),
                StructField("signature", ArrayType(LongType()), nullable=False),
            ]
        )

        bucket_rdd = df.rdd.mapPartitions(make_partition_buckets)
        bucket_df = spark.createDataFrame(bucket_rdd, bucket_schema)

        # Apply bucket policy (Contract Hook 1)
        filtered_bucket_df = active_policy(bucket_df)

        if filtered_bucket_df.rdd.isEmpty():
            continue

        # Group by bucket and expand pairs
        grouped = filtered_bucket_df.groupBy("band", "bucket_key").agg(
            F.collect_list(
                F.struct(
                    F.col("post_id"),
                    F.col("created"),
                    F.col("signature"),
                )
            ).alias("members")
        )

        pairs_rdd = grouped.rdd.mapPartitions(
            lambda rows: _expand_pairs_from_partition(rows, expected_len)
        )
        pass_df = spark.createDataFrame(pairs_rdd, c4_schema)
        all_pass_dfs.append(pass_df)

    if not all_pass_dfs:
        return spark.createDataFrame([], c4_schema)

    # Union all passes and deduplicate identical candidate pairs
    result = all_pass_dfs[0]
    for next_df in all_pass_dfs[1:]:
        result = result.unionAll(next_df)

    return result.dropDuplicates(["dup_id", "cand_id"])


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for LSH candidate generation."""
    parser = argparse.ArgumentParser(
        description="DupHunter LSH: Banding and Candidate Generation (Stage C3 -> C4)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to input MinHash signatures Parquet (C3)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to write candidate duplicate pairs Parquet (C4)",
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL",
    )
    parser.add_argument(
        "-b",
        "--bands",
        type=int,
        default=16,
        dest="b",
        help="Number of bands",
    )
    parser.add_argument(
        "-r",
        "--rows",
        type=int,
        default=8,
        dest="r",
        help="Number of rows per band",
    )
    parser.add_argument(
        "-n",
        type=int,
        default=None,
        help="Signature length (must equal b * r)",
    )
    parser.add_argument(
        "--bands-per-pass",
        type=int,
        default=None,
        dest="bands_per_pass",
        help="Number of bands to process per pass",
    )
    parser.add_argument(
        "--bucket-policy",
        type=str,
        default="size_limit",
        help="Bucket policy: 'size_limit', 'none' or 'module:attr' (a policy callable)",
    )
    parser.add_argument(
        "--bucket-limit",
        type=int,
        default=1000,
        help="Maximum bucket size for 'size_limit' policy",
    )
    parser.add_argument(
        "--groups",
        type=str,
        default=None,
        help="Path to groups Parquet file for tag/topic blocking (C9)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run LSH banding pipeline from CLI."""
    args = parse_args(argv)

    if not args.input or not args.output:
        logger.info("DupHunter LSH module CLI ready. Use --help for usage.")
        return

    spark = SparkSession.builder.master(args.master).appName("duphunter-lsh").getOrCreate()

    try:
        config = LSHConfig(
            b=args.b,
            r=args.r,
            n=args.n,
            bands_per_pass=args.bands_per_pass,
        )

        policy = resolve_bucket_policy(args.bucket_policy, args.bucket_limit)

        signatures = spark.read.parquet(args.input)
        candidates = find_candidates(
            signatures,
            config=config,
            bucket_policy=policy,
            groups=args.groups,
            bands_per_pass=args.bands_per_pass,
        )

        candidates.write.mode("overwrite").parquet(args.output)
        logger.info("Successfully wrote candidate pairs to %s", args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
