"""Bucket-splitting (salting) policy for oversized LSH buckets (#41)."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DEFAULT_SALTS = 2


def _salt_pair_keys(chunk: int, num_chunks: int) -> list[str]:
    """Return sorted ``i:j`` sub-bucket keys for ``chunk`` across ``0 <= i < j < num_chunks``."""
    keys: list[str] = []
    for i in range(num_chunks):
        for j in range(i + 1, num_chunks):
            if chunk in (i, j):
                keys.append(f"{i}:{j}")
    return keys


def _apply_salt_split(
    df: DataFrame,
    *,
    k: int = DEFAULT_SALTS,
    min_bucket_size: int = 2,
) -> DataFrame:
    """Split buckets larger than ``min_bucket_size`` into chunk-pair sub-buckets."""
    if k < 1:
        raise ValueError(f"k must be at least 1, got {k}")
    if k == 1 or df.rdd.isEmpty():
        return df

    input_cols = list(df.columns)
    num_chunks = 3 if k == 2 else k
    pair_map_expr = F.create_map(
        *[
            arg
            for c in range(num_chunks)
            for arg in (F.lit(c), F.array(*[F.lit(tag) for tag in _salt_pair_keys(c, num_chunks)]))
        ]
    )

    order_col = next((c for c in ("post_id", "id") if c in input_cols), None)
    order_expr = F.col(order_col) if order_col is not None else F.monotonically_increasing_id()

    bucket_win = Window.partitionBy("band", "bucket_key")
    ordered_win = bucket_win.orderBy(order_expr)

    with_meta = df.withColumn("_bucket_size", F.count(F.lit(1)).over(bucket_win)).withColumn(
        "_chunk", (F.row_number().over(ordered_win) - F.lit(1)) % F.lit(num_chunks)
    )

    small_buckets = with_meta.filter(F.col("_bucket_size") <= min_bucket_size).select(*input_cols)
    large_buckets = (
        with_meta.filter(F.col("_bucket_size") > min_bucket_size)
        .withColumn("_salt_tag", F.explode(pair_map_expr[F.col("_chunk")]))
        .withColumn("bucket_key", F.concat_ws(":salt:", F.col("bucket_key"), F.col("_salt_tag")))
        .select(*input_cols)
    )
    return small_buckets.unionByName(large_buckets)


def salt_split(
    df_or_k: DataFrame | int | None = None,
    k: int = DEFAULT_SALTS,
    *,
    min_bucket_size: int = 2,
) -> DataFrame | Callable[[DataFrame], DataFrame]:
    """Contract Hook 1 policy that splits big buckets into chunk pairs.

    Supports both direct DataFrame invocation (for ``--bucket-policy
    src.verification.salt_split:salt_split``) and factory usage ``salt_split(k=2)``.
    """
    if isinstance(df_or_k, DataFrame):
        return _apply_salt_split(df_or_k, k=k, min_bucket_size=min_bucket_size)

    resolved_k = k if df_or_k is None else int(df_or_k)
    if resolved_k < 1:
        raise ValueError(f"k must be at least 1, got {resolved_k}")

    def policy(df: DataFrame) -> DataFrame:
        return _apply_salt_split(df, k=resolved_k, min_bucket_size=min_bucket_size)

    policy.__name__ = f"salt_split_{resolved_k}"
    return policy
