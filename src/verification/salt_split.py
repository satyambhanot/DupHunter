"""Bucket-splitting (salting) policy for oversized LSH buckets (#41).

A bucket with more than ``min_bucket_size`` members is cut into ``chunks`` chunks by a hash of the
member id. Every pair of chunks ``i < j`` becomes one sub-bucket ``bucket_key:salt:i:j`` holding
the members of both chunks. Two members in different chunks meet in exactly one sub-bucket and two
members in the same chunk meet in ``chunks - 1`` of them, so no candidate pair is lost, no
sub-bucket holds more than about ``2 / chunks`` of the members, and the pair work in total is
about ``2 * (chunks - 1) / chunks`` times the original (4/3 for 3 chunks). The gain is a smaller
slowest task, not less work in total.

The chunk comes from ``hash(id) mod chunks`` and the bucket sizes from a ``groupBy`` whose small
result is broadcast, so the policy never sorts or counts a whole bucket inside one task.

Use it from the command line as ``--bucket-policy src.verification.salt_split:salt_split``
(defaults) or from Python as ``make_salt_split(chunks=4, min_bucket_size=500)``.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_CHUNKS = 3
DEFAULT_MIN_BUCKET_SIZE = 1000  # the same limit as the size_limit policy
_ID_COLUMNS = ("post_id", "id")


def _salt_pair_keys(chunk: int, num_chunks: int) -> list[str]:
    """Return the ``i:j`` sub-bucket keys that contain ``chunk`` (``0 <= i < j < num_chunks``)."""
    keys: list[str] = []
    for i in range(num_chunks):
        for j in range(i + 1, num_chunks):
            if chunk in (i, j):
                keys.append(f"{i}:{j}")
    return keys


def _apply_salt_split(df: DataFrame, *, chunks: int, min_bucket_size: int) -> DataFrame:
    """Split every bucket larger than ``min_bucket_size`` into chunk-pair sub-buckets."""
    if chunks < 3:
        raise ValueError(
            f"chunks must be at least 3, got {chunks} (with 2 chunks there is a single pair key, "
            "so nothing would be split)"
        )
    if min_bucket_size < 1:
        raise ValueError(f"min_bucket_size must be at least 1, got {min_bucket_size}")

    input_cols = list(df.columns)
    id_col = next((c for c in _ID_COLUMNS if c in input_cols), None)
    if id_col is None:
        raise ValueError(f"salt_split needs one of the columns {_ID_COLUMNS}, got {input_cols}")

    # Sizes come from a map-side combined groupBy; only the keys of the big buckets are kept
    # and broadcast, so the rows of a huge bucket are never shuffled to a single task.
    big_keys = (
        df.groupBy("band", "bucket_key")
        .count()
        .filter(F.col("count") > min_bucket_size)
        .select("band", "bucket_key")
        .withColumn("_is_big", F.lit(True))
    )
    tagged = df.join(F.broadcast(big_keys), ["band", "bucket_key"], "left")

    pair_keys = F.array(
        *[
            F.array(*[F.lit(key) for key in _salt_pair_keys(chunk, chunks)])
            for chunk in range(chunks)
        ]
    )
    small = tagged.filter(F.col("_is_big").isNull()).select(*input_cols)
    large = (
        tagged.filter(F.col("_is_big").isNotNull())
        .withColumn("_chunk", F.pmod(F.hash(F.col(id_col)), F.lit(chunks)))
        .withColumn("_salt_tag", F.explode(pair_keys[F.col("_chunk")]))
        .withColumn("bucket_key", F.concat_ws(":salt:", F.col("bucket_key"), F.col("_salt_tag")))
        .select(*input_cols)
    )
    return small.unionByName(large)


def make_salt_split(
    chunks: int = DEFAULT_CHUNKS, min_bucket_size: int = DEFAULT_MIN_BUCKET_SIZE
) -> Callable[[DataFrame], DataFrame]:
    """Return a Contract Hook 1 policy ``DataFrame -> DataFrame`` with the given settings.

    Args:
        chunks: number of chunks a big bucket is cut into (at least 3; ``k`` in issue #41).
        min_bucket_size: only buckets with more members than this are split.
    """
    if chunks < 3:
        raise ValueError(f"chunks must be at least 3, got {chunks}")
    if min_bucket_size < 1:
        raise ValueError(f"min_bucket_size must be at least 1, got {min_bucket_size}")

    def policy(df: DataFrame) -> DataFrame:
        return _apply_salt_split(df, chunks=chunks, min_bucket_size=min_bucket_size)

    policy.__name__ = f"salt_split_{chunks}_{min_bucket_size}"
    return policy


def salt_split(df: DataFrame) -> DataFrame:
    """Contract Hook 1 policy with the default settings (3 chunks, buckets over 1000 members).

    This is what ``--bucket-policy src.verification.salt_split:salt_split`` loads. Use
    ``make_salt_split`` for other settings.
    """
    return _apply_salt_split(df, chunks=DEFAULT_CHUNKS, min_bucket_size=DEFAULT_MIN_BUCKET_SIZE)
