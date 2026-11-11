"""NumPy MinHash signatures with a map-only Spark DataFrame wrapper."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import Any

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, LongType, StructField, StructType


PRIME = 2_147_483_647
SHINGLE_MASK = PRIME
EMPTY_SIGNATURE_VALUE = PRIME


@dataclass(frozen=True)
class MinHashConfig:
    """Configuration for a reproducible MinHash hash family."""

    n: int = 128
    seed: int = 42

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ValueError("n must be at least 1")


def _hash_coefficients(n: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Generate prefix-stable coefficients for h(x) = (a*x+b) mod PRIME."""
    generator = np.random.default_rng(seed)
    pairs = generator.integers(
        1,
        PRIME,
        size=(n, 2),
        dtype=np.int64,
    )
    return pairs[:, 0], pairs[:, 1]


def _normalize_shingles(shingles: Iterable[int] | np.ndarray) -> np.ndarray:
    values = np.asarray(list(shingles) if not isinstance(shingles, np.ndarray) else shingles)
    if values.ndim != 1:
        raise ValueError("shingles must be a one-dimensional collection")
    if values.size == 0:
        return np.empty(0, dtype=np.int64)
    if values.dtype.kind not in "iu":
        raise TypeError("shingles must contain integers")
    return np.bitwise_and(values.astype(np.int64, copy=False), SHINGLE_MASK)


def _signature_from_coefficients(
    shingles: np.ndarray,
    a: np.ndarray,
    b: np.ndarray,
) -> np.ndarray:
    if shingles.size == 0:
        return np.full(a.size, EMPTY_SIGNATURE_VALUE, dtype=np.int64)
    hashed = (
        a[:, np.newaxis] * shingles[np.newaxis, :]
        + b[:, np.newaxis]
    ) % PRIME
    return np.min(hashed, axis=1)


def compute_signature(
    shingles: Iterable[int] | np.ndarray,
    n: int = 128,
    seed: int = 42,
) -> np.ndarray:
    """Compute one length-``n`` signature for integer shingle hashes.

    Input hashes are masked to 31 bits. Empty sets receive the documented
    sentinel ``PRIME`` in every position; non-empty signature values are in
    ``[0, PRIME)``. Reusing a seed yields a stable hash family, and shorter
    signatures are prefixes of longer signatures with the same seed.
    """
    config = MinHashConfig(n=n, seed=seed)
    values = _normalize_shingles(shingles)
    a, b = _hash_coefficients(config.n, config.seed)
    return _signature_from_coefficients(values, a, b)


def _partition_signatures(
    rows: Iterator[Any],
    id_index: int,
    shingles_index: int,
    a: np.ndarray,
    b: np.ndarray,
) -> Iterator[tuple[Any, list[int]]]:
    for row in rows:
        values = _normalize_shingles(row[shingles_index] or [])
        signature = _signature_from_coefficients(values, a, b)
        yield row[id_index], signature.tolist()


def build_signatures(
    posts: DataFrame,
    config: MinHashConfig = MinHashConfig(),
    *,
    post_id_col: str = "post_id",
    shingles_col: str = "shingles",
) -> DataFrame:
    """Add MinHash signatures to rows using Spark ``mapPartitions`` only.

    The returned C3 DataFrame contains ``post_id`` and ``signature`` columns.
    """
    required = {post_id_col, shingles_col}
    missing = required.difference(posts.columns)
    if missing:
        raise ValueError(f"input DataFrame is missing columns: {sorted(missing)}")
    if not isinstance(posts.sparkSession, SparkSession):
        raise TypeError("posts must be a Spark DataFrame")

    id_field = posts.schema[post_id_col]
    if isinstance(id_field.dataType, ArrayType):
        raise TypeError(f"{post_id_col} must be a scalar identifier column")

    id_index = posts.columns.index(post_id_col)
    shingles_index = posts.columns.index(shingles_col)
    a, b = _hash_coefficients(config.n, config.seed)
    signatures = posts.rdd.mapPartitions(
        partial(
            _partition_signatures,
            id_index=id_index,
            shingles_index=shingles_index,
            a=a,
            b=b,
        )
    )
    output_schema = StructType(
        [
            StructField("post_id", id_field.dataType, id_field.nullable),
            StructField(
                "signature",
                ArrayType(LongType(), containsNull=False),
                nullable=False,
            ),
        ]
    )
    return posts.sparkSession.createDataFrame(signatures, output_schema)
