"""Tests for the Spark ML MinHashLSH baseline (Issue #11)."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.baselines.spark_baseline import spark_minhash_pairs


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Start a small local Spark session for baseline tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-spark-minhash-baseline")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_minhash_finds_near_duplicate_drops_empty_and_matches_c4(
    spark: SparkSession,
) -> None:
    """The 0.6 similarity cutoff includes a J=0.9 pair, not a disjoint pair."""
    common = list(range(1, 11))
    shingles = spark.createDataFrame(
        [
            (1, common),
            (2, [*common, 11]),
            (3, [100, 101]),
            (4, []),
        ],
        ["post_id", "shingles"],
    )

    pairs, empty_count = spark_minhash_pairs(
        shingles,
        similarity_threshold=0.6,
        num_hash_tables=10,
    )

    assert pairs.columns == ["dup_id", "cand_id", "est_sim"]
    assert empty_count == 1
    rows = pairs.collect()
    assert [(row.dup_id, row.cand_id) for row in rows] == [(2, 1)]
    assert rows[0].est_sim == pytest.approx(10 / 11)


@pytest.mark.parametrize("threshold", [-0.1, 1.1, float("nan"), float("inf")])
def test_rejects_invalid_similarity_threshold(spark: SparkSession, threshold: float) -> None:
    """Similarity thresholds must be finite values from zero to one."""
    shingles = spark.createDataFrame([(1, [1])], ["post_id", "shingles"])
    with pytest.raises(ValueError, match="similarity_threshold must be between 0 and 1"):
        spark_minhash_pairs(shingles, similarity_threshold=threshold)


def test_rejects_nonpositive_hash_table_count(spark: SparkSession) -> None:
    """At least one MinHash table is required."""
    shingles = spark.createDataFrame([(1, [1])], ["post_id", "shingles"])
    with pytest.raises(ValueError, match="num_hash_tables must be greater than zero"):
        spark_minhash_pairs(shingles, num_hash_tables=0)


def test_several_empty_rows_are_dropped_without_a_crash(spark: SparkSession) -> None:
    """Spark's hash step must never see a question without shingles."""
    common = list(range(1, 11))
    shingles = spark.createDataFrame(
        [(1, common), (2, [*common, 11]), (3, []), (4, [])],
        "post_id long, shingles array<long>",
    )
    pairs, empty_count = spark_minhash_pairs(shingles, 0.6, num_hash_tables=10)
    assert empty_count == 2
    assert [(row.dup_id, row.cand_id) for row in pairs.collect()] == [(2, 1)]


@pytest.mark.parametrize(
    ("threshold", "other", "expected_similarity"),
    [(1.0, [1, 2, 3, 4], 1.0), (0.6, [1, 2, 3, 5], 0.6)],
)
def test_threshold_is_inclusive(
    spark: SparkSession, threshold: float, other: list[int], expected_similarity: float
) -> None:
    """Identical sets pass 1.0, and a pair exactly at the cutoff is kept."""
    shingles = spark.createDataFrame([(1, [1, 2, 3, 4]), (2, other)], ["post_id", "shingles"])
    pairs, _ = spark_minhash_pairs(shingles, threshold, num_hash_tables=20)
    (row,) = pairs.collect()
    assert row.est_sim == pytest.approx(expected_similarity)


def test_same_seed_gives_the_same_pairs(spark: SparkSession) -> None:
    """A repeated call, with the rows split differently, returns identical candidates."""
    rows = [(i, [i % 7, i % 11 + 20, i % 5 + 40]) for i in range(1, 61)]
    shingles = spark.createDataFrame(rows, ["post_id", "shingles"])

    def run(frame):
        pairs, _ = spark_minhash_pairs(frame, 0.2, num_hash_tables=2, seed=7)
        return sorted((r.dup_id, r.cand_id, r.est_sim) for r in pairs.collect())

    first = run(shingles)
    assert first
    assert run(shingles.repartition(5)) == first
