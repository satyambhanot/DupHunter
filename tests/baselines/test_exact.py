"""Tests for the exact Jaccard baseline (Issue #10)."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.baselines.exact import exact_jaccard_pairs


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Start a small local Spark session for baseline tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-exact-baseline")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _as_pairs(dataframe: DataFrame) -> list[tuple[int, int, float]]:
    """Collect C4 rows in a stable order for concise assertions."""
    return [
        (row.dup_id, row.cand_id, row.est_sim)
        for row in dataframe.orderBy("dup_id", "cand_id").collect()
    ]


def test_sparse_blocks_and_spark_join_match_issue_example(spark: SparkSession) -> None:
    """Both engines return the three shared-shingle pairs from the issue example."""
    shingles = spark.createDataFrame(
        [
            (1, [1, 2, 3]),
            (2, [2, 3, 4]),
            (3, [9]),
            (4, [1, 2, 3]),
        ],
        ["post_id", "shingles"],
    )

    expected = [(2, 1, 0.5), (4, 1, 1.0), (4, 2, 0.5)]
    for engine in ("blocks", "join"):
        actual = _as_pairs(exact_jaccard_pairs(shingles, 0.1, engine=engine))
        assert actual == expected


def test_block_size_does_not_change_pairs_or_scores(spark: SparkSession) -> None:
    """Different sparse-block sizes produce the same exact results."""
    shingles = spark.createDataFrame(
        [(1, [1, 2, 3]), (2, [2, 3, 4]), (4, [1, 2, 3])],
        ["post_id", "shingles"],
    )

    one_question_blocks = _as_pairs(
        exact_jaccard_pairs(shingles, 0.1, engine="sparse_blocks", block_size=1)
    )
    larger_blocks = _as_pairs(
        exact_jaccard_pairs(shingles, 0.1, engine="sparse_blocks", block_size=3)
    )
    assert one_question_blocks == larger_blocks


def test_threshold_is_inclusive_and_uses_newer_question(spark: SparkSession) -> None:
    """A pair exactly at the threshold is kept and points from newer to older."""
    shingles = spark.createDataFrame(
        [
            (20, "2024-01-01", [1, 2, 3]),
            (10, "2024-01-02", [1, 2, 4]),
        ],
        ["post_id", "created", "shingles"],
    )

    actual = _as_pairs(exact_jaccard_pairs(shingles, 0.5, engine="sparse_blocks"))
    assert actual == [(10, 20, 0.5)]


@pytest.mark.parametrize("threshold", [-0.1, 1.1, float("nan"), float("inf")])
def test_rejects_invalid_threshold(spark: SparkSession, threshold: float) -> None:
    """Thresholds must be finite similarities between zero and one."""
    shingles = spark.createDataFrame([(1, [1])], ["post_id", "shingles"])
    with pytest.raises(ValueError, match="threshold must be between 0 and 1"):
        exact_jaccard_pairs(shingles, threshold)


def test_rejects_unknown_engine(spark: SparkSession) -> None:
    """Engine typos fail clearly rather than changing the baseline silently."""
    shingles = spark.createDataFrame([(1, [1])], ["post_id", "shingles"])
    with pytest.raises(ValueError, match="engine must be one of"):
        exact_jaccard_pairs(shingles, 0.1, engine="approximate")
