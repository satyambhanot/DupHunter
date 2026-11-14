"""Unit tests for E7 big-bucket profiling and mitigation policies (#30)."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.similarity.lsh import resolve_bucket_policy
from src.verification.buckets import (
    COMPARISON_COLUMNS,
    compare_bucket_strategies,
    count_lost_answer_pairs,
    profile_bucket_sizes,
    profile_buckets,
    size_limit,
)


class TestBucketProfiling:
    """Acceptance tests for bucket-size profiling."""

    def test_profile_bucket_sizes_acceptance_criterion(self) -> None:
        profile = profile_bucket_sizes([1, 1, 2, 10])
        assert profile.num_buckets == 4
        assert profile.total_items == 14
        assert profile.min_size == 1
        assert profile.max_size == 10
        assert profile.total_pairs == 46
        assert profile.largest_bucket_pairs == 45
        assert profile.largest_bucket_share == pytest.approx(45 / 46, abs=1e-3)
        assert round(profile.largest_bucket_share, 3) == 0.978

    def test_profile_buckets_spark_dataframe(self, spark) -> None:
        rows = (
            [(0, "b1", 1, 100)]
            + [(0, "b2", 2, 101)]
            + [(0, "b3", i, 100 + i) for i in range(10, 12)]
            + [(0, "b4", i, 200 + i) for i in range(20, 30)]
        )
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])
        profile = profile_buckets(df)
        assert profile.max_size == 10
        assert profile.total_pairs == 46
        assert profile.largest_bucket_share == pytest.approx(45 / 46, abs=1e-3)


class TestSizeLimitPolicy:
    """Acceptance tests for Contract Hook 1 ``size_limit`` policy."""

    def test_size_limit_drops_bucket_of_5_and_keeps_bucket_of_3(self, spark) -> None:
        rows = [(0, "drop5", i, 1000 + i) for i in range(5)] + [
            (0, "keep3", 100 + i, 2000 + i) for i in range(3)
        ]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])
        filtered = size_limit(3)(df)

        assert filtered.columns == df.columns
        kept_keys = {row["bucket_key"] for row in filtered.collect()}
        assert kept_keys == {"keep3"}
        assert filtered.count() == 3

    def test_size_limit_loads_via_resolve_bucket_policy(self) -> None:
        factory = resolve_bucket_policy("src.verification.buckets:size_limit")
        assert callable(factory)
        assert callable(factory(3))


class TestBucketComparison:
    """Tests for lost-pair counting and strategy comparison."""

    def test_count_lost_answer_pairs(self, spark) -> None:
        candidates = spark.createDataFrame(
            [(2, 1, 0.9), (4, 3, 0.8)],
            ["dup_id", "cand_id", "est_sim"],
        )
        answer_key = spark.createDataFrame(
            [(2, 1), (4, 3), (6, 5)],
            ["dup_id", "original_id"],
        )
        lost, total, share = count_lost_answer_pairs(candidates, answer_key)
        assert (lost, total) == (1, 3)
        assert share == pytest.approx(1 / 3)

    def test_compare_bucket_strategies_writes_csv(self, spark, tmp_path: Path) -> None:
        posts = spark.createDataFrame(
            [
                (
                    1,
                    100,
                    "python list sort",
                    "python list comprehension sort dictionary keys",
                    "python",
                    "python",
                ),
                (
                    2,
                    200,
                    "python list sort",
                    "python list comprehension sort dictionary keys",
                    "python",
                    "python",
                ),
                (
                    3,
                    300,
                    "django postgres",
                    "django migrate postgres database schema table",
                    "python",
                    "python",
                ),
                (
                    4,
                    400,
                    "django postgres",
                    "django migrate postgres database schema table",
                    "python",
                    "python",
                ),
            ],
            ["post_id", "created", "title", "body", "tags", "group"],
        )
        answer_key = spark.createDataFrame(
            [(2, 1), (4, 3)],
            ["dup_id", "original_id"],
        )
        out_csv = tmp_path / "e7_comparison.csv"

        rows = compare_bucket_strategies(
            posts=posts,
            answer_key=answer_key,
            strategies=("none", "size_limit", "drop_common_shingles"),
            bucket_limit=10,
            drop_common_share=0.5,
            shingle_k=3,
            num_perm=20,
            bands=5,
            rows_per_band=4,
            output_path=out_csv,
        )

        assert len(rows) == 3
        assert out_csv.exists()
        with out_csv.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == list(COMPARISON_COLUMNS)
            csv_rows = list(reader)
        assert [r["strategy"] for r in csv_rows] == ["none", "size_limit", "drop_common_shingles"]
