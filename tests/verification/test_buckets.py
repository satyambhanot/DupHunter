"""Unit tests for E7 big-bucket profiling and mitigation policies (#30)."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.similarity.lsh import LSHConfig, resolve_bucket_policy
from src.verification.buckets import (
    COMPARISON_COLUMNS,
    PROFILE_COLUMNS,
    compare_bucket_strategies,
    count_lost_answer_pairs,
    main,
    parse_args,
    profile_bucket_sizes,
    profile_buckets,
    profile_lsh_buckets,
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

    def test_compare_scores_a_sample_against_its_own_answer_key(self, spark) -> None:
        posts = spark.createDataFrame(
            [
                (1, 100, "python list sort", "x"),
                (2, 200, "python list sort", "x"),
                (3, 300, "django postgres", "x"),
            ],
            ["post_id", "created", "title", "body"],
        )
        # (900, 800) is outside the sample: it must not count as a lost pair
        answer_key = spark.createDataFrame(
            [(2, 1), (900, 800)],
            ["dup_id", "original_id"],
        )
        (row,) = compare_bucket_strategies(
            posts=posts,
            answer_key=answer_key,
            strategies=("none",),
            num_perm=16,
            bands=4,
            rows_per_band=4,
        )
        assert row["total_answer_pairs"] == 1
        assert row["lost_answer_pairs"] == 0
        assert row["recall@10"] == 1.0

    def test_bucket_profile_counts_what_the_size_limit_removes(self, spark) -> None:
        # one band of 2 rows. Posts 1-5 share a bucket of 5; 6 and 7 a bucket of 2; 8 is alone
        rows = [(i, 100 + i, [1, 1]) for i in range(1, 6)]
        rows += [(6, 106, [2, 2]), (7, 107, [2, 2]), (8, 108, [3, 3])]
        sigs = spark.createDataFrame(rows, ["post_id", "created", "signature"])
        key = spark.createDataFrame(
            [(2, 1), (4, 3), (7, 6), (8, 1), (900, 800)], ["dup_id", "original_id"]
        )
        profile = profile_lsh_buckets(sigs, key, LSHConfig(b=1, r=2, n=2), bucket_limit=3)
        assert (profile["num_buckets"], profile["max_size"], profile["total_pairs"]) == (3, 5, 11)
        assert profile["buckets_over_limit"] == 1
        assert profile["pairs_over_limit"] == 10
        assert profile["share_of_pairs_over_limit"] == pytest.approx(10 / 11)
        assert profile["answer_pairs"] == 4  # (900, 800) is outside the signatures
        assert profile["answer_pairs_sharing_a_bucket"] == 3  # not (8, 1)
        assert profile["answer_pairs_only_in_oversize_buckets"] == 2  # (2, 1) and (4, 3)

    def test_pair_that_also_shares_a_small_bucket_is_not_lost_to_the_limit(self, spark) -> None:
        # band 0 puts all four posts together; in band 1 only 1 and 2 meet
        rows = [(1, 1, [7, 5]), (2, 2, [7, 5]), (3, 3, [7, 6]), (4, 4, [7, 8])]
        sigs = spark.createDataFrame(rows, ["post_id", "created", "signature"])
        key = spark.createDataFrame([(2, 1), (4, 3)], ["dup_id", "original_id"])
        profile = profile_lsh_buckets(sigs, key, LSHConfig(b=2, r=1, n=2), bucket_limit=3)
        assert profile["answer_pairs_sharing_a_bucket"] == 2
        assert profile["answer_pairs_only_in_oversize_buckets"] == 1  # only (4, 3)

    def test_profile_command_writes_one_row(self, spark, tmp_path: Path) -> None:
        sigs = spark.createDataFrame(
            [(1, 1, [1, 1]), (2, 2, [1, 1])], ["post_id", "created", "signature"]
        )
        sigs.write.parquet(str(tmp_path / "sigs"))
        spark.createDataFrame([(2, 1)], ["dup_id", "original_id"]).write.parquet(
            str(tmp_path / "key")
        )
        out = tmp_path / "profile.csv"
        argv = ["--signatures", str(tmp_path / "sigs"), "--answer-key", str(tmp_path / "key")]
        argv += ["--bands", "1", "--rows", "2", "--num-perm", "2", "--profile-out", str(out)]
        assert main([*argv, "--master", "local[2]"]) == 0
        with out.open() as handle:
            (row,) = list(csv.DictReader(handle))
        assert list(row) == list(PROFILE_COLUMNS)
        assert row["answer_pairs_sharing_a_bucket"] == "1"

    def test_command_needs_exactly_one_input(self) -> None:
        for argv in (
            ["--answer-key", "k"],
            ["--answer-key", "k", "--posts", "p", "--signatures", "s"],
        ):
            with pytest.raises(SystemExit):
                parse_args(argv)

    def test_compare_rejects_unknown_fields(self, spark) -> None:
        posts = spark.createDataFrame([(1, 1, "t", "b")], ["post_id", "created", "title", "body"])
        key = spark.createDataFrame([(1, 1)], ["dup_id", "original_id"])
        with pytest.raises(ValueError, match="fields"):
            compare_bucket_strategies(posts, key, fields="body")
