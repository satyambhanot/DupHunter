"""Tests for the reproducible E5 exact benchmark (#10)."""

from __future__ import annotations

import csv

import pytest

from src.baselines import benchmark


def _posts(spark):
    rows = [
        (1, 100, "python list sort", "body one"),
        (2, 200, "python list order", "body two"),
        (3, 300, "django migrate table", "x"),
        (4, 400, "python list sort", "y"),
    ]
    return spark.createDataFrame(rows, ["post_id", "created", "title", "body"])


def test_samples_are_nested_and_use_the_title_only(spark):
    small = {r.post_id: r.shingles for r in benchmark.title_shingles(_posts(spark), 2).collect()}
    large = {r.post_id: r.shingles for r in benchmark.title_shingles(_posts(spark), 4).collect()}
    assert set(small) == {1, 2} and set(large) == {1, 2, 3, 4}
    assert all(large[i] == small[i] for i in small)
    assert len(large[1]) == 3  # three title words, nothing from the body


def test_digest_ignores_row_order_and_sees_a_changed_score(spark):
    schema = ["dup_id", "cand_id", "est_sim"]
    a = spark.createDataFrame([(2, 1, 0.5), (4, 1, 1.0)], schema)
    b = spark.createDataFrame([(4, 1, 1.0), (2, 1, 0.5)], schema)
    c = spark.createDataFrame([(4, 1, 1.0), (2, 1, 0.6)], schema)
    assert benchmark.pairs_digest(a) == benchmark.pairs_digest(b) != benchmark.pairs_digest(c)


def test_benchmark_times_both_engines_and_they_agree(spark):
    rows = benchmark.run_benchmark(_posts(spark), [2, 4], threshold=0.1)
    assert [(r["sample_size"], r["engine"]) for r in rows] == [
        (2, "sparse_blocks"), (2, "spark_join"), (4, "sparse_blocks"), (4, "spark_join"),
    ]  # fmt: skip
    assert rows[0]["n_candidates"] == 1 and rows[2]["n_candidates"] == 3
    assert rows[2]["digest"] == rows[3]["digest"]


def test_benchmark_fails_when_the_engines_disagree(spark, monkeypatch):
    real = benchmark.exact_jaccard_pairs

    def broken(df, threshold, engine, block_size):
        pairs = real(df, threshold=threshold, engine=engine, block_size=block_size)
        return pairs.limit(0) if engine == "spark_join" else pairs

    monkeypatch.setattr(benchmark, "exact_jaccard_pairs", broken)
    with pytest.raises(RuntimeError, match="disagree"):
        benchmark.run_benchmark(_posts(spark), [4])


def test_command_writes_runs_and_plot(spark, tmp_path):
    _posts(spark).write.parquet(str(tmp_path / "posts"))
    argv = [
        "--posts",
        str(tmp_path / "posts"),
        "--sample-sizes",
        "4",
        "--out",
        str(tmp_path / "out"),
    ]
    assert benchmark.main([*argv, "--master", "local[2]"]) == 0
    with (tmp_path / "out" / "runs.csv").open() as handle:
        assert [r["engine"] for r in csv.DictReader(handle)] == ["sparse_blocks", "spark_join"]
    assert (tmp_path / "out" / "runtime.png").stat().st_size > 0
