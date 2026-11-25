"""Tests for the shared-sample E5 comparison (#10, #11)."""

from __future__ import annotations

import csv
from datetime import datetime

import pytest

from src.baselines import compare


def _created(day):
    return datetime(2024, 1, day)


def _data(spark):
    titles = {
        1: "how to sort a python list",
        2: "how to sort a python list",  # duplicate of 1, identical title
        3: "django database migration fails",
        4: "django database migration error",  # duplicate of 3, J = 3/5
        5: "windows blue screen on boot",
        6: "ubuntu wifi driver missing",
        7: "excel formula returns zero",
    }
    posts = spark.createDataFrame(
        [(i, _created(i), t, "body") for i, t in titles.items()],
        ["post_id", "created", "title", "body"],
    )
    key = spark.createDataFrame(
        [(2, 1, _created(2)), (4, 3, _created(4)), (900, 800, _created(5))],
        ["dup_id", "original_id", "created"],
    )
    return posts, key


def test_sample_is_deterministic_and_holds_whole_pairs(spark):
    posts, key = _data(spark)
    first = {r.post_id for r in compare.sample_ids(posts, key, 1, 2, seed=1).collect()}
    again = compare.sample_ids(posts.repartition(3), key, 1, 2, seed=1)
    assert {r.post_id for r in again.collect()} == first
    assert len(first) == 4
    assert {1, 2} <= first or {3, 4} <= first  # one whole labelled pair
    everything = compare.sample_ids(posts, key, 10, 10, seed=1)
    assert sorted(r.post_id for r in everything.collect()) == [1, 2, 3, 4, 5, 6, 7]


def test_every_method_is_scored_on_the_same_sample(spark, tmp_path):
    posts, key = _data(spark)
    posts.write.parquet(str(tmp_path / "posts"))
    key.write.parquet(str(tmp_path / "key"))
    argv = ["--posts", str(tmp_path / "posts"), "--answer-key", str(tmp_path / "key")]
    argv += ["--out", str(tmp_path / "out"), "--pairs", "10", "--extra", "10"]
    assert compare.main([*argv, "--master", "local[2]"]) == 0

    with (tmp_path / "out" / "shared_sample.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert [r["method"] for r in rows] == ["exact"] + ["our_lsh"] * 3 + ["spark_minhash"] * 3
    exact = rows[0]
    assert float(exact["recall@10"]) == 1.0  # both labelled pairs share words
    assert float(exact["labelled_pairs_found"]) == 1.0
    assert float(exact["exact_pairs_found_J>=0.6"]) == 1.0
    for row in rows:
        assert 0.0 <= float(row["exact_pairs_found_J>=0.2"]) <= 1.0
    ids = (tmp_path / "out" / "sample_ids.csv").read_text().split()
    assert ids == ["post_id", "1", "2", "3", "4", "5", "6", "7"]


def test_levels_below_the_exact_threshold_are_rejected(spark):
    shingles = spark.createDataFrame([(1, [1], _created(1))], ["post_id", "shingles", "created"])
    key = spark.createDataFrame([(1, 1)], ["dup_id", "original_id"])
    with pytest.raises(ValueError, match="exact threshold"):
        compare.compare_methods(shingles, key, exact_threshold=0.5, levels=(0.2,))
