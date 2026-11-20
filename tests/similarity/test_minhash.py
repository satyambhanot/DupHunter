import numpy as np
import pytest
from pyspark.sql import SparkSession

from src.similarity.minhash import (
    EMPTY_SIGNATURE_VALUE,
    PRIME,
    MinHashConfig,
    build_signatures,
    compute_signature,
)


def test_identical_sets_have_identical_signatures():
    left = compute_signature([1, 5, 9, 20], n=128, seed=7)
    right = compute_signature([20, 9, 5, 1, 5], n=128, seed=7)

    np.testing.assert_array_equal(left, right)


def test_disjoint_sets_have_few_agreeing_positions():
    left = compute_signature(range(50), n=128, seed=11)
    right = compute_signature(range(100, 150), n=128, seed=11)

    assert np.count_nonzero(left == right) <= 3


def test_jaccard_estimate_for_half_overlap():
    left = set(range(100))
    right = set(range(33, 133))
    estimate = np.mean(
        compute_signature(left, n=256, seed=23) == compute_signature(right, n=256, seed=23)
    )

    assert estimate == pytest.approx(0.5, abs=0.1)


def test_values_are_bounded_and_input_hashes_are_masked():
    extreme = np.array(
        [np.iinfo(np.int64).min, np.iinfo(np.int64).max, PRIME - 1],
        dtype=np.int64,
    )
    signature = compute_signature(extreme, n=128, seed=3)

    assert np.all(signature >= 0)
    assert np.all(signature < PRIME)
    np.testing.assert_array_equal(
        signature,
        compute_signature([0, PRIME - 1, PRIME - 1], n=128, seed=3),
    )


def test_signature_length_is_prefix_stable():
    shingles = [3, 8, 13, 21, 34]
    short = compute_signature(shingles, n=64, seed=19)
    long = compute_signature(shingles, n=128, seed=19)

    np.testing.assert_array_equal(short, long[:64])


def test_empty_set_uses_documented_sentinel():
    signature = compute_signature([], n=8)

    np.testing.assert_array_equal(
        signature,
        np.full(8, EMPTY_SIGNATURE_VALUE, dtype=np.int64),
    )


def test_config_and_input_validation():
    with pytest.raises(ValueError, match="at least 1"):
        MinHashConfig(n=0)
    with pytest.raises(ValueError, match="one-dimensional"):
        compute_signature([[1, 2], [3, 4]])
    with pytest.raises(TypeError, match="integers"):
        compute_signature([1.5, 2.5])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-minhash")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_wrapper_returns_c3_rows(spark):
    source = spark.sql(
        """
        SELECT 1 AS post_id, array(1L, 2L, 3L) AS shingles
        UNION ALL
        SELECT 2, cast(array() as array<bigint>)
        """
    )

    rows = build_signatures(source, MinHashConfig(n=8, seed=5)).orderBy("post_id").collect()

    assert source.count() == len(rows) == 2
    assert rows[0].__fields__ == ["post_id", "signature"]
    assert all(len(row.signature) == 8 for row in rows)
    assert all(value == EMPTY_SIGNATURE_VALUE for value in rows[1].signature)


def _write_c2(spark, path) -> None:
    spark.createDataFrame(
        [(1, [5, 9, 11, 40]), (2, [5, 9, 12]), (3, [])], "post_id long, shingles array<long>"
    ).write.parquet(str(path))


def test_command_line_writes_the_same_signatures_as_python(spark, tmp_path):
    from src.similarity.minhash import main

    _write_c2(spark, tmp_path / "c2")
    argv = ["--shingles", str(tmp_path / "c2"), "--out", str(tmp_path / "c3")]
    assert main([*argv, "--n", "16", "--seed", "7"]) == 0
    rows = {r.post_id: r.signature for r in spark.read.parquet(str(tmp_path / "c3")).collect()}
    assert rows[1] == compute_signature([5, 9, 11, 40], 16, 7).tolist()
    assert rows[3] == [2_147_483_647] * 16  # the documented empty-set value
    assert set(spark.read.parquet(str(tmp_path / "c3")).columns) == {"post_id", "signature"}


def test_command_line_adds_created_when_asked(spark, tmp_path):
    from datetime import datetime

    from src.similarity.minhash import main

    _write_c2(spark, tmp_path / "c2")
    spark.createDataFrame(
        [(1, datetime(2020, 1, 1)), (2, datetime(2021, 1, 1))],
        "post_id long, created timestamp",
    ).write.parquet(str(tmp_path / "meta"))
    argv = ["--shingles", str(tmp_path / "c2"), "--out", str(tmp_path / "c3")]
    assert main([*argv, "--created-from", str(tmp_path / "meta")]) == 0
    out = {r.post_id: r.created for r in spark.read.parquet(str(tmp_path / "c3")).collect()}
    assert out[2] == datetime(2021, 1, 1) and out[3] is None  # no metadata gives a null, not a drop
