"""Unit tests for LSH banding and candidate generation (Issue #9)."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.similarity.lsh import (
    LSHConfig,
    estimate_similarity,
    find_candidates,
    lsh_threshold,
    parse_args,
    pass_through_policy,
    s_curve,
    size_limit,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-lsh")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Mathematical Formulas & Config Validation
# ---------------------------------------------------------------------------


def test_s_curve_formula():
    """Verify S-curve formula matches Issue #13: s_curve(0.5, 32, 4) ≈ 0.8732."""
    assert s_curve(0.5, 32, 4) == pytest.approx(0.8732, abs=1e-3)
    assert s_curve(0.0, 32, 4) == 0.0
    assert s_curve(1.0, 32, 4) == 1.0


def test_lsh_threshold():
    """Verify LSH threshold formula (1/b)^(1/r)."""
    t = lsh_threshold(16, 8)
    expected = (1.0 / 16.0) ** (1.0 / 8.0)
    assert t == pytest.approx(expected)


def test_config_validation():
    """Verify b * r != n raises ValueError and config bounds."""
    # Valid config
    cfg = LSHConfig(b=2, r=2, n=4)
    assert cfg.total_hashes == 4

    # b * r != n raises ValueError
    with pytest.raises(ValueError, match=r"b \* r \(4\) != n \(5\)"):
        LSHConfig(b=2, r=2, n=5)

    with pytest.raises(ValueError, match="b must be at least 1"):
        LSHConfig(b=0, r=4)

    with pytest.raises(ValueError, match="r must be at least 1"):
        LSHConfig(b=4, r=0)

    with pytest.raises(ValueError, match="bands_per_pass must be at least 1"):
        LSHConfig(b=4, r=2, bands_per_pass=0)


def test_estimate_similarity():
    """Test pure helper for signature overlap."""
    assert estimate_similarity([1, 2, 3, 4], [1, 2, 9, 9]) == 0.5
    assert estimate_similarity([1, 2], [1, 2]) == 1.0
    assert estimate_similarity([1, 2], [3, 4]) == 0.0
    assert estimate_similarity([], []) == 0.0


# ---------------------------------------------------------------------------
# Acceptance Criterion 1: Core Example
# ---------------------------------------------------------------------------


def test_acceptance_criterion_1_core_example(spark: SparkSession):
    """b=2, r=2: id1 [1,2,3,4], id2 [1,2,9,9], id3 [7,7,7,7] gives exactly

    one pair (newer id2, older id1) with est_sim 0.5.
    """
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
            (3, [7, 7, 7, 7]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config, bucket_policy=pass_through_policy)
    rows = candidates.collect()

    assert len(rows) == 1
    pair = rows[0]
    # newer is id2 (post_id=2), older is id1 (cand_id=1)
    assert pair.dup_id == 2
    assert pair.cand_id == 1
    assert pair.est_sim == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Acceptance Criterion 2: Multi-band deduplication, no self-pairs, earlier questions
# ---------------------------------------------------------------------------


def test_pair_meeting_in_several_bands_appears_once(spark: SparkSession):
    """A pair meeting in several bands appears once."""
    # id1 and id2 match on both band 0 [1, 2] and band 1 [3, 4]
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 3, 4]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config)
    rows = candidates.collect()

    assert len(rows) == 1
    assert rows[0].dup_id == 2
    assert rows[0].cand_id == 1
    assert rows[0].est_sim == pytest.approx(1.0)


def test_no_self_pairs(spark: SparkSession):
    """A question is never paired with itself."""
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config)
    assert candidates.count() == 0


def test_only_earlier_questions_are_candidates_with_timestamp(spark: SparkSession):
    """When created timestamp exists, older question is always cand_id."""
    df = spark.createDataFrame(
        [
            (100, 1000, [1, 2, 3, 4]),  # post_id 100 created at time 1000 (newer)
            (200, 500, [1, 2, 3, 4]),  # post_id 200 created at time 500 (older)
        ],
        ["post_id", "created", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config)
    rows = candidates.collect()

    assert len(rows) == 1
    # post_id 100 is newer (created=1000), cand_id 200 is older (created=500)
    assert rows[0].dup_id == 100
    assert rows[0].cand_id == 200


# ---------------------------------------------------------------------------
# Acceptance Criterion 3: Bucket Policy & Groups
# ---------------------------------------------------------------------------


def test_policy_dropping_everything_yields_no_candidates(spark: SparkSession):
    """A policy that drops everything yields no candidates."""
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
        ],
        ["post_id", "signature"],
    )

    def drop_all(bucket_df):
        return bucket_df.filter(F.lit(False))

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config, bucket_policy=drop_all)
    assert candidates.count() == 0


def test_size_limit_policy(spark: SparkSession):
    """size_limit(3) drops a bucket of 5 and keeps a bucket of 3;

    output columns equal input columns.
    """
    mock_df = spark.createDataFrame(
        [
            # Bucket A (5 items)
            (0, "bucket_A", 1, 1, [1, 2]),
            (0, "bucket_A", 2, 2, [1, 2]),
            (0, "bucket_A", 3, 3, [1, 2]),
            (0, "bucket_A", 4, 4, [1, 2]),
            (0, "bucket_A", 5, 5, [1, 2]),
            # Bucket B (3 items)
            (0, "bucket_B", 6, 6, [3, 4]),
            (0, "bucket_B", 7, 7, [3, 4]),
            (0, "bucket_B", 8, 8, [3, 4]),
        ],
        ["band", "bucket_key", "id", "created", "signature"],
    )

    policy = size_limit(3)
    filtered = policy(mock_df)

    assert filtered.columns == mock_df.columns
    # Bucket A (5 items) dropped, Bucket B (3 items) kept
    assert filtered.count() == 3
    remaining_keys = [r.bucket_key for r in filtered.select("bucket_key").distinct().collect()]
    assert remaining_keys == ["bucket_B"]


def test_groups_different_groups_never_pair(spark: SparkSession):
    """Items in different groups never pair."""
    # id1 and id2 have identical signatures on band 0, but different groups
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
        ],
        ["post_id", "signature"],
    )

    groups_df = spark.createDataFrame(
        [
            (1, "python"),
            (2, "javascript"),
        ],
        ["post_id", "group"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config, groups=groups_df)
    assert candidates.count() == 0


def test_groups_same_group_pairs(spark: SparkSession):
    """Items in the same group can pair."""
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
        ],
        ["post_id", "signature"],
    )

    groups_df = spark.createDataFrame(
        [
            (1, "python"),
            (2, "python"),
        ],
        ["post_id", "group"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config, groups=groups_df)
    assert candidates.count() == 1


# ---------------------------------------------------------------------------
# Acceptance Criterion 4: Multi-pass equivalence, b*r != n, Contract C4
# ---------------------------------------------------------------------------


def test_bands_per_pass_identical_output(spark: SparkSession):
    """Output is identical for --bands-per-pass 1 and 2."""
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
            (3, [5, 6, 3, 4]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    res_pass1 = (
        find_candidates(df, config=config, bands_per_pass=1).orderBy("dup_id", "cand_id").collect()
    )
    res_pass2 = (
        find_candidates(df, config=config, bands_per_pass=2).orderBy("dup_id", "cand_id").collect()
    )

    assert len(res_pass1) == len(res_pass2)
    assert [(r.dup_id, r.cand_id, r.est_sim) for r in res_pass1] == [
        (r.dup_id, r.cand_id, r.est_sim) for r in res_pass2
    ]


def test_signature_length_mismatch_raises_value_error(spark: SparkSession):
    """b*r != n raises ValueError."""
    # Signature length 5 != b * r (4)
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4, 5]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    with pytest.raises(Exception, match=r"Signature length 5 != expected b \* r \(4\)"):
        find_candidates(df, config=config).collect()


def test_contract_c4_schema(spark: SparkSession):
    """Output matches contract C4 columns: dup_id, cand_id, est_sim."""
    df = spark.createDataFrame(
        [
            (1, [1, 2, 3, 4]),
            (2, [1, 2, 9, 9]),
        ],
        ["post_id", "signature"],
    )

    config = LSHConfig(b=2, r=2)
    candidates = find_candidates(df, config=config)
    assert candidates.columns == ["dup_id", "cand_id", "est_sim"]


def test_cli_help():
    """Verify parse_args supports all required CLI flags."""
    args = parse_args(
        [
            "--input",
            "in.parquet",
            "--output",
            "out.parquet",
            "-b",
            "32",
            "-r",
            "4",
            "--bands-per-pass",
            "2",
            "--bucket-policy",
            "size_limit",
            "--bucket-limit",
            "500",
            "--groups",
            "groups.parquet",
        ]
    )
    assert args.input == "in.parquet"
    assert args.output == "out.parquet"
    assert args.b == 32
    assert args.r == 4
    assert args.bands_per_pass == 2
    assert args.bucket_policy == "size_limit"
    assert args.bucket_limit == 500
    assert args.groups == "groups.parquet"


def test_resolve_bucket_policy_names_and_module_attr():
    from src.similarity.lsh import pass_through_policy, resolve_bucket_policy

    assert resolve_bucket_policy("none") is pass_through_policy
    assert callable(resolve_bucket_policy("size_limit", 5))
    # module:attr loads a policy provided by another workstream
    assert resolve_bucket_policy("src.similarity.lsh:pass_through_policy") is pass_through_policy
    with pytest.raises(ValueError, match="Unknown bucket policy"):
        resolve_bucket_policy("nonsense")
    with pytest.raises(ValueError, match="Cannot load bucket policy"):
        resolve_bucket_policy("src.similarity.lsh:does_not_exist")
    with pytest.raises(ValueError, match="Cannot load bucket policy"):
        resolve_bucket_policy("no_such_module:policy")


def test_candidates_accept_a_module_attr_policy_name(spark):
    df = spark.createDataFrame(
        [(1, [1, 2, 3, 4]), (2, [1, 2, 9, 9])],
        ["post_id", "signature"],
    )
    rows = find_candidates(
        df, config=LSHConfig(b=2, r=2), bucket_policy="src.similarity.lsh:pass_through_policy"
    ).collect()
    assert [(r.dup_id, r.cand_id) for r in rows] == [(2, 1)]
