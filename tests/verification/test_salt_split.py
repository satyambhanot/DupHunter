"""Unit tests for E7 stretch bucket splitting (salting) policy (#41)."""

from __future__ import annotations

from itertools import combinations

import pytest

from src.similarity.lsh import LSHConfig, find_candidates, resolve_bucket_policy
from src.verification.salt_split import make_salt_split, salt_split


def _sub_buckets(df) -> dict[str, set[int]]:
    by_subbucket: dict[str, set[int]] = {}
    for row in df.collect():
        by_subbucket.setdefault(row["bucket_key"], set()).add(row["id"])
    return by_subbucket


def _emitted_pairs(by_subbucket: dict[str, set[int]]) -> set[tuple[int, int]]:
    return {pair for members in by_subbucket.values() for pair in combinations(sorted(members), 2)}


class TestSaltSplit:
    """Acceptance and integration tests for the salting policy."""

    def test_bucket_of_5_keeps_all_10_pairs(self, spark) -> None:
        rows = [(0, "big_bucket", post_id, 100 + post_id) for post_id in range(1, 6)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])

        split_df = make_salt_split(chunks=3, min_bucket_size=2)(df)
        assert split_df.columns == df.columns

        # Chunks come from a hash, so balance is only statistical (see the larger bucket below);
        # what must hold for any bucket is that no pair is lost.
        by_subbucket = _sub_buckets(split_df)
        assert len(by_subbucket) > 1
        assert _emitted_pairs(by_subbucket) == set(combinations(range(1, 6), 2))

    @pytest.mark.parametrize("chunks", [3, 4, 5])
    def test_larger_bucket_keeps_every_pair_and_shrinks_the_largest_sub_bucket(
        self, spark, chunks: int
    ) -> None:
        n = 60
        rows = [(0, "big_bucket", post_id, 100 + post_id) for post_id in range(1, n + 1)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])

        by_subbucket = _sub_buckets(make_salt_split(chunks=chunks, min_bucket_size=10)(df))

        assert len(by_subbucket) == chunks * (chunks - 1) // 2
        assert _emitted_pairs(by_subbucket) == set(combinations(range(1, n + 1), 2))
        # about 2 / chunks of the members per sub-bucket; a hash is not exactly even
        assert max(len(members) for members in by_subbucket.values()) < 1.5 * (2 * n / chunks)

    def test_split_is_deterministic(self, spark) -> None:
        rows = [(0, "big_bucket", post_id, 100 + post_id) for post_id in range(1, 40)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])
        policy = make_salt_split(chunks=3, min_bucket_size=5)
        assert _sub_buckets(policy(df)) == _sub_buckets(policy(df))

    def test_small_buckets_stay_intact_and_big_ones_split_in_the_same_frame(self, spark) -> None:
        rows = [(0, "pair_bucket", 1, 10), (0, "pair_bucket", 2, 20)]
        rows += [(1, "big_bucket", post_id, 100 + post_id) for post_id in range(10, 16)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])

        by_subbucket = _sub_buckets(make_salt_split(chunks=3, min_bucket_size=2)(df))
        assert by_subbucket["pair_bucket"] == {1, 2}
        assert all(
            key.startswith("big_bucket:salt:") for key in by_subbucket if key != "pair_bucket"
        )

    def test_default_threshold_leaves_a_bucket_of_5_alone(self, spark) -> None:
        rows = [(0, "bucket", post_id, 100 + post_id) for post_id in range(1, 6)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])
        assert _sub_buckets(salt_split(df)) == {"bucket": {1, 2, 3, 4, 5}}

    def test_empty_frame_and_bad_settings(self, spark) -> None:
        empty = spark.createDataFrame(
            [], "band long, bucket_key string, post_id long, created long"
        )
        assert make_salt_split(chunks=3, min_bucket_size=2)(empty).count() == 0
        with pytest.raises(ValueError, match="at least 3"):
            make_salt_split(chunks=2)
        with pytest.raises(ValueError, match="at least 1"):
            make_salt_split(min_bucket_size=0)
        no_id = spark.createDataFrame([(0, "b", 1)], ["band", "bucket_key", "x"])
        with pytest.raises(ValueError, match="post_id"):
            salt_split(no_id)

    def test_salt_split_plugs_into_lsh_resolve_bucket_policy(self, spark) -> None:
        policy = resolve_bucket_policy("src.verification.salt_split:salt_split")
        assert callable(policy)

        sig = [1, 2, 3, 4]
        sigs = spark.createDataFrame(
            [(post_id, 100 + post_id, sig) for post_id in range(1, 6)],
            ["post_id", "created", "signature"],
        )
        candidates = find_candidates(sigs, config=LSHConfig(b=1, r=4, n=4), bucket_policy=policy)
        pairs = {(row["dup_id"], row["cand_id"]) for row in candidates.collect()}
        assert len(pairs) == 10

    def test_split_in_lsh_finds_the_same_pairs_as_no_policy(self, spark) -> None:
        sig = [1, 2, 3, 4]
        sigs = spark.createDataFrame(
            [(post_id, 100 + post_id, sig) for post_id in range(1, 21)],
            ["post_id", "created", "signature"],
        )
        config = LSHConfig(b=1, r=4, n=4)
        plain = find_candidates(sigs, config=config, bucket_policy="none")
        split = find_candidates(
            sigs, config=config, bucket_policy=make_salt_split(chunks=3, min_bucket_size=5)
        )
        as_set = lambda df: {(r["dup_id"], r["cand_id"]) for r in df.collect()}  # noqa: E731
        assert as_set(split) == as_set(plain)
        assert len(as_set(plain)) == 190
