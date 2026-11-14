"""Unit tests for E7 stretch bucket splitting (salting) policy (#41)."""

from __future__ import annotations

from itertools import combinations

from src.similarity.lsh import LSHConfig, find_candidates, resolve_bucket_policy
from src.verification.salt_split import salt_split


class TestSaltSplit:
    """Acceptance and integration tests for ``salt_split``."""

    def test_salt_split_k2_on_bucket_of_5_yields_all_10_pairs_and_splits_partitions(
        self, spark
    ) -> None:
        rows = [(0, "big_bucket", post_id, 100 + post_id) for post_id in range(1, 6)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])

        split_df = salt_split(k=2)(df)
        assert split_df.columns == df.columns

        by_subbucket: dict[str, set[int]] = {}
        for row in split_df.collect():
            by_subbucket.setdefault(row["bucket_key"], set()).add(row["id"])

        # No single sub-bucket / partition holds all 5 items.
        assert len(by_subbucket) > 1
        assert all(len(members) < 5 for members in by_subbucket.values())

        # Every pair among the 5 ids appears in at least one sub-bucket.
        emitted_pairs: set[tuple[int, int]] = set()
        for members in by_subbucket.values():
            for u, v in combinations(sorted(members), 2):
                emitted_pairs.add((u, v))
        assert emitted_pairs == set(combinations(range(1, 6), 2))
        assert len(emitted_pairs) == 10

    def test_salt_split_keeps_small_buckets_intact(self, spark) -> None:
        rows = [(0, "pair_bucket", 1, 10), (0, "pair_bucket", 2, 20)]
        df = spark.createDataFrame(rows, ["band", "bucket_key", "id", "created"])

        split_df = salt_split(k=2)(df)
        assert split_df.count() == 2
        assert {r["bucket_key"] for r in split_df.collect()} == {"pair_bucket"}

    def test_salt_split_plugs_into_lsh_resolve_bucket_policy(self, spark) -> None:
        policy = resolve_bucket_policy("src.verification.salt_split:salt_split")
        assert callable(policy)

        sig = [1, 2, 3, 4]
        sigs = spark.createDataFrame(
            [(post_id, 100 + post_id, sig) for post_id in range(1, 6)],
            ["post_id", "created", "signature"],
        )
        candidates = find_candidates(
            sigs,
            config=LSHConfig(b=1, r=4, n=4),
            bucket_policy=policy,
        )
        pairs = {(row["dup_id"], row["cand_id"]) for row in candidates.collect()}
        assert len(pairs) == 10
