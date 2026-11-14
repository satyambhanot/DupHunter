"""Tests for E6 tag-blocking strategies, C9 contract, recall_cost, and LSH integration."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.blocking.blocking import (
    STRATEGIES,
    FrequentTagset,
    assign_groups,
    compare_blocking,
    compute_recall_cost,
    load_frequent_tagsets,
)
from src.similarity.lsh import LSHConfig, find_candidates


class TestTagBlocking:
    """Unit and Spark integration tests for tag-based blocking."""

    def test_son_prefers_largest_tagset_then_lowest_support_else_other(self, spark) -> None:
        tagsets = [
            FrequentTagset(frozenset({"python"}), "python", size=1, support=0.50),
            FrequentTagset(frozenset({"pandas"}), "pandas", size=1, support=0.10),
            FrequentTagset(frozenset({"python", "pandas"}), "python pandas", size=2, support=0.08),
            FrequentTagset(frozenset({"python", "django"}), "python django", size=2, support=0.15),
        ]
        posts = spark.createDataFrame(
            [
                (1, ["python", "pandas", "django"]),  # matches size-2 sets; lowest support wins
                (2, ["python", "numpy"]),  # matches python (size 1)
                (3, ["python", "pandas"]),  # matches python pandas (size 2)
                (4, ["rust"]),  # no match -> other
                (5, []),  # empty -> other
            ],
            "post_id long, tags array<string>",
        )

        rows = {
            r["post_id"]: r["group"]
            for r in assign_groups(posts, mode="son", tagsets=tagsets).collect()
        }
        assert rows == {
            1: "python pandas",
            2: "python",
            3: "python pandas",
            4: "other",
            5: "other",
        }

    def test_single_tag_and_none_modes_and_strategy_registry(self, spark) -> None:
        assert set(STRATEGIES) == {"son", "single_tag", "none"}
        posts = spark.createDataFrame(
            [(1, ["python", "pandas"]), (2, [])],
            "post_id long, tags array<string>",
        )

        single = {
            r["post_id"]: r["group"] for r in assign_groups(posts, mode="single_tag").collect()
        }
        assert single == {1: "python", 2: "other"}

        none_groups = {
            r["post_id"]: r["group"] for r in assign_groups(posts, mode="none").collect()
        }
        assert none_groups == {1: "all", 2: "all"}

        with pytest.raises(ValueError, match="Unknown blocking mode"):
            assign_groups(posts, mode="invalid")

    def test_c9_groups_accepted_by_lsh_find_candidates(self, spark) -> None:
        tagsets = [
            FrequentTagset(frozenset({"python"}), "python", size=1, support=0.4),
            FrequentTagset(frozenset({"pandas"}), "pandas", size=1, support=0.2),
        ]
        posts = spark.createDataFrame(
            [(1, ["python"]), (2, ["python"]), (3, ["pandas"])],
            "post_id long, tags array<string>",
        )
        groups = assign_groups(posts, mode="son", tagsets=tagsets)
        assert groups.columns == ["post_id", "group"]

        sig = [1] * 8
        sigs = spark.createDataFrame(
            [(1, sig), (2, sig), (3, sig)],
            "post_id long, signature array<int>",
        )
        # Without groups, (3, 1) and (3, 2) would also pair; with groups only (2, 1) pairs.
        cands = find_candidates(sigs, config=LSHConfig(b=4, r=2, n=8), groups=groups).collect()
        assert [(r["dup_id"], r["cand_id"]) for r in cands] == [(2, 1)]

    def test_compute_recall_cost_and_compare_blocking(self, spark) -> None:
        tagsets = [
            FrequentTagset(frozenset({"python"}), "python", size=1, support=0.4),
            FrequentTagset(frozenset({"pandas"}), "pandas", size=1, support=0.2),
        ]
        posts = spark.createDataFrame(
            [(1, ["python"]), (2, ["python"]), (3, ["pandas"])],
            "post_id long, tags array<string>",
        )
        groups = assign_groups(posts, mode="son", tagsets=tagsets)
        answer_key = spark.createDataFrame(
            [(2, 1), (3, 1)],
            "dup_id long, original_id long",
        )
        assert compute_recall_cost(groups, answer_key) == pytest.approx(0.5)

        empty_key = spark.createDataFrame([], "dup_id long, original_id long")
        assert compute_recall_cost(groups, empty_key) == 0.0

        sig = [1] * 8
        sigs = spark.createDataFrame(
            [(1, sig), (2, sig), (3, sig)],
            "post_id long, signature array<int>",
        )
        rows = compare_blocking(
            sigs,
            posts,
            answer_key,
            tagsets,
            lsh_config=LSHConfig(b=4, r=2, n=8),
        )
        assert [r["mode"] for r in rows] == ["none", "single_tag", "son"]
        by_mode = {r["mode"]: r for r in rows}
        assert by_mode["none"]["recall_cost"] == 0.0
        assert by_mode["son"]["recall_cost"] == 0.5
        assert by_mode["none"]["n_candidates"] == 3
        assert by_mode["son"]["n_candidates"] == 1

    def test_load_frequent_tagsets_from_repo_csv(self) -> None:
        csv_path = (
            Path(__file__).resolve().parents[2] / "results" / "e6_blocking" / "frequent_tagsets.csv"
        )
        tagsets = load_frequent_tagsets(csv_path)
        assert tagsets
        assert tagsets[0].size >= tagsets[-1].size
