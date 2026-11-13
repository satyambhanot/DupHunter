"""Tests for E4 exact Jaccard verification and ranking strategies (#29)."""

from __future__ import annotations

import csv

import pytest

from src.verification.rankers import C5_COLUMNS, RANKERS
from src.verification.verify import (
    compare_rankings,
    parse_args,
    verify_candidates,
    write_comparison_csv,
)


class TestRankers:
    """Contract-fixture tests for EstimatedRanker and ExactJaccardRanker."""

    def test_registry_has_required_rankers(self) -> None:
        assert "estimated" in RANKERS
        assert "exact_jaccard" in RANKERS

    def test_c1_vs_c2_ranking_flip(self, spark) -> None:
        """Acceptance criterion: est [c1, c2] flips to exact [c2, c1]."""
        # q=1 has {1..10}; c1=101 shares {1,2} -> exact 2/10 = 0.2;
        # c2=102 shares {1..7} -> exact 7/10 = 0.7.
        shingles = spark.createDataFrame(
            [
                (1, list(range(1, 11))),
                (101, list(range(1, 11))[:2]),
                (102, list(range(1, 11))[:7]),
            ],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.9), (1, 102, 0.6)],
            "dup_id long, cand_id long, est_sim double",
        )

        est_rows = (
            verify_candidates(candidates, shingles, method="estimated").orderBy("rank").collect()
        )
        exact_rows = (
            verify_candidates(candidates, shingles, method="exact_jaccard")
            .orderBy("rank")
            .collect()
        )

        assert [r["cand_id"] for r in est_rows] == [101, 102]
        assert [r["cand_id"] for r in exact_rows] == [102, 101]
        assert exact_rows[0]["score"] == pytest.approx(0.7)
        assert exact_rows[1]["score"] == pytest.approx(0.2)

    def test_top_10_and_tie_break_by_cand_id(self, spark) -> None:
        """Keep at most 10 per question and break score ties by cand_id ascending."""
        base = [1, 2, 3, 4]
        shingle_rows = [(10, base)]
        cand_rows = []
        # 12 candidates all sharing {1, 2} with {1,2,3,4} -> exact 2/4 = 0.5
        for cid in range(212, 200, -1):
            shingle_rows.append((cid, [1, 2]))
            cand_rows.append((10, cid, 0.5))

        shingles = spark.createDataFrame(shingle_rows, "post_id long, shingles array<int>")
        candidates = spark.createDataFrame(cand_rows, "dup_id long, cand_id long, est_sim double")

        ranked = (
            verify_candidates(candidates, shingles, method="exact_jaccard")
            .orderBy("rank")
            .collect()
        )
        assert len(ranked) == 10
        assert [r["cand_id"] for r in ranked] == list(range(201, 211))
        assert [r["rank"] for r in ranked] == list(range(1, 11))

    def test_min_jaccard_filters_weak_candidates(self, spark) -> None:
        """Optional min_jaccard removes candidates below the exact Jaccard threshold."""
        shingles = spark.createDataFrame(
            [(1, [1, 2, 3, 4]), (101, [1]), (102, [1, 2, 3])],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.8), (1, 102, 0.6)],
            "dup_id long, cand_id long, est_sim double",
        )

        filtered = verify_candidates(
            candidates, shingles, method="exact_jaccard", min_jaccard=0.5
        ).collect()
        assert [(r["cand_id"], r["rank"]) for r in filtered] == [(102, 1)]
        assert filtered[0]["score"] == pytest.approx(0.75)

    def test_c5_schema_and_types(self, spark) -> None:
        """Output columns and types match contract C5."""
        shingles = spark.createDataFrame(
            [(1, [1, 2]), (2, [1, 3])],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 2, 0.5)], "dup_id long, cand_id long, est_sim double"
        )

        ranked = verify_candidates(candidates, shingles, method="exact_jaccard")
        assert ranked.columns == C5_COLUMNS
        dtypes = dict(ranked.dtypes)
        assert dtypes["rank"] == "int"
        assert dtypes["score"] == "double"
        assert dtypes["method"] == "string"

    def test_error_on_missing_shingles_or_unknown_method(self, spark) -> None:
        candidates = spark.createDataFrame(
            [(1, 2, 0.5)], "dup_id long, cand_id long, est_sim double"
        )
        with pytest.raises(ValueError, match="shingles"):
            verify_candidates(candidates, shingles=None, method="exact_jaccard")
        with pytest.raises(ValueError, match="Unknown ranking method"):
            verify_candidates(candidates, shingles=None, method="unknown")


class TestVerifyComparisonAndCLI:
    """Tests for ranking comparison output and CLI argument parsing."""

    def test_compare_rankings_and_write_csv(self, spark, tmp_path) -> None:
        shingles = spark.createDataFrame(
            [
                (1, list(range(1, 11))),
                (101, [1, 2]),
                (102, list(range(1, 8))),
            ],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.9), (1, 102, 0.6)],
            "dup_id long, cand_id long, est_sim double",
        )
        answer_key = spark.createDataFrame([(1, 102)], "dup_id long, original_id long")

        _, _, rows = compare_rankings(candidates, shingles, answer_key=answer_key)
        assert len(rows) == 2
        by_method = {r["method"]: r for r in rows}
        assert by_method["estimated"]["top1_agreement_with_exact"] == 0.0
        assert by_method["estimated"]["mean_abs_rank_shift"] == 1.0
        assert by_method["estimated"]["recall_at_1"] == 0.0
        assert by_method["exact_jaccard"]["recall_at_1"] == 1.0

        out_csv = write_comparison_csv(rows, tmp_path / "ranking_comparison.csv")
        assert out_csv.exists()
        with out_csv.open(encoding="utf-8") as f:
            parsed = list(csv.DictReader(f))
        assert [r["method"] for r in parsed] == ["estimated", "exact_jaccard"]

    def test_parse_args_defaults_and_overrides(self) -> None:
        args = parse_args(
            ["--input", "cand.parquet", "--shingles", "sh.parquet", "--output", "out.csv"]
        )
        assert args.method == "both"
        assert args.top_k == 10
        assert args.min_jaccard is None
        assert args.master == "local[*]"

        custom = parse_args(
            [
                "--input",
                "c.parquet",
                "--shingles",
                "s.parquet",
                "--output",
                "o.parquet",
                "--method",
                "exact_jaccard",
                "--top-k",
                "5",
                "--min-jaccard",
                "0.2",
                "--master",
                "local[2]",
            ]
        )
        assert custom.method == "exact_jaccard"
        assert custom.top_k == 5
        assert custom.min_jaccard == 0.2
        assert custom.master == "local[2]"
