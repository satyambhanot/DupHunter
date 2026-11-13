"""Tests for E4 TF-IDF cosine re-ranking (#40)."""

from __future__ import annotations

import pytest

from src.verification.rankers import C5_COLUMNS, RANKERS
from src.verification.rerank import TfidfRanker, compare_with_tfidf, parse_args


class TestTfidfRanker:
    """Contract-fixture tests for TfidfRanker and RANKERS['tfidf']."""

    def test_tfidf_registered_in_rankers(self) -> None:
        assert "tfidf" in RANKERS
        assert isinstance(RANKERS["tfidf"], TfidfRanker)

    def test_toy_query_pandas_dataframe_ranks_both_words_first(self, spark) -> None:
        """Acceptance criterion: query 'pandas dataframe' ranks doc with both words first."""
        posts = spark.createDataFrame(
            [
                (1, "pandas dataframe", ""),
                (101, "pandas dataframe merge", "join two tables"),
                (102, "pandas series indexing", "loc iloc guide"),
                (103, "spark dataframe select", "pyspark sql"),
            ],
            "post_id long, title string, body string",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.4), (1, 102, 0.9), (1, 103, 0.8)],
            "dup_id long, cand_id long, est_sim double",
        )

        ranked = RANKERS["tfidf"].rank(candidates, posts=posts, top_k=10)
        assert ranked.columns == C5_COLUMNS
        dtypes = dict(ranked.dtypes)
        assert dtypes["rank"] == "int"
        assert dtypes["score"] == "double"
        assert dtypes["method"] == "string"

        rows = ranked.orderBy("rank").collect()
        assert rows[0]["cand_id"] == 101
        assert rows[0]["method"] == "tfidf"
        assert rows[0]["score"] > rows[1]["score"]

    def test_candidate_pool_limit_and_exact_jaccard_tiebreak(self, spark) -> None:
        """Restrict to candidate_pool before TF-IDF and break ties by exact Jaccard."""
        posts = spark.createDataFrame(
            [
                (1, "python list sort", ""),
                (101, "python list sort", ""),
                (102, "python list sort", ""),
                (103, "python list sort", ""),
            ],
            "post_id long, title string, body string",
        )
        shingles = spark.createDataFrame(
            [
                (1, [1, 2, 3, 4]),
                (101, [1]),
                (102, [1, 2, 3]),
                (103, [1, 2, 3, 4]),
            ],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.9), (1, 102, 0.7), (1, 103, 0.5)],
            "dup_id long, cand_id long, est_sim double",
        )

        reranker = TfidfRanker(posts=posts, candidate_pool=2)
        rows = reranker.rank(candidates, shingles=shingles, top_k=10).orderBy("rank").collect()
        # Pool=2 by exact Jaccard keeps 103 (1.0) and 102 (0.75); TF-IDF ties -> exact Jaccard wins
        assert [r["cand_id"] for r in rows] == [103, 102]

    def test_compare_with_tfidf_returns_three_methods(self, spark) -> None:
        posts = spark.createDataFrame(
            [
                (1, "pandas dataframe", ""),
                (101, "pandas dataframe", ""),
                (102, "pandas only", ""),
            ],
            "post_id long, title string, body string",
        )
        shingles = spark.createDataFrame(
            [(1, [1, 2]), (101, [1, 2]), (102, [1])],
            "post_id long, shingles array<int>",
        )
        candidates = spark.createDataFrame(
            [(1, 101, 0.5), (1, 102, 0.9)],
            "dup_id long, cand_id long, est_sim double",
        )

        rows = compare_with_tfidf(candidates, shingles, posts)
        assert [r["method"] for r in rows] == ["estimated", "exact_jaccard", "tfidf"]

    def test_missing_posts_raises(self, spark) -> None:
        candidates = spark.createDataFrame(
            [(1, 2, 0.5)], "dup_id long, cand_id long, est_sim double"
        )
        with pytest.raises(ValueError, match="posts text"):
            RANKERS["tfidf"].rank(candidates)

    def test_parse_args(self) -> None:
        args = parse_args(
            [
                "--input",
                "cand.parquet",
                "--shingles",
                "sh.parquet",
                "--posts",
                "posts.parquet",
                "--output",
                "out.csv",
            ]
        )
        assert args.candidate_pool == 50
        assert args.top_k == 10
