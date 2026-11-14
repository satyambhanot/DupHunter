"""Tests for E4 TF-IDF cosine re-ranking (#40)."""

from __future__ import annotations

import logging

import pytest

from src.verification.rankers import C5_COLUMNS, RANKERS
from src.verification.rerank import (
    TfidfRanker,
    _tfidf_unit_vector,
    compare_with_tfidf,
    parse_args,
    tfidf_cosine,
)
from src.verification.verify import parse_args as verify_parse_args
from src.verification.verify import verify_candidates


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

    def test_compare_with_tfidf_returns_methods(self, spark) -> None:
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

        answer_key = spark.createDataFrame(
            [(1, 101, "2020-01-01")], "dup_id long, original_id long, created string"
        )
        rows = compare_with_tfidf(candidates, shingles, posts, answer_key=answer_key, n_questions=3)
        assert [r["method"] for r in rows] == ["estimated", "exact_jaccard", "tfidf_title", "tfidf"]
        by_method = {r["method"]: r for r in rows}
        assert by_method["estimated"]["recall@1"] == 0.0
        assert by_method["exact_jaccard"]["recall@1"] == 1.0
        assert by_method["tfidf_title"]["recall@1"] == 1.0
        assert by_method["tfidf"]["recall@1"] == 1.0
        assert by_method["tfidf"]["ndcg@10"] == 1.0

    def test_verify_candidates_tfidf_requires_posts(self, spark) -> None:
        candidates = spark.createDataFrame(
            [(1, 101, 0.5)], "dup_id long, cand_id long, est_sim double"
        )
        with pytest.raises(ValueError, match="--posts"):
            verify_candidates(candidates, method="tfidf")

        posts = spark.createDataFrame(
            [(1, "pandas dataframe", ""), (101, "pandas dataframe", "")],
            "post_id long, title string, body string",
        )
        ranked = verify_candidates(candidates, method="tfidf", posts=posts)
        assert ranked.collect()[0]["method"] == "tfidf"

        with pytest.raises(SystemExit):
            verify_parse_args(
                ["--input", "c", "--shingles", "s", "--output", "o", "--method", "tfidf"]
            )

    def test_missing_post_ids_logs_warning(self, spark, caplog) -> None:
        posts = spark.createDataFrame(
            [(1, "pandas dataframe", "")], "post_id long, title string, body string"
        )
        candidates = spark.createDataFrame(
            [(1, 999, 0.8)], "dup_id long, cand_id long, est_sim double"
        )
        with caplog.at_level(logging.WARNING):
            rows = RANKERS["tfidf"].rank(candidates, posts=posts).collect()
        assert rows[0]["score"] == 0.0
        assert "missing from posts text" in caplog.text

    def test_missing_posts_raises(self, spark) -> None:
        candidates = spark.createDataFrame(
            [(1, 2, 0.5)], "dup_id long, cand_id long, est_sim double"
        )
        with pytest.raises(ValueError, match="posts text"):
            RANKERS["tfidf"].rank(candidates)

    def test_tfidf_unit_vector_excludes_out_of_vocab_terms(self) -> None:
        idf = {"python": 1.5}
        assert _tfidf_unit_vector(["unique_only"], idf) == {}
        vec_a = _tfidf_unit_vector(["python", "unique_a"], idf)
        vec_b = _tfidf_unit_vector(["python", "unique_b"], idf)
        assert vec_a == {"python": 1.0}
        assert vec_b == {"python": 1.0}
        assert tfidf_cosine(vec_a, vec_b) == pytest.approx(1.0)
        assert tfidf_cosine(_tfidf_unit_vector(["unique_only"], idf), vec_a) == 0.0

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
