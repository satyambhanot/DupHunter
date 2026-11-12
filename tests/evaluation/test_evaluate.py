"""Tests for the Spark evaluation of candidates against the answer key (issue #12)."""

import csv
import math
from datetime import datetime

import pytest

from src.evaluation import evaluate

T = datetime(2016, 6, 1)


def _key(spark, pairs):
    return spark.createDataFrame(
        [(d, o, T) for d, o in pairs], "dup_id long, original_id long, created timestamp"
    )


def _cands(spark, rows):
    return spark.createDataFrame(rows, "dup_id long, cand_id long, est_sim double")


def test_hand_made_example(spark):
    # question 10, original 2, ranked [1, 2, 3] by similarity
    candidates = _cands(spark, [(10, 1, 0.9), (10, 2, 0.8), (10, 3, 0.1)])
    result = evaluate.evaluate(candidates, _key(spark, [(10, 2)]), n_questions=1000)
    assert result["recall@1"] == 0.0 and result["recall@5"] == 1.0
    assert result["map"] == pytest.approx(0.5)
    assert result["ndcg@10"] == pytest.approx(0.6309, abs=1e-4)
    assert result["precision@10"] == pytest.approx(0.1)
    assert result["n_candidates"] == 3 and result["n_queries"] == 1


def test_ties_are_ordered_by_cand_id(spark):
    candidates = _cands(spark, [(10, 7, 0.5), (10, 3, 0.5), (10, 5, 0.5)])
    ranked = evaluate.rank_candidates(candidates).orderBy("rank").collect()
    assert [r.cand_id for r in ranked] == [3, 5, 7]


def test_question_without_candidates_scores_zero(spark):
    candidates = _cands(spark, [(10, 2, 0.9)])
    result = evaluate.evaluate(candidates, _key(spark, [(10, 2), (20, 5)]))
    assert result["n_queries"] == 2
    assert result["recall@1"] == pytest.approx(0.5)  # question 20 has no candidates


def test_only_answer_key_questions_are_evaluated(spark):
    rows = [(q, 1, 0.5) for q in range(100, 1100)]
    candidates = _cands(spark, rows + [(10, 2, 0.9)])
    result = evaluate.evaluate(candidates, _key(spark, [(10, 2)]), n_questions=2000)
    assert result["n_queries"] == 1 and result["recall@1"] == 1.0
    assert result["n_candidates"] == 1001  # every candidate counts for the reduction ratio


def test_reduction_ratio_is_nan_without_the_question_count(spark):
    result = evaluate.evaluate(_cands(spark, [(10, 2, 0.9)]), _key(spark, [(10, 2)]))
    assert math.isnan(result["reduction_ratio"])


def test_c5_input_uses_the_given_rank(spark):
    ranked = spark.createDataFrame(
        [(10, 1, 1, 0.9, "exact"), (10, 2, 2, 0.5, "exact")],
        "dup_id long, cand_id long, rank int, score double, method string",
    )
    result = evaluate.evaluate(ranked, _key(spark, [(10, 2)]))
    assert result["recall@1"] == 0.0 and result["recall@5"] == 1.0


def test_max_rank_ignores_deep_candidates(spark):
    candidates = _cands(spark, [(10, c, 1.0 - c / 100) for c in range(1, 6)])
    result = evaluate.evaluate(candidates, _key(spark, [(10, 5)]), max_rank=3)
    assert result["recall@10"] == 0.0


def test_metrics_csv_row_and_header(tmp_path):
    path = tmp_path / "metrics.csv"
    metrics = {name: 0.5 for name in evaluate.METRICS} | {
        "reduction_ratio": 0.99,
        "n_candidates": 3,
    }
    evaluate.append_metrics(path, "E3", {"b": 32, "r": 4}, metrics)
    evaluate.append_metrics(path, "E3", {"b": 16, "r": 8}, metrics)
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert list(rows[0]) == evaluate.COLUMNS and len(evaluate.COLUMNS) == 10
    assert rows[0]["settings_json"] == '{"b": 32, "r": 4}'


def test_per_pair_table(spark):
    candidates = _cands(spark, [(10, 1, 0.9), (10, 2, 0.8), (20, 5, 0.9)])
    key = _key(spark, [(10, 2), (20, 5), (30, 9)])
    posts = spark.createDataFrame(
        [(10, ["python"], "abcd"), (20, ["pandas"], "ab"), (30, [], "")],
        "post_id long, tags array<string>, body string",
    )
    table = evaluate.per_pair_table(candidates, key, posts).orderBy("dup_id")
    rows = table.collect()
    assert [(r.found, r.rank) for r in rows] == [(True, 2), (True, 1), (False, None)]
    assert rows[0].tags == ["python"] and rows[0].body_len == 4 and rows[0].year == 2016
    assert table.columns == [
        "dup_id", "original_id", "found", "rank", "exact_jaccard", "tags", "body_len", "year",
    ]  # fmt: skip


def test_per_pair_table_takes_exact_jaccard(spark):
    candidates = _cands(spark, [(10, 2, 0.8)])
    exact = spark.createDataFrame([(10, 2, 0.75)], "dup_id long, cand_id long, jaccard double")
    row = evaluate.per_pair_table(candidates, _key(spark, [(10, 2)]), None, exact).collect()[0]
    assert row.exact_jaccard == pytest.approx(0.75) and row.tags is None
