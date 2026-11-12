"""Tests for the pure ranking metrics (issue #12)."""

import math

import pytest

from src.evaluation import metrics

RANKED = ["x", "o1", "y"]
RELEVANT = {"o1"}


def test_recall_at_k():
    assert metrics.recall_at_k(RANKED, RELEVANT, 1) == 0.0
    assert metrics.recall_at_k(RANKED, RELEVANT, 5) == 1.0


def test_precision_at_k_is_hits_over_k():
    assert metrics.precision_at_k(RANKED, RELEVANT, 3) == pytest.approx(1 / 3)
    assert metrics.precision_at_k(RANKED, RELEVANT, 10) == pytest.approx(1 / 10)


def test_average_precision_with_one_relevant_item_is_one_over_rank():
    assert metrics.average_precision(RANKED, RELEVANT) == pytest.approx(0.5)


def test_average_precision_with_two_relevant_items():
    # relevant at ranks 1 and 3: (1/1 + 2/3) / 2
    assert metrics.average_precision(["a", "x", "b"], {"a", "b"}) == pytest.approx((1 + 2 / 3) / 2)


def test_ndcg_at_k():
    assert metrics.ndcg_at_k(RANKED, RELEVANT, 10) == pytest.approx(1 / math.log2(3))
    assert metrics.ndcg_at_k(["o1", "x"], RELEVANT, 10) == pytest.approx(1.0)


def test_ndcg_respects_k():
    assert metrics.ndcg_at_k(RANKED, RELEVANT, 1) == 0.0


def test_reduction_ratio():
    assert metrics.reduction_ratio(1000, 4995) == pytest.approx(0.99)
    assert metrics.reduction_ratio(1, 0) == 0.0


def test_empty_candidates_and_empty_relevant_give_zero():
    assert metrics.mean_metrics([([], {"o1"})]) == {name: 0.0 for name in metrics.METRICS}
    assert metrics.recall_at_k(RANKED, set(), 5) == 0.0
    assert metrics.average_precision(RANKED, set()) == 0.0
    assert metrics.ndcg_at_k(RANKED, set(), 5) == 0.0


def test_rank_of():
    assert metrics.rank_of(RANKED, "o1") == 2
    assert metrics.rank_of(RANKED, "zzz") is None


def test_k_must_be_positive():
    with pytest.raises(ValueError):
        metrics.recall_at_k(RANKED, RELEVANT, 0)


def test_mean_metrics_averages_over_questions():
    result = metrics.mean_metrics([(["o1"], {"o1"}), (["x"], {"o2"})])
    assert result["recall@1"] == 0.5
    assert result["map"] == 0.5


def test_registry_has_the_reported_columns():
    assert list(metrics.METRICS) == [
        "recall@1", "recall@5", "recall@10", "precision@10", "map", "ndcg@10",
    ]  # fmt: skip
