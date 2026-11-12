"""Ranking metrics, as pure Python functions (no Spark), for the duplicate-detection experiments.

A *ranked* list holds the candidate ids of one question, best first. The *relevant* set holds
the ids that are labelled as its original(s). Precision is a lower bound: some true duplicates
were never labelled, so a candidate that is not in ``relevant`` may still be a real duplicate.
"""

import math
from collections.abc import Callable, Collection, Sequence


def _top(ranked: Sequence, k: int) -> Sequence:
    if k < 1:
        raise ValueError("k must be at least 1")
    return ranked[:k]


def rank_of(ranked: Sequence, item) -> int | None:
    """Return the 1-based rank of ``item`` in ``ranked``, or ``None`` if it is not there."""
    try:
        return list(ranked).index(item) + 1
    except ValueError:
        return None


def recall_at_k(ranked: Sequence, relevant: Collection, k: int) -> float:
    """Share of the relevant ids that appear among the first ``k`` ranked ids."""
    if not relevant:
        return 0.0
    hits = sum(1 for item in _top(ranked, k) if item in relevant)
    return hits / len(relevant)


def precision_at_k(ranked: Sequence, relevant: Collection, k: int) -> float:
    """Share of the first ``k`` ranked ids that are relevant (a lower bound, see the module doc)."""
    return sum(1 for item in _top(ranked, k) if item in relevant) / k


def average_precision(ranked: Sequence, relevant: Collection) -> float:
    """Average of the precision at each rank where a relevant id appears (0 if none is relevant)."""
    if not relevant:
        return 0.0
    hits = 0
    total = 0.0
    for position, item in enumerate(ranked, start=1):
        if item in relevant:
            hits += 1
            total += hits / position
    return total / len(relevant)


def ndcg_at_k(ranked: Sequence, relevant: Collection, k: int) -> float:
    """Normalised discounted cumulative gain with binary relevance, over the first ``k`` ids."""
    if not relevant:
        return 0.0
    dcg = sum(
        1.0 / math.log2(position + 1)
        for position, item in enumerate(_top(ranked, k), start=1)
        if item in relevant
    )
    ideal = sum(1.0 / math.log2(position + 1) for position in range(1, min(len(relevant), k) + 1))
    return dcg / ideal


def reduction_ratio(n_questions: int, n_candidates: int) -> float:
    """Share of all question pairs not considered: 1 - candidates / (n(n-1)/2)."""
    if n_questions < 2:
        return 0.0
    return 1.0 - n_candidates / (n_questions * (n_questions - 1) / 2)


# Every metric that is reported per question, by the column name used in results/metrics.csv.
# A new metric is added here without touching the evaluation code (open/closed).
METRICS: dict[str, Callable[[Sequence, Collection], float]] = {
    "recall@1": lambda ranked, relevant: recall_at_k(ranked, relevant, 1),
    "recall@5": lambda ranked, relevant: recall_at_k(ranked, relevant, 5),
    "recall@10": lambda ranked, relevant: recall_at_k(ranked, relevant, 10),
    "precision@10": lambda ranked, relevant: precision_at_k(ranked, relevant, 10),
    "map": average_precision,
    "ndcg@10": lambda ranked, relevant: ndcg_at_k(ranked, relevant, 10),
}


def mean_metrics(rows: Collection[tuple[Sequence, Collection]]) -> dict[str, float]:
    """Average every metric in ``METRICS`` over ``(ranked, relevant)`` pairs, one per question."""
    if not rows:
        return {name: 0.0 for name in METRICS}
    return {
        name: sum(metric(ranked, relevant) for ranked, relevant in rows) / len(rows)
        for name, metric in METRICS.items()
    }
