"""Candidate ranking strategies for E4 verification (contract C5).

Implements the Strategy pattern via the ``Ranker`` protocol and ``RANKERS``
registry so downstream evaluation only depends on the C5 output schema:
``(dup_id, cand_id, rank, score, method)``.
"""

from __future__ import annotations

from typing import Protocol

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.verification.rerank import TfidfRanker

C5_COLUMNS = ["dup_id", "cand_id", "rank", "score", "method"]


class Ranker(Protocol):
    """Strategy interface for ranking candidate pairs per duplicate query."""

    name: str

    def rank(
        self,
        candidates: DataFrame,
        shingles: DataFrame | None = None,
        top_k: int = 10,
        min_jaccard: float | None = None,
    ) -> DataFrame:
        """Return C5-compliant ranked candidates."""
        ...


def compute_exact_jaccard(candidates: DataFrame, shingles: DataFrame) -> DataFrame:
    """Attach ``exact_jaccard`` to C4 candidates using C2 shingle sets.

    Args:
        candidates: C4 DataFrame ``(dup_id, cand_id, est_sim)``.
        shingles: C2 DataFrame ``(post_id, shingles, ...)``.

    Returns:
        DataFrame with ``(dup_id, cand_id, est_sim, exact_jaccard)``.
    """
    dup_shingles = shingles.select(
        F.col("post_id").alias("_dup_post_id"),
        F.array_distinct(F.col("shingles")).alias("_dup_shingles"),
    )
    cand_shingles = shingles.select(
        F.col("post_id").alias("_cand_post_id"),
        F.array_distinct(F.col("shingles")).alias("_cand_shingles"),
    )

    joined = candidates.join(
        dup_shingles,
        candidates["dup_id"] == dup_shingles["_dup_post_id"],
        "inner",
    ).join(
        cand_shingles,
        candidates["cand_id"] == cand_shingles["_cand_post_id"],
        "inner",
    )

    inter_size = F.size(F.array_intersect(F.col("_dup_shingles"), F.col("_cand_shingles")))
    union_size = F.size(F.array_union(F.col("_dup_shingles"), F.col("_cand_shingles")))
    exact_col = F.when(
        union_size > 0, inter_size.cast("double") / union_size.cast("double")
    ).otherwise(F.lit(0.0))

    return joined.select(
        candidates["dup_id"],
        candidates["cand_id"],
        candidates["est_sim"].cast("double").alias("est_sim"),
        exact_col.cast("double").alias("exact_jaccard"),
    )


def _finalize_ranking(df: DataFrame, score_col: str, method: str, top_k: int) -> DataFrame:
    """Partition by ``dup_id``, break ties by ``cand_id`` ascending, and take ``top_k``."""
    if top_k < 1:
        raise ValueError(f"top_k must be >= 1, got {top_k}")
    order = Window.partitionBy("dup_id").orderBy(F.col(score_col).desc(), F.col("cand_id").asc())
    return (
        df.withColumn("rank", F.row_number().over(order).cast("int"))
        .filter(F.col("rank") <= top_k)
        .select(
            F.col("dup_id"),
            F.col("cand_id"),
            F.col("rank"),
            F.col(score_col).cast("double").alias("score"),
            F.lit(method).alias("method"),
        )
    )


class EstimatedRanker:
    """Rank candidates by LSH estimated similarity (``est_sim`` descending)."""

    name: str = "estimated"

    def rank(
        self,
        candidates: DataFrame,
        shingles: DataFrame | None = None,
        top_k: int = 10,
        min_jaccard: float | None = None,
    ) -> DataFrame:
        """Rank C4 candidates by ``est_sim`` descending, ties by ``cand_id`` ascending."""
        scored = candidates
        if min_jaccard is not None:
            if shingles is not None:
                scored = compute_exact_jaccard(candidates, shingles).filter(
                    F.col("exact_jaccard") >= min_jaccard
                )
            else:
                scored = candidates.filter(F.col("est_sim") >= min_jaccard)
        return _finalize_ranking(scored, score_col="est_sim", method=self.name, top_k=top_k)


class ExactJaccardRanker:
    """Re-score candidates with exact Jaccard on C2 shingles and rank descending."""

    name: str = "exact_jaccard"

    def rank(
        self,
        candidates: DataFrame,
        shingles: DataFrame | None = None,
        top_k: int = 10,
        min_jaccard: float | None = None,
    ) -> DataFrame:
        """Compute exact Jaccard from ``shingles`` and rank descending, ties by ``cand_id``."""
        if shingles is None:
            raise ValueError("ExactJaccardRanker requires C2 shingles DataFrame")
        scored = compute_exact_jaccard(candidates, shingles)
        if min_jaccard is not None:
            scored = scored.filter(F.col("exact_jaccard") >= min_jaccard)
        return _finalize_ranking(scored, score_col="exact_jaccard", method=self.name, top_k=top_k)


RANKERS: dict[str, Ranker] = {
    "estimated": EstimatedRanker(),
    "exact_jaccard": ExactJaccardRanker(),
    "tfidf": TfidfRanker(),
}
