"""TF-IDF cosine re-ranking of top LSH candidates (Experiment E4, #40).

Re-ranks the top-N candidates (default 50) per duplicate query using TF-IDF
cosine similarity computed over the question corpus, with exact Jaccard and
``cand_id`` as tie-breakers, producing contract C5 output:
``(dup_id, cand_id, rank, score, method)``.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import re
from collections import Counter
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def tokenize_text(text: str) -> list[str]:
    """Strip HTML tags and extract lowercase alphanumeric word tokens."""
    cleaned = _HTML_TAG_RE.sub(" ", text or "").lower()
    return _TOKEN_RE.findall(cleaned)


def compute_idf(corpus_tokens: dict[int, list[str]]) -> dict[str, float]:
    """Compute smooth IDF weights ``log((N + 1) / (df + 1)) + 1`` over a corpus."""
    n_docs = len(corpus_tokens)
    if n_docs == 0:
        return {}
    doc_freq: Counter[str] = Counter()
    for tokens in corpus_tokens.values():
        doc_freq.update(set(tokens))
    return {term: math.log((n_docs + 1.0) / (df + 1.0)) + 1.0 for term, df in doc_freq.items()}


def _compute_idf_spark(posts: DataFrame, min_df: int = 1) -> dict[str, float]:
    """Compute smooth IDF weights in Spark without collecting full post texts."""
    n_docs = posts.select("post_id").distinct().count()
    if n_docs == 0:
        return {}

    cols = set(posts.columns)
    if {"title", "body"} <= cols:
        raw_text = F.concat_ws(
            " ", F.coalesce(F.col("title"), F.lit("")), F.coalesce(F.col("body"), F.lit(""))
        )
    elif "text" in cols:
        raw_text = F.coalesce(F.col("text"), F.lit(""))
    elif "title" in cols:
        raw_text = F.coalesce(F.col("title"), F.lit(""))
    else:
        raise ValueError("Posts DataFrame must contain ('title', 'body'), 'text', or 'title'")

    cleaned = F.lower(F.regexp_replace(raw_text, r"<[^>]+>", " "))
    tokens = F.filter(F.split(cleaned, r"[^a-z0-9_]+"), lambda t: t != "")
    distinct_terms = posts.select("post_id", F.explode(F.array_distinct(tokens)).alias("term"))
    df_counts = distinct_terms.groupBy("term").count()
    if min_df > 1:
        df_counts = df_counts.filter(F.col("count") >= min_df)

    return {
        str(row["term"]): math.log((n_docs + 1.0) / (float(row["count"]) + 1.0)) + 1.0
        for row in df_counts.collect()
    }


def _tfidf_unit_vector(tokens: list[str], idf: dict[str, float]) -> dict[str, float]:
    """Build an L2-normalized TF-IDF sparse vector for a token list."""
    if not tokens:
        return {}
    tf = Counter(tokens)
    vec = {term: count * idf.get(term, 1.0) for term, count in tf.items()}
    norm = math.sqrt(sum(w * w for w in vec.values()))
    if norm == 0.0:
        return {}
    return {term: w / norm for term, w in vec.items()}


def tfidf_cosine(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    """Cosine similarity between two L2-normalized sparse TF-IDF vectors."""
    if not vec_a or not vec_b:
        return 0.0
    if len(vec_a) > len(vec_b):
        vec_a, vec_b = vec_b, vec_a
    return float(sum(w * vec_b[t] for t, w in vec_a.items() if t in vec_b))


def _extract_corpus_tokens(posts: DataFrame) -> dict[int, list[str]]:
    """Collect ``post_id -> word tokens`` for the filtered posts DataFrame."""
    cols = set(posts.columns)
    if "title" in cols and "body" in cols:
        rows = posts.select("post_id", "title", "body").collect()
        return {
            int(r["post_id"]): tokenize_text(f"{r['title'] or ''} {r['body'] or ''}") for r in rows
        }
    if "text" in cols:
        rows = posts.select("post_id", "text").collect()
        return {int(r["post_id"]): tokenize_text(r["text"] or "") for r in rows}
    if "title" in cols:
        rows = posts.select("post_id", "title").collect()
        return {int(r["post_id"]): tokenize_text(r["title"] or "") for r in rows}
    raise ValueError("Posts DataFrame must contain ('title', 'body'), 'text', or 'title'")


def _attach_exact_jaccard(candidates: DataFrame, shingles: DataFrame) -> DataFrame:
    """Compute exact Jaccard from C2 shingles for candidate pairs."""
    dup_s = shingles.select(
        F.col("post_id").alias("_dup_id"),
        F.array_distinct(F.col("shingles")).alias("_dup_sh"),
    )
    cand_s = shingles.select(
        F.col("post_id").alias("_cand_id"),
        F.array_distinct(F.col("shingles")).alias("_cand_sh"),
    )
    joined = candidates.join(dup_s, candidates["dup_id"] == dup_s["_dup_id"], "inner").join(
        cand_s, candidates["cand_id"] == cand_s["_cand_id"], "inner"
    )
    inter = F.size(F.array_intersect(F.col("_dup_sh"), F.col("_cand_sh")))
    union = F.size(F.array_union(F.col("_dup_sh"), F.col("_cand_sh")))
    exact = F.when(union > 0, inter.cast("double") / union.cast("double")).otherwise(F.lit(0.0))
    return joined.select(
        candidates["dup_id"],
        candidates["cand_id"],
        candidates["est_sim"].cast("double").alias("est_sim"),
        exact.cast("double").alias("exact_jaccard"),
    )


class TfidfRanker:
    """Re-rank top-N candidates by TF-IDF cosine on words, then exact Jaccard."""

    name: str = "tfidf"

    def __init__(
        self,
        posts: DataFrame | None = None,
        candidate_pool: int = 50,
        min_df: int = 1,
    ) -> None:
        self.posts = posts
        self.candidate_pool = candidate_pool
        self.min_df = min_df

    def rank(
        self,
        candidates: DataFrame,
        shingles: DataFrame | None = None,
        top_k: int = 10,
        min_jaccard: float | None = None,
        posts: DataFrame | None = None,
    ) -> DataFrame:
        """Re-rank up to ``candidate_pool`` candidates per query by TF-IDF cosine."""
        if top_k < 1:
            raise ValueError(f"top_k must be >= 1, got {top_k}")
        text_df = posts if posts is not None else self.posts
        if text_df is None and shingles is not None:
            if {"title", "body"} <= set(shingles.columns) or "text" in shingles.columns:
                text_df = shingles
        if text_df is None:
            raise ValueError("TfidfRanker requires C1 posts text DataFrame (post_id, title, body)")

        spark = candidates.sparkSession
        if shingles is not None:
            scored = _attach_exact_jaccard(candidates, shingles)
            if min_jaccard is not None:
                scored = scored.filter(F.col("exact_jaccard") >= min_jaccard)
            pool_order = Window.partitionBy("dup_id").orderBy(
                F.col("exact_jaccard").desc(), F.col("est_sim").desc(), F.col("cand_id").asc()
            )
            tie_col = "exact_jaccard"
        else:
            scored = candidates.withColumn("tie_score", F.col("est_sim").cast("double"))
            if min_jaccard is not None:
                scored = scored.filter(F.col("est_sim") >= min_jaccard)
            pool_order = Window.partitionBy("dup_id").orderBy(
                F.col("est_sim").desc(), F.col("cand_id").asc()
            )
            tie_col = "tie_score"

        pool = (
            scored.withColumn("_pool_rank", F.row_number().over(pool_order))
            .filter(F.col("_pool_rank") <= self.candidate_pool)
            .select("dup_id", "cand_id", tie_col)
            .collect()
        )

        if not pool:
            return spark.createDataFrame(
                [],
                "dup_id long, cand_id long, rank int, score double, method string",
            )

        idf = _compute_idf_spark(text_df, min_df=self.min_df)

        pool_ids = {int(r["dup_id"]) for r in pool} | {int(r["cand_id"]) for r in pool}
        pool_ids_df = spark.createDataFrame([(pid,) for pid in sorted(pool_ids)], "post_id long")
        pool_posts = text_df.join(pool_ids_df, "post_id", "inner")
        pool_tokens = _extract_corpus_tokens(pool_posts)

        missing_ids = pool_ids - set(pool_tokens)
        if missing_ids:
            logger.warning(
                "%d query/candidate post_id(s) missing from posts text; cosine defaults to 0.0",
                len(missing_ids),
            )

        vectors = {pid: _tfidf_unit_vector(tokens, idf) for pid, tokens in pool_tokens.items()}

        by_query: dict[int, list[tuple[int, float, float]]] = {}
        for row in pool:
            dup_id = int(row["dup_id"])
            cand_id = int(row["cand_id"])
            tie_score = float(row[tie_col])
            cos = tfidf_cosine(vectors.get(dup_id, {}), vectors.get(cand_id, {}))
            by_query.setdefault(dup_id, []).append((cand_id, cos, tie_score))

        ranked_rows: list[tuple[int, int, int, float, str]] = []
        for dup_id in sorted(by_query):
            items = sorted(
                by_query[dup_id],
                key=lambda x: (-x[1], -x[2], x[0]),
            )[:top_k]
            for idx, (cand_id, cos, _) in enumerate(items, start=1):
                ranked_rows.append((dup_id, cand_id, idx, cos, self.name))

        return spark.createDataFrame(
            ranked_rows,
            "dup_id long, cand_id long, rank int, score double, method string",
        )


def _read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read Parquet with PyArrow fallback for Windows without ``winutils.exe``."""
    try:
        return spark.read.parquet(path)
    except Py4JJavaError:
        if not Path(path).exists():
            raise
        return spark.createDataFrame(pq.read_table(path).to_pylist())


def compare_with_tfidf(
    candidates: DataFrame,
    shingles: DataFrame,
    posts: DataFrame,
    answer_key: DataFrame | None = None,
    n_questions: int | None = None,
    top_k: int = 10,
    min_jaccard: float | None = None,
    candidate_pool: int = 50,
) -> list[dict[str, object]]:
    """Compare ``estimated``, ``exact_jaccard``, and ``tfidf`` rankings."""
    from src.evaluation.evaluate import evaluate
    from src.verification.rankers import RANKERS

    est_df = (
        RANKERS["estimated"]
        .rank(candidates, shingles=shingles, top_k=top_k, min_jaccard=min_jaccard)
        .cache()
    )
    exact_df = (
        RANKERS["exact_jaccard"]
        .rank(candidates, shingles=shingles, top_k=top_k, min_jaccard=min_jaccard)
        .cache()
    )
    tfidf_df = (
        TfidfRanker(posts=posts, candidate_pool=candidate_pool)
        .rank(candidates, shingles=shingles, top_k=top_k, min_jaccard=min_jaccard)
        .cache()
    )

    rows: list[dict[str, object]] = []
    for method_name, df in [
        ("estimated", est_df),
        ("exact_jaccard", exact_df),
        ("tfidf", tfidf_df),
    ]:
        stats = df.agg(
            F.countDistinct("dup_id").alias("n_queries"),
            F.count("*").alias("n_ranked"),
        ).collect()[0]
        row: dict[str, object] = {
            "method": method_name,
            "n_queries": int(stats["n_queries"] or 0),
            "n_ranked": int(stats["n_ranked"] or 0),
        }
        if answer_key is not None:
            metrics = evaluate(df, answer_key, n_questions=n_questions, max_rank=top_k)
            for key in (
                "recall@1",
                "recall@5",
                "recall@10",
                "precision@10",
                "map",
                "ndcg@10",
                "reduction_ratio",
            ):
                row[key] = round(float(metrics[key]), 6)
        rows.append(row)
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for TF-IDF re-ranking comparison."""
    parser = argparse.ArgumentParser(description="E4 TF-IDF cosine re-ranking (#40)")
    parser.add_argument("--input", "--candidates", dest="input", required=True)
    parser.add_argument("--shingles", required=True)
    parser.add_argument("--posts", required=True)
    parser.add_argument("--answer-key", default=None)
    parser.add_argument("--n-questions", type=int, default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--candidate-pool", type=int, default=50)
    parser.add_argument("--min-jaccard", type=float, default=None)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for TF-IDF re-ranking comparison."""
    args = parse_args(argv)
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = builder.master(args.master).appName("DupHunter-E4-TFIDF").getOrCreate()
    try:
        candidates = _read_parquet(spark, args.input)
        shingles = _read_parquet(spark, args.shingles)
        posts = _read_parquet(spark, args.posts)
        answer_key = _read_parquet(spark, args.answer_key) if args.answer_key else None
        rows = compare_with_tfidf(
            candidates=candidates,
            shingles=shingles,
            posts=posts,
            answer_key=answer_key,
            n_questions=args.n_questions,
            top_k=args.top_k,
            min_jaccard=args.min_jaccard,
            candidate_pool=args.candidate_pool,
        )
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
