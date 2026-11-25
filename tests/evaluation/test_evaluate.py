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


def test_exact_for_pairs_covers_found_and_missed_pairs(spark):
    key = _key(spark, [(10, 1), (20, 5), (30, 7)])
    shingles = spark.createDataFrame(
        [(10, [1, 2, 3]), (1, [2, 3, 4]), (20, [8]), (5, [9]), (30, [1])],  # 7 has no shingles
        "post_id long, shingles array<int>",
    )
    exact = {
        (r.dup_id, r.cand_id): r.jaccard for r in evaluate.exact_for_pairs(key, shingles).collect()
    }
    assert exact == {(10, 1): pytest.approx(0.5), (20, 5): 0.0}
    # a pair without shingles keeps an empty similarity in the table instead of disappearing
    table = evaluate.per_pair_table(
        _cands(spark, [(10, 1, 0.9)]), key, exact=evaluate.exact_for_pairs(key, shingles)
    )
    rows = {r.dup_id: (r.found, r.exact_jaccard) for r in table.collect()}
    assert rows[10] == (True, pytest.approx(0.5))
    assert rows[20] == (False, 0.0)
    assert rows[30] == (False, None)


def test_command_line_computes_exact_similarity_from_shingles(spark, tmp_path):
    _cands(spark, [(10, 2, 0.8)]).write.parquet(str(tmp_path / "cands"))
    _key(spark, [(10, 2)]).write.parquet(str(tmp_path / "key"))
    spark.createDataFrame(
        [(10, [1, 2]), (2, [1, 2])], "post_id long, shingles array<int>"
    ).write.parquet(str(tmp_path / "shingles"))
    argv = ["--candidates", str(tmp_path / "cands"), "--answer-key", str(tmp_path / "key")]
    argv += ["--out", str(tmp_path / "m.csv"), "--experiment", "E3", "--n-questions", "10"]
    argv += ["--per-pair-out", str(tmp_path / "pp"), "--shingles", str(tmp_path / "shingles")]
    assert evaluate.main(argv) == 0
    (row,) = spark.read.parquet(str(tmp_path / "pp")).collect()
    assert row.exact_jaccard == pytest.approx(1.0)
    with pytest.raises(SystemExit):  # the two sources of similarity exclude each other
        evaluate.parse_args(argv + ["--exact", "x"])


def test_command_line_end_to_end(spark, tmp_path):
    candidates = _cands(spark, [(10, 1, 0.9), (10, 2, 0.8), (20, 5, 0.9)])
    candidates.write.parquet(str(tmp_path / "cands"))
    _key(spark, [(10, 2), (20, 5)]).write.parquet(str(tmp_path / "key"))
    csv_path = tmp_path / "metrics.csv"
    per_pair = tmp_path / "per_pair"
    argv = ["--candidates", str(tmp_path / "cands"), "--answer-key", str(tmp_path / "key")]
    argv += ["--out", str(csv_path), "--experiment", "E3", "--settings", '{"b": 32}']
    argv += ["--n-questions", "1000", "--per-pair-out", str(per_pair)]
    assert evaluate.main(argv) == 0
    with csv_path.open() as handle:
        row = next(csv.DictReader(handle))
    assert row["experiment"] == "E3" and row["settings_json"] == '{"b": 32}'
    assert float(row["recall@1"]) == pytest.approx(0.5)  # question 20 ranks its original first
    assert float(row["reduction_ratio"]) == pytest.approx(1 - 3 / 499500)
    assert spark.read.parquet(str(per_pair)).count() == 2


def test_an_empty_answer_key_gives_zero_queries_and_no_error(spark):
    empty = spark.createDataFrame([], "dup_id long, original_id long, created timestamp")
    result = evaluate.evaluate(_cands(spark, [(10, 1, 0.9)]), empty, n_questions=10)
    assert result["n_queries"] == 0 and result["recall@10"] == 0.0


def test_restrict_answer_key_keeps_pairs_with_both_questions_in_the_universe(spark):
    key = _key(spark, [(2, 1), (4, 3), (3, 1)])
    universe = spark.createDataFrame([(1,), (2,), (2,)], ["post_id"])  # a repeated id is harmless
    kept = evaluate.restrict_answer_key(key, universe).collect()
    assert [(r.dup_id, r.original_id) for r in kept] == [(2, 1)]
    assert evaluate.restrict_answer_key(key, universe.limit(0)).count() == 0


def test_a_sample_is_not_scored_against_pairs_outside_it(spark):
    # Questions 1 and 2 are the sample; the pair (4, 3) is outside it and must not count as a miss.
    candidates = _cands(spark, [(2, 1, 0.9)])
    key = _key(spark, [(2, 1), (4, 3)])
    universe = spark.createDataFrame([(1,), (2,)], ["post_id"])
    whole = evaluate.evaluate(candidates, key, n_questions=2)
    scoped = evaluate.evaluate(candidates, evaluate.restrict_answer_key(key, universe), 2)
    assert whole["recall@10"] == pytest.approx(0.5)
    assert scoped["recall@10"] == pytest.approx(1.0) and scoped["n_queries"] == 1


def test_command_line_universe_restricts_the_key_and_sets_the_question_count(spark, tmp_path):
    _cands(spark, [(2, 1, 0.9)]).write.parquet(str(tmp_path / "cands"))
    _key(spark, [(2, 1), (4, 3)]).write.parquet(str(tmp_path / "key"))
    spark.createDataFrame([(1,), (2,), (5,)], ["post_id"]).write.parquet(str(tmp_path / "sample"))
    csv_path = tmp_path / "metrics.csv"
    argv = ["--candidates", str(tmp_path / "cands"), "--answer-key", str(tmp_path / "key")]
    argv += ["--out", str(csv_path), "--experiment", "E3", "--universe", str(tmp_path / "sample")]
    assert evaluate.main(argv) == 0
    with csv_path.open() as handle:
        row = next(csv.DictReader(handle))
    assert float(row["recall@10"]) == pytest.approx(1.0)
    assert float(row["reduction_ratio"]) == pytest.approx(1 - 1 / 3)  # 1 candidate of 3 questions
