"""Tests for the answer key, the dataset profile and the exports (issue #6)."""

from datetime import datetime

import pytest

from src.etl import answer_key, exports, profile

T1 = datetime(2015, 1, 1)
T2 = datetime(2016, 1, 1)
T3 = datetime(2017, 1, 1)


def _questions(spark, rows):
    return spark.createDataFrame(rows, "post_id long, created timestamp")


def _links(spark, rows):
    return spark.createDataFrame(rows, "post_id long, related_post_id long, link_type int")


QUESTIONS = [(1, T1), (2, T2), (3, T3), (4, T3)]


# ---------------------------------------------------------------- answer key
def test_pair_is_oriented_newer_first(spark):
    key = answer_key.build_answer_key(_links(spark, [(2, 1, 3)]), _questions(spark, QUESTIONS))
    row = key.collect()[0]
    assert (row.dup_id, row.original_id, row.created) == (2, 1, T2)


def test_wrong_direction_is_swapped_and_counted(spark):
    links = _links(spark, [(1, 2, 3)])  # the older question links to the newer one
    questions = _questions(spark, QUESTIONS)
    row = answer_key.build_answer_key(links, questions).collect()[0]
    assert (row.dup_id, row.original_id) == (2, 1)
    assert answer_key.answer_key_counts(links, questions)["swapped"] == 1


def test_only_duplicate_links_are_used(spark):
    links = _links(spark, [(2, 1, 3), (3, 1, 1)])
    key = answer_key.build_answer_key(links, _questions(spark, QUESTIONS))
    assert [(r.dup_id, r.original_id) for r in key.collect()] == [(2, 1)]


def test_missing_question_is_dropped_and_counted(spark):
    links = _links(spark, [(2, 1, 3), (2, 99, 3)])
    questions = _questions(spark, QUESTIONS)
    assert answer_key.build_answer_key(links, questions).count() == 1
    counts = answer_key.answer_key_counts(links, questions)
    assert counts["dropped_missing"] == 1 and counts["pairs_kept"] == 1


def test_equal_timestamps_are_dropped_and_counted(spark):
    links = _links(spark, [(3, 4, 3)])
    questions = _questions(spark, QUESTIONS)
    assert answer_key.build_answer_key(links, questions).count() == 0
    assert answer_key.answer_key_counts(links, questions)["dropped_equal_time"] == 1


def test_a_pair_linked_both_ways_appears_once(spark):
    links = _links(spark, [(2, 1, 3), (1, 2, 3), (2, 1, 3)])
    assert answer_key.build_answer_key(links, _questions(spark, QUESTIONS)).count() == 1


# ------------------------------------------------------------------- profile
def _meta(spark):
    rows = [
        (1, 1, None, T1, ["python", "list"], 7, False),
        (2, 1, None, T2, ["python"], None, False),
        (3, 1, None, T2, ["java"], 8, False),
        (4, 1, None, T3, [], 9, False),
        (5, 2, 1, T2, [], 8, False),
    ]
    schema = (
        "post_id long, post_type int, parent_id long, created timestamp, tags array<string>, "
        "owner_user_id long, closed boolean"
    )
    return spark.createDataFrame(rows, schema)


def _text(spark):
    rows = [
        (1, T1, ["python"], "t1", "<p>x</p><code>a = 1</code>"),
        (2, T2, ["python"], "t2", ""),
        (3, T2, [], "t3", "<p>abcd</p>"),
        (4, T3, [], "t4", None),
    ]
    return spark.createDataFrame(
        rows, "post_id long, created timestamp, tags array<string>, title string, body string"
    )


def test_profile_numbers(spark):
    links = _links(spark, [(2, 1, 3), (3, 1, 1)])
    stats = profile.compute_profile(_meta(spark), _text(spark), links)
    assert stats["questions"] == 4 and stats["answers"] == 1
    assert stats["python_questions"] == 2
    assert stats["missing_owners"] == 1
    assert stats["duplicate_links"] == 1 and stats["linked_links"] == 1
    assert stats["questions_per_year"] == {2015: 1, 2016: 2, 2017: 1}
    assert stats["text_questions"] == 4
    assert stats["empty_bodies"] == 2  # empty string and null
    assert stats["code_share"] == pytest.approx(0.25)


def test_profile_markdown_section_is_replaced_not_duplicated(tmp_path):
    stats = {
        "questions": 10, "answers": 5, "python_questions": 3, "missing_owners": 1,
        "questions_per_year": {2015: 10}, "duplicate_links": 2, "linked_links": 4,
        "text_questions": 10, "empty_bodies": 0, "code_share": 0.5, "mean_body_length": 100.0,
    }  # fmt: skip
    path = tmp_path / "stats.md"
    profile.write_profile(stats, "superuser", path)
    profile.write_profile(stats | {"questions": 11}, "superuser", path)
    profile.write_profile(stats, "stackoverflow", path)
    text = path.read_text()
    assert text.count("<!-- site:superuser -->") == 1
    assert "| Questions | 11 |" in text and "| Questions | 10 |" in text
    assert "<!-- site:stackoverflow -->" in text


# ------------------------------------------------------------------- exports
def _many_questions(spark, n=200):
    rows = [(i, T1, ["python"], f"title {i}", f"body {i}") for i in range(1, n + 1)]
    return spark.createDataFrame(
        rows, "post_id long, created timestamp, tags array<string>, title string, body string"
    )


def _key(spark, pairs):
    return spark.createDataFrame([(a, b) for a, b in pairs], "dup_id long, original_id long")


def test_sample_has_exactly_n_and_contains_every_pair_member(spark):
    key = _key(spark, [(10, 1), (20, 2), (30, 3)])
    sample = exports.make_sample(_many_questions(spark), key, n=50, seed=0)
    ids = {r.post_id for r in sample.collect()}
    assert sample.count() == 50 and len(ids) == 50
    assert {10, 1, 20, 2, 30, 3} <= ids


def test_sample_is_deterministic_and_depends_on_the_seed(spark):
    key = _key(spark, [(10, 1)])
    questions = _many_questions(spark)

    def ids(seed):
        return {r.post_id for r in exports.make_sample(questions, key, 40, seed).collect()}

    assert ids(0) == ids(0)
    assert ids(0) != ids(1)


def test_sample_rejects_too_many_pair_members(spark):
    key = _key(spark, [(10, 1), (20, 2)])
    with pytest.raises(ValueError, match="do not fit"):
        exports.make_sample(_many_questions(spark), key, n=3, seed=0)


def test_pairs_are_half_duplicates_half_random_with_text(spark):
    key = _key(spark, [(i + 100, i) for i in range(1, 21)])
    questions = _many_questions(spark, 300)
    pairs = exports.make_pairs(key, questions, dup_pairs=10, random_pairs=10, seed=0)
    rows = pairs.collect()
    assert sum(r.is_dup for r in rows) == 10
    assert sum(not r.is_dup for r in rows) == 10
    known = {(i + 100, i) for i in range(1, 21)} | {(i, i + 100) for i in range(1, 21)}
    assert all((r.post_id_a, r.post_id_b) not in known for r in rows if not r.is_dup)
    assert all(
        r.title_a == f"title {r.post_id_a}" and r.body_b == f"body {r.post_id_b}" for r in rows
    )
    assert pairs.columns == [
        "post_id_a",
        "post_id_b",
        "title_a",
        "body_a",
        "title_b",
        "body_b",
        "is_dup",
    ]


def test_python_tags_export(spark):
    meta = _meta(spark)
    ids = sorted(r.post_id for r in exports.export_python_tags(meta).collect())
    assert ids == [1, 2]


def test_load_config_defaults_and_override(tmp_path):
    assert exports.load_config(None)["sample_size"] == 50_000
    path = tmp_path / "c.json"
    path.write_text('{"seed": 5}')
    config = exports.load_config(str(path))
    assert config["seed"] == 5 and config["dup_pairs"] == 2_000
