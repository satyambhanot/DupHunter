"""End-to-end tests of the profile, answer-key and export commands (issue #6)."""

import json
from datetime import datetime

from src.etl import answer_key, exports, profile

T = [datetime(2015, 1, 1), datetime(2016, 1, 1), datetime(2017, 1, 1)]
META = "post_id long, post_type int, parent_id long, created timestamp, tags array<string>, owner_user_id long, closed boolean"  # noqa: E501
TEXT = "post_id long, created timestamp, tags array<string>, title string, body string"
LINKS = "id long, created timestamp, post_id long, related_post_id long, link_type int"


def _site(spark, tmp_path, tag="python"):
    """Write a three-question site: 2 duplicates 1, question 3 is unrelated."""
    root = tmp_path / "site"
    questions = [(1, T[0]), (2, T[1]), (3, T[2])]
    meta = [(i, 1, None, t, [tag], 7, False) for i, t in questions]
    text = [(i, t, [tag], f"title {i}", f"body {i}") for i, t in questions]
    spark.createDataFrame(meta, META).write.parquet(str(root / "posts_meta"))
    spark.createDataFrame(text, TEXT).write.parquet(str(root / "posts_text"))
    spark.createDataFrame([(1, T[1], 2, 1, 3)], LINKS).write.parquet(str(root / "post_links"))
    return root


def test_the_three_commands_end_to_end(spark, tmp_path):
    root = _site(spark, tmp_path)
    stats = tmp_path / "stats.md"
    assert profile.main(["--parquet-root", str(root), "--site", "toy", "--output", str(stats)]) == 0
    assert (
        "| Questions | 3 |" in stats.read_text()
        and "| Duplicate links (type 3) | 1 |" in stats.read_text()
    )

    key_path = tmp_path / "key"
    assert answer_key.main(["--parquet-root", str(root), "--output", str(key_path)]) == 0
    key = spark.read.parquet(str(key_path)).collect()
    assert [(r.dup_id, r.original_id) for r in key] == [(2, 1)]

    config = tmp_path / "sample.json"
    config.write_text(json.dumps({"sample_size": 3, "dup_pairs": 1, "random_pairs": 1}))
    out = tmp_path / "exports"
    argv = ["--parquet-root", str(root), "--answer-key", str(key_path), "--output", str(out)]
    assert exports.main([*argv, "--config", str(config)]) == 0
    assert spark.read.parquet(str(out / "python_tags")).count() == 3
    assert spark.read.parquet(str(out / "python_sample")).count() == 3
    pairs = spark.read.parquet(str(out / "pairs_e1")).collect()
    assert sum(r.is_dup for r in pairs) == 1


def test_an_empty_tag_keeps_every_question_for_super_user(spark, tmp_path):
    root = _site(spark, tmp_path, tag="windows")  # Super User has no python tag
    key_path = tmp_path / "key"
    answer_key.main(["--parquet-root", str(root), "--output", str(key_path)])
    out = tmp_path / "exports"
    argv = ["--parquet-root", str(root), "--answer-key", str(key_path), "--output", str(out)]
    exports.main([*argv, "--tag", "python"])
    assert spark.read.parquet(str(out / "python_tags")).count() == 0  # nothing has the tag
    exports.main([*argv, "--tag", ""])
    assert spark.read.parquet(str(out / "python_tags")).count() == 3
