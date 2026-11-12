"""Tests for the E1 representation study (issue #27)."""

import json
import random

import pandas as pd
import pytest

from src.similarity import representation_study as study


def test_jaccard():
    assert study.jaccard({1, 2, 3}, {2, 3, 4}) == 0.5
    assert study.jaccard(set(), set()) == 0.0
    assert study.jaccard({1}, {1}) == 1.0


def test_auc_perfect_none_and_ties():
    assert study.separation_auc([0.9, 0.8], [0.1, 0.2]) == 1.0
    assert study.separation_auc([0.1, 0.2], [0.9, 0.8]) == 0.0
    assert study.separation_auc([0.5, 0.5], [0.5, 0.5]) == 0.5
    assert study.separation_auc([0.5], [0.5]) == 0.5


def test_auc_matches_the_brute_force_definition():
    rng = random.Random(0)
    dup = [rng.random() for _ in range(40)]
    rand = [rng.random() * 0.8 for _ in range(50)]
    brute = sum((a > b) + 0.5 * (a == b) for a in dup for b in rand) / (len(dup) * len(rand))
    assert study.separation_auc(dup, rand) == pytest.approx(brute)


def test_auc_needs_both_kinds_of_pairs():
    with pytest.raises(ValueError):
        study.separation_auc([0.5], [])


def _pairs(spark):
    rows = [
        (
            1,
            2,
            "sort list python",
            "<p>how to sort a list</p>",
            "python list sorting",
            "<p>sort a list please</p>",
            True,
        ),
        (
            3,
            4,
            "install wifi driver",
            "<p>my wifi stopped</p>",
            "banana bread recipe",
            "<p>flour and eggs</p>",
            False,
        ),
    ]
    schema = (
        "post_id_a long, post_id_b long, title_a string, body_a string, "
        "title_b string, body_b string, "
        "is_dup boolean"
    )
    return spark.createDataFrame(rows, schema)


def test_config_drives_the_rows(spark, tmp_path):
    options = [
        {"name": "w1", "representation": "word", "k": 1, "fields": "title+body"},
        {"name": "w1_title", "representation": "word", "k": 1, "fields": "title"},
        {"name": "c5", "representation": "character", "k": 5, "fields": "title+body"},
    ]
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"options": options}))
    assert [o["name"] for o in study.load_options(path)] == ["w1", "w1_title", "c5"]
    table, sims = study.run_study(spark, _pairs(spark), study.load_options(path))
    assert list(table["option"]) == ["w1", "w1_title", "c5"] and set(sims) == {
        "w1",
        "w1_title",
        "c5",
    }
    assert {"median_dup", "auc", "mean_shingles", "dup_share_ge_0.4"} <= set(table.columns)


def test_duplicates_are_more_similar_than_the_unrelated_pair(spark):
    sims = study.pair_similarities(
        _pairs(spark),
        study.shingles_for(
            study.posts_of(_pairs(spark)),
            {"name": "w1", "representation": "word", "k": 1, "fields": "title+body"},
        ),
    )
    dup = sims.loc[sims["is_dup"], "jaccard"].iloc[0]
    rand = sims.loc[~sims["is_dup"], "jaccard"].iloc[0]
    assert dup > rand and rand == 0.0


def test_spark_jaccard_equals_the_python_jaccard(spark):
    pairs = _pairs(spark)
    shingles = study.shingles_for(
        study.posts_of(pairs),
        {"name": "w1", "representation": "word", "k": 1, "fields": "title+body"},
    )
    by_id = {r.post_id: set(r.shingles) for r in shingles.collect()}
    sims = study.pair_similarities(pairs, shingles)
    expected = sorted([study.jaccard(by_id[1], by_id[2]), study.jaccard(by_id[3], by_id[4])])
    assert sorted(sims["jaccard"]) == pytest.approx(expected, abs=1e-9)


def test_title_only_ignores_the_body(spark):
    posts = study.posts_of(_pairs(spark))
    full = {
        r.post_id: len(r.shingles)
        for r in study.shingles_for(
            posts, {"name": "a", "representation": "word", "k": 1, "fields": "title+body"}
        ).collect()
    }
    title = {
        r.post_id: len(r.shingles)
        for r in study.shingles_for(
            posts, {"name": "b", "representation": "word", "k": 1, "fields": "title"}
        ).collect()
    }
    assert all(title[i] <= full[i] for i in full) and title[1] < full[1]


def test_summarise_columns_and_shares():
    sims = pd.DataFrame(
        {
            "is_dup": [True] * 4 + [False] * 4,
            "jaccard": [0.5, 0.3, 0.1, 0.0, 0.0, 0.0, 0.1, 0.0],
            "n_a": 5,
            "n_b": 7,
        }
    )
    row = study.summarise("x", sims)
    assert row["dup_share_ge_0.4"] == 0.25 and row["dup_share_ge_0.2"] == 0.5
    assert row["mean_shingles"] == 6.0 and 0.5 < row["auc"] <= 1.0


def test_histograms_are_written(tmp_path):
    sims = pd.DataFrame(
        {"is_dup": [True, False, True, False], "jaccard": [0.6, 0.0, 0.3, 0.1], "n_a": 1, "n_b": 1}
    )
    path = tmp_path / "h" / "best.png"
    study.plot_histograms({"a": sims, "b": sims}, ["a", "b"], path)
    assert path.stat().st_size > 1000
