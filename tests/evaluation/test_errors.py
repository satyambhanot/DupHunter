"""Tests for the error analysis of the per-pair table (issue #33). Plain pandas, no Spark."""

import math

import pandas as pd
import pytest

from src.evaluation import errors


def _table(jaccard, found, **extra):
    n = len(jaccard)
    data = {
        "dup_id": range(n),
        "original_id": range(100, 100 + n),
        "found": found,
        "rank": [1 if f else None for f in found],
        "exact_jaccard": jaccard,
        "tags": [["python"]] * n,
        "body_len": [300] * n,
        "year": [2016] * n,
    }
    data.update(extra)
    return pd.DataFrame(data)


def test_recall_by_similarity_bins():
    df = _table([0.05] * 5 + [0.95] * 5, [False, False, False, True, False] + [True] * 5)
    table = errors.recall_by_similarity(df)
    first, last = table.iloc[0], table.iloc[-1]
    assert (first["n_pairs"], first["recall"]) == (5, pytest.approx(0.2))
    assert (last["n_pairs"], last["recall"]) == (5, 1.0)


def test_similarity_of_exactly_one_is_in_the_last_bin():
    table = errors.recall_by_similarity(_table([1.0], [True]))
    assert table.iloc[-1]["n_pairs"] == 1 and table["n_pairs"].sum() == 1


def test_empty_bin_has_nan_recall_and_no_error():
    table = errors.recall_by_similarity(_table([0.05], [True]))
    middle = table.iloc[4]
    assert middle["n_pairs"] == 0 and math.isnan(middle["recall"])


def test_small_bins_are_flagged():
    seven = errors.recall_by_similarity(_table([0.55] * 7, [True] * 7))
    twenty = errors.recall_by_similarity(_table([0.55] * 20, [True] * 20))
    assert bool(seven.iloc[5]["low_n"]) is True
    assert bool(twenty.iloc[5]["low_n"]) is False


def test_a_hit_needs_the_original_within_k():
    df = _table([0.5, 0.5], [True, True])
    df.loc[1, "rank"] = 12
    assert errors.hits(df, k=10).tolist() == [True, False]
    assert errors.hits(df, k=20).tolist() == [True, True]


def test_tags_count_once_per_tag():
    df = _table([0.5, 0.5], [True, False], tags=[["pandas"], ["pandas", "numpy"]])
    table = errors.recall_by_tag(df, top=5).set_index("bin")
    assert table.loc["pandas", "n_pairs"] == 2 and table.loc["pandas", "recall"] == 0.5
    assert table.loc["numpy", "n_pairs"] == 1 and table.loc["numpy", "recall"] == 0.0


def test_only_the_most_frequent_tags_are_kept():
    df = _table([0.5] * 3, [True] * 3, tags=[["a"], ["a"], ["b"]])
    assert list(errors.recall_by_tag(df, top=1)["bin"]) == ["a"]


def test_years_are_ordered():
    df = _table([0.5, 0.5, 0.5], [True, False, True], year=[2016, 2012, 2016])
    table = errors.recall_by_year(df)
    assert list(table["bin"]) == [2012, 2016]
    assert list(table["n_pairs"]) == [1, 2]


def test_length_bins():
    df = _table([0.5, 0.5], [True, False], body_len=[100, 800])
    table = errors.recall_by_length(df)
    assert table.iloc[0]["n_pairs"] == 1 and table.iloc[2]["n_pairs"] == 1


def test_missing_column_raises_key_error_naming_it():
    df = _table([0.5], [True]).drop(columns=["exact_jaccard"])
    with pytest.raises(KeyError, match="exact_jaccard"):
        errors.recall_by_similarity(df)


def test_sample_misses_is_deterministic_and_only_misses():
    df = _table([0.5] * 6, [True, False, False, True, False, False])
    sample = errors.sample_misses(df, n=3)
    assert sample["dup_id"].tolist() == [1, 2, 4]
    assert not errors.hits(sample).any()


def test_lsh_probability_matches_the_s_curve():
    assert errors.lsh_probability(0.5, 32, 4) == pytest.approx(0.8732, abs=1e-4)
    assert errors.lsh_probability(0.0, 32, 4) == 0.0 and errors.lsh_probability(1.0, 32, 4) == 1.0


def test_expected_column_with_b_and_r():
    table = errors.recall_by_similarity(_table([0.5], [True]), b=32, r=4)
    assert "expected" in table and table["expected"].is_monotonic_increasing


def test_plot_writes_a_png(tmp_path):
    table = errors.recall_by_similarity(_table([0.05] * 5 + [0.95] * 5, [True] * 10), b=32, r=4)
    path = tmp_path / "plots" / "sim.png"
    errors.plot_recall(table, "Recall by similarity", path)
    assert path.stat().st_size > 1000


def test_main_writes_tables_plots_and_misses(tmp_path):
    df = _table([0.05, 0.95, 0.5], [False, True, True])
    source = tmp_path / "per_pair.csv"
    df.assign(tags="python").to_csv(source, index=False)  # CSV keeps tags as one string
    df.to_parquet(tmp_path / "per_pair.parquet")
    out = tmp_path / "out"
    assert errors.main(["--per-pair", str(tmp_path / "per_pair.parquet"), "--out", str(out)]) == 0
    names = {p.name for p in out.iterdir()}
    assert {
        "recall_by_similarity.csv",
        "recall_by_tags.csv",
        "recall_by_length.csv",
        "recall_by_year.csv",
    } <= names
    assert {"recall_by_similarity.png", "misses.csv"} <= names


def test_an_empty_column_says_which_input_is_missing():
    df = _table([None, None], [True, False])
    with pytest.raises(ValueError, match="--exact"):
        errors.recall_by_similarity(df)
    with pytest.raises(ValueError, match="--posts-text"):
        errors.recall_by_tag(_table([0.5], [True], tags=[None]))


def test_main_skips_a_breakdown_whose_input_is_missing(tmp_path, capsys):
    df = _table([None, None], [True, False])  # evaluate.py was run without --exact
    df.to_parquet(tmp_path / "per_pair.parquet")
    out = tmp_path / "out"
    assert errors.main(["--per-pair", str(tmp_path / "per_pair.parquet"), "--out", str(out)]) == 0
    names = {p.name for p in out.iterdir()}
    assert "recall_by_year.csv" in names and "recall_by_similarity.csv" not in names
    assert "skipped the breakdown by similarity" in capsys.readouterr().err
