"""Tests for the E2 signature-length study (issue #28). No Spark needed."""

import numpy as np
import pandas as pd
import pytest

from src.similarity import minhash_study as study
from src.similarity.minhash import compute_signature
from src.similarity.representation_study import jaccard


def test_theoretical_std():
    assert study.theoretical_std(0.5, 128) == pytest.approx(0.0442, abs=1e-4)
    assert study.theoretical_std(0.0, 128) == 0.0 and study.theoretical_std(1.0, 128) == 0.0


def test_synthetic_pairs_have_the_requested_similarity():
    for level in (0.1, 0.5, 0.9):
        for a, b in study.synthetic_pairs(level, 5, size=40, seed=1):
            assert len(a) == len(b) == 40
            assert jaccard(a, b) == pytest.approx(level, abs=0.03)


def test_estimate_and_error():
    assert study.estimate(np.array([1, 2, 3, 4]), np.array([1, 2, 9, 9])) == 0.5
    assert study.mean_absolute_error([0.5, 0.2], [0.4, 0.4]) == pytest.approx(0.15)


def test_error_shrinks_with_the_signature_length():
    pairs = [("0.5", a, b) for a, b in study.synthetic_pairs(0.5, 60, seed=3)]
    summary = study.summarise_accuracy(study.accuracy_rows(pairs)).set_index("n")
    assert summary.loc[32, "mae"] > summary.loc[256, "mae"]
    assert summary.loc[32, "std"] > summary.loc[256, "std"]


def test_the_measured_spread_is_close_to_the_theory():
    pairs = [("0.5", a, b) for a, b in study.synthetic_pairs(0.5, 300, seed=4)]
    summary = study.summarise_accuracy(study.accuracy_rows(pairs, lengths=(64, 128))).set_index("n")
    for n in (64, 128):
        assert summary.loc[n, "std"] == pytest.approx(summary.loc[n, "theory_std"], rel=0.25)
        assert abs(summary.loc[n, "bias"]) < 0.02


def test_all_lengths_come_from_one_hash_family():
    values = sorted(range(1, 60, 3))
    assert list(compute_signature(values, 64, 42)) == list(compute_signature(values, 128, 42)[:64])
    rows = study.accuracy_rows([("x", set(values), set(values))], lengths=(32, 64))
    assert list(rows["estimate"]) == [1.0, 1.0] and list(rows["n"]) == [32, 64]


def test_accuracy_rows_have_one_row_per_pair_and_length():
    pairs = [("a", {1, 2, 3}, {2, 3, 4}), ("b", {1}, {9})]
    rows = study.accuracy_rows(pairs, lengths=(32, 64))
    assert len(rows) == 4 and set(rows.columns) == {"kind", "n", "exact", "estimate"}


def test_cost_row_columns_and_size_grows_with_n():
    sets = [list(range(i, i + 10)) for i in range(20)]
    small, large = study.cost_row(32, sets), study.cost_row(256, sets)
    assert set(small) == {"n", "questions", "time_s", "sig_bytes", "peak_mem_mb"}
    assert small["sig_bytes"] == 20 * 32 * 8 and large["sig_bytes"] == 20 * 256 * 8
    assert large["peak_mem_mb"] > 0


def test_plots_are_written(tmp_path):
    pairs = [("0.5", a, b) for a, b in study.synthetic_pairs(0.5, 10, seed=5)]
    pairs += [("duplicate", {1, 2}, {2, 3}), ("random", {1}, {5})]
    summary = study.summarise_accuracy(study.accuracy_rows(pairs))
    study.plot_accuracy(summary, tmp_path / "a.png")
    cost = pd.DataFrame([study.cost_row(n, [[1, 2, 3]]) for n in (32, 64)])
    study.plot_cost(cost, tmp_path / "c.png")
    assert (tmp_path / "a.png").stat().st_size > 1000 and (tmp_path / "c.png").stat().st_size > 1000
