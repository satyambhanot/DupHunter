"""Tests for the E3 measurements on real questions (issue #13). Pure functions, no Spark."""

import math

import numpy as np
import pytest

from src.similarity import lsh_measure as measure
from src.similarity.lsh import s_curve


def test_collision_needs_a_whole_band_to_match():
    a = np.array([1, 2, 3, 4])
    assert measure.collides(a, np.array([1, 2, 9, 9]), b=2, r=2) is True  # band 0 matches
    assert measure.collides(a, np.array([1, 9, 3, 9]), b=2, r=2) is False  # one value each, no band
    assert measure.collides(a, np.array([9, 9, 9, 9]), b=2, r=2) is False
    assert measure.collides(a, a, b=1, r=4) is True


def test_a_wrong_band_shape_is_rejected():
    with pytest.raises(ValueError, match="does not match"):
        measure.collides(np.arange(4), np.arange(4), b=3, r=2)


def test_the_measured_share_per_bin():
    exact = [0.05, 0.05, 0.05, 0.05, 0.95, 0.95]
    hit = [False, False, False, True, True, True]
    curve = measure.measured_curve(exact, hit, b=32, r=4)
    assert len(curve) == 10
    assert curve.iloc[0]["pairs"] == 4 and curve.iloc[0]["measured"] == 0.25
    assert curve.iloc[-1]["pairs"] == 2 and curve.iloc[-1]["measured"] == 1.0


def test_similarity_one_is_in_the_last_bin_and_empty_bins_are_nan():
    curve = measure.measured_curve([1.0], [True], b=16, r=8)
    assert curve.iloc[-1]["pairs"] == 1 and curve["pairs"].sum() == 1
    assert math.isnan(curve.iloc[3]["measured"])


def test_the_predicted_value_is_the_s_curve_at_the_middle_of_the_bin():
    curve = measure.measured_curve([0.5], [True], b=32, r=4)
    middle = curve.iloc[5]
    assert middle["predicted"] == pytest.approx(s_curve(0.55, 32, 4))


def test_curve_error_ignores_small_bins():
    curve = measure.measured_curve([0.55] * 30 + [0.05], [True] * 30 + [True], b=32, r=4)
    assert measure.curve_error(curve) == pytest.approx(1 - s_curve(0.55, 32, 4))


def test_the_plot_is_written(tmp_path):
    curve = measure.measured_curve([0.55] * 30 + [0.15] * 30, [True] * 30 + [False] * 30, b=32, r=4)
    path = tmp_path / "plots" / "s.png"
    measure.plot_curves({(32, 4): curve, (64, 2): curve}, path)
    assert path.stat().st_size > 1000


def test_the_settings_split_the_signature():
    assert all(b * r == measure.N for b, r in measure.SETTINGS)
