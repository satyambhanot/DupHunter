"""Tests for the Amdahl fit (issue #15)."""

import pytest

from src.scaling import amdahl


def _times(serial, cores, t1=100.0):
    return [t1 * (serial + (1 - serial) / p) for p in cores]


def test_recovers_the_serial_share():
    cores = [1, 2, 4, 8]
    assert amdahl.fit_serial_share(cores, _times(0.2, cores)) == pytest.approx(0.20, abs=0.01)


def test_perfect_scaling_has_no_serial_part():
    assert amdahl.fit_serial_share([1, 2, 4, 8], [100, 50, 25, 12.5]) == pytest.approx(
        0.0, abs=0.01
    )


def test_no_scaling_is_all_serial():
    assert amdahl.fit_serial_share([1, 2, 4, 8], [100, 100, 100, 100]) == pytest.approx(
        1.0, abs=0.01
    )


def test_noisy_data_stays_between_zero_and_one():
    share = amdahl.fit_serial_share([1, 2, 4, 8], [100, 48, 27, 11])
    assert 0.0 <= share <= 1.0


def test_needs_two_distinct_core_counts():
    with pytest.raises(ValueError):
        amdahl.fit_serial_share([4, 4], [10, 11])
    with pytest.raises(ValueError):
        amdahl.fit_serial_share([1, 2], [10])


def test_speedup_relative_to_the_fewest_cores():
    assert amdahl.speedup([1, 2, 4], [100, 50, 25]) == [1.0, 2.0, 4.0]
