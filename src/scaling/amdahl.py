"""Amdahl's law: how much of a run is serial, from run times at different core counts."""

from collections.abc import Sequence

import numpy as np


def fit_serial_share(cores: Sequence[float], times: Sequence[float]) -> float:
    """Fit ``T(p) = T1 * (s + (1 - s) / p)`` by least squares and return the serial share ``s``.

    The model is linear in ``a = T1 * s`` and ``b = T1 * (1 - s)``: ``T = a + b / p``. The share is
    ``a / (a + b)``, clipped to [0, 1] because noisy measurements can push it slightly outside.

    Args:
        cores: core counts of the runs (at least two different values).
        times: run times at those core counts (any unit).

    Raises:
        ValueError: if the inputs differ in length or have fewer than two distinct core counts.
    """
    if len(cores) != len(times):
        raise ValueError("cores and times must have the same length")
    if len(set(cores)) < 2:
        raise ValueError("need at least two different core counts")
    design = np.column_stack([np.ones(len(cores)), 1.0 / np.asarray(cores, dtype=float)])
    (a, b), *_ = np.linalg.lstsq(design, np.asarray(times, dtype=float), rcond=None)
    total = a + b
    if total <= 0:
        raise ValueError("the fitted run time at one core is not positive")
    return float(min(1.0, max(0.0, a / total)))


def speedup(cores: Sequence[float], times: Sequence[float]) -> list[float]:
    """Speedup of every run relative to the slowest-core run (the smallest core count)."""
    baseline = times[min(range(len(cores)), key=lambda i: cores[i])]
    return [baseline / t for t in times]
