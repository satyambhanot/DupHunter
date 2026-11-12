"""Unit tests for LSH band/row study and S-curve comparison (Issue #13)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.similarity.lsh_study import (
    lsh_threshold,
    measured_curve,
    parse_args,
    run_study,
    s_curve,
    simulate_lsh_retrieval,
)

# ---------------------------------------------------------------------------
# Acceptance Criterion 1: S-Curve Formula & Invariants
# ---------------------------------------------------------------------------


def test_s_curve_reference_point():
    """Verify s_curve(0.5, 32, 4) ≈ 0.8732."""
    prob = s_curve(0.5, 32, 4)
    assert prob == pytest.approx(0.8732, abs=1e-3)


def test_s_curve_boundaries():
    """Verify s_curve is 0 at s=0, and 1 at s=1."""
    assert s_curve(0.0, 32, 4) == 0.0
    assert s_curve(1.0, 32, 4) == 1.0

    # Also holds across arbitrary (b, r)
    for b in [1, 8, 16, 32, 64]:
        for r in [1, 2, 4, 8, 16]:
            assert s_curve(0.0, b, r) == 0.0
            assert s_curve(1.0, b, r) == 1.0


def test_s_curve_never_decreases():
    """Verify the curve never decreases for s in [0, 1]."""
    similarities = [i / 100.0 for i in range(101)]

    for b, r in [(16, 8), (32, 4), (64, 2)]:
        probs = [s_curve(s, b, r) for s in similarities]
        for i in range(len(probs) - 1):
            assert probs[i] <= probs[i + 1], (
                f"Decreased from {probs[i]} to {probs[i + 1]} at step {i} for b={b}, r={r}"
            )


def test_s_curve_input_validation():
    """Verify invalid parameters raise ValueError."""
    with pytest.raises(ValueError, match="b must be at least 1"):
        s_curve(0.5, 0, 4)

    with pytest.raises(ValueError, match="r must be at least 1"):
        s_curve(0.5, 32, 0)

    with pytest.raises(ValueError, match="Similarity s must be between 0.0 and 1.0"):
        s_curve(-0.1, 32, 4)

    with pytest.raises(ValueError, match="Similarity s must be between 0.0 and 1.0"):
        s_curve(1.1, 32, 4)


def test_s_curve_vectorized_and_list_support():
    """Verify s_curve accepts lists and preserves shapes."""
    sims = [0.0, 0.5, 1.0]
    res = s_curve(sims, 32, 4)
    assert len(res) == 3
    assert res[0] == 0.0
    assert res[1] == pytest.approx(0.8732, abs=1e-3)
    assert res[2] == 1.0


# ---------------------------------------------------------------------------
# Acceptance Criterion 2: Measured Curve Pure Function
# ---------------------------------------------------------------------------


def test_measured_curve_binning():
    """Verify measured_curve computes empirical fractions per similarity bin."""
    # Pair similarities and detection flags
    sims = [0.05, 0.08, 0.52, 0.55, 0.92, 0.95]
    detected = [False, False, True, False, True, True]

    centers, rates = measured_curve(sims, detected, bins=10)

    assert len(centers) == 10
    assert len(rates) == 10

    # Bin 0: [0.0, 0.1) -> 0 / 2 = 0.0
    assert rates[0] == 0.0

    # Bin 5: [0.5, 0.6) -> 1 / 2 = 0.5
    assert rates[5] == 0.5

    # Bin 9: [0.9, 1.0] -> 2 / 2 = 1.0
    assert rates[9] == 1.0

    # Unpopulated bins are None
    assert rates[1] is None


def test_measured_curve_validation():
    """Verify input length mismatch or invalid bins raise ValueError."""
    with pytest.raises(ValueError, match="Length mismatch"):
        measured_curve([0.1, 0.2], [True])

    with pytest.raises(ValueError, match="bins must be at least 1"):
        measured_curve([0.1], [True], bins=0)


def test_lsh_threshold():
    """Verify lsh_threshold returns (1/b)^(1/r)."""
    assert lsh_threshold(16, 8) == pytest.approx((1 / 16) ** (1 / 8))
    assert lsh_threshold(32, 4) == pytest.approx((1 / 32) ** (1 / 4))
    assert lsh_threshold(64, 2) == pytest.approx(0.125)


def test_simulate_lsh_retrieval():
    """Verify simulation closely approximates theoretical probability."""
    # Near threshold for b=32, r=4 (threshold ~0.420)
    p_theory = s_curve(0.5, 32, 4)
    p_sim = simulate_lsh_retrieval(0.5, 32, 4, n_trials=3000)
    assert abs(p_theory - p_sim) < 0.05


# ---------------------------------------------------------------------------
# Acceptance Criterion 3: Config & Results Artifacts
# ---------------------------------------------------------------------------


def test_config_e3_lsh_exists_and_valid():
    """Verify configs/e3_lsh.json has at least three settings and chosen setting."""
    cfg_path = Path("configs/e3_lsh.json")
    assert cfg_path.exists(), "configs/e3_lsh.json must exist"

    with cfg_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    settings = data.get("settings", [])
    assert len(settings) >= 3, "Must evaluate at least three settings"

    # All settings must satisfy b * r == signature_length (128)
    sig_len = data.get("signature_length", 128)
    for s in settings:
        assert s["b"] * s["r"] == sig_len

    assert "chosen_setting" in data
    assert data["chosen_setting"]["b"] == 32
    assert data["chosen_setting"]["r"] == 4


def test_run_study_generates_expected_files(tmp_path: Path):
    """Verify run_study creates CSV and Markdown report."""
    res = run_study(
        config_path="configs/e3_lsh.json",
        output_dir=tmp_path,
        sample_points=5,
        n_sim_trials=50,
    )

    assert (tmp_path / "s_curves.csv").exists()
    assert (tmp_path / "README.md").exists()
    assert len(res["settings"]) >= 3
    assert res["chosen"]["b"] == 32


def test_cli_parser():
    """Verify parse_args supports all arguments."""
    args = parse_args(
        [
            "--config",
            "configs/e3_lsh.json",
            "--output-dir",
            "results/e3_lsh",
            "--samples",
            "15",
            "--trials",
            "500",
            "--seed",
            "123",
        ]
    )
    assert args.config == "configs/e3_lsh.json"
    assert args.output_dir == "results/e3_lsh"
    assert args.samples == 15
    assert args.trials == 500
    assert args.seed == 123
