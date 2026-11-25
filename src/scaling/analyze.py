"""Speedup analysis of the E8 scaling runs: ``python -m src.scaling.analyze``.

Reads ``runs.csv`` (core count and second machine, tests A and B) and, when it exists,
``runs_data_scaling.csv`` (data size, test C), both written by WS4 and never edited here. Writes a
summary table and the plots to ``results/e8_scaling/analysis/``.
"""

import argparse
import csv
import json
import statistics
import sys
from collections.abc import Sequence
from pathlib import Path

from src.scaling.amdahl import fit_serial_share, speedup
from src.scaling.commands import parse_cores

SUMMARY_COLUMNS = [
    "test", "cores", "fraction", "runs", "median_s", "min_s", "max_s",
    "slowest_task_s", "median_task_s", "shuffle_write_gb", "same_runs_as",
]  # fmt: skip

SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, ORANGE = "#2a78d6", "#eb6834"


def load_runs(path: str | Path) -> list[dict[str, str]]:
    """The rows of a ``runs.csv`` file."""
    with Path(path).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"{path} has no runs")
    return rows


def group_runs(rows: Sequence[dict]) -> dict[tuple[str, str, float], list[dict]]:
    """Rows grouped by setting ``(test, cores, fraction)``, each group ordered by run number."""
    groups: dict[tuple[str, str, float], list[dict]] = {}
    for row in sorted(rows, key=lambda r: int(r["run"])):
        key = (row["test"], row["cores"], float(row["fraction"]))
        groups.setdefault(key, []).append(row)
    return groups


def _fingerprint(runs: Sequence[dict]) -> tuple:
    return tuple((r["total_ms"], r["shuffle_write_bytes"], r.get("stage_ms_json")) for r in runs)


def summarise(rows: Sequence[dict]) -> list[dict]:
    """One row per setting: median, minimum and maximum run time, task times and shuffle size.

    ``same_runs_as`` names an earlier setting whose runs are identical, which means the same
    measurements are listed twice and must not be counted as separate evidence.
    """
    seen: dict[tuple, str] = {}
    summary = []
    for (test, cores, fraction), runs in sorted(group_runs(rows).items()):
        times = [float(r["total_ms"]) / 1000 for r in runs]
        fingerprint = _fingerprint(runs)
        summary.append(
            {
                "test": test,
                "cores": cores,
                "fraction": fraction,
                "runs": len(runs),
                "median_s": round(statistics.median(times), 1),
                "min_s": round(min(times), 1),
                "max_s": round(max(times), 1),
                "slowest_task_s": round(
                    statistics.median(float(r["slowest_task_ms"]) for r in runs) / 1000, 1
                ),
                "median_task_s": round(
                    statistics.median(float(r["median_task_ms"]) for r in runs) / 1000, 2
                ),
                "shuffle_write_gb": round(
                    statistics.median(float(r["shuffle_write_bytes"]) for r in runs) / 1e9, 2
                ),
                "same_runs_as": seen.get(fingerprint, ""),
            }
        )
        seen.setdefault(fingerprint, f"{test} {cores}")
    return summary


def core_scaling(summary: Sequence[dict]) -> dict:
    """Speedup, efficiency and the Amdahl fit of test A, from the median run time per core count.

    Returns a dict with ``cores``, ``times``, ``speedup``, ``efficiency``, ``serial_share`` and
    ``fitted_speedup`` (the speedup the fitted model gives at each measured core count).
    """
    points = sorted(
        (parse_cores(row["cores"])[1], row["median_s"]) for row in summary if row["test"] == "A"
    )
    if len(points) < 2:
        raise ValueError("test A needs at least two core counts")
    cores = [c for c, _ in points]
    times = [t for _, t in points]
    share = fit_serial_share(cores, times)
    measured = speedup(cores, times)
    base = cores[0]

    def model(p: float) -> float:
        return share + (1 - share) / p

    return {
        "cores": cores,
        "times": times,
        "speedup": measured,
        "efficiency": [s * base / c for s, c in zip(measured, cores, strict=True)],
        "serial_share": share,
        "fitted_speedup": [model(base) / model(c) for c in cores],
    }


def stage_medians(rows: Sequence[dict]) -> dict[tuple[str, str, float], dict[int, float]]:
    """Median duration in seconds of every stage, per setting, from ``stage_ms_json``."""
    medians = {}
    for key, runs in group_runs(rows).items():
        durations: dict[int, list[float]] = {}
        for run in runs:
            for stage, ms in json.loads(run["stage_ms_json"]).items():
                durations.setdefault(int(stage), []).append(ms / 1000)
        medians[key] = {stage: statistics.median(values) for stage, values in durations.items()}
    return medians


def dominant_stages(stages: dict[int, float], count: int = 3) -> list[int]:
    """The ids of the ``count`` longest stages, in id order."""
    return sorted(sorted(stages, key=stages.get, reverse=True)[:count])


def pause_stages(
    stages: dict[int, float],
    reference: dict[int, float],
    long_s: float = 5.0,
    short_s: float = 2.0,
) -> tuple[list[int], float]:
    """Stages that take at least ``long_s`` here but less than ``short_s`` in ``reference``.

    Returns their ids and how many more seconds they take in total than in ``reference``.
    """
    ids = sorted(s for s, d in stages.items() if d >= long_s and reference.get(s, 0.0) < short_s)
    return ids, sum(stages[s] - reference.get(s, 0.0) for s in ids)


def write_stage_table(medians: dict[tuple[str, str, float], dict[int, float]], path: Path) -> None:
    """Write the median stage durations: one row per stage, one column per setting."""
    keys = sorted(medians)
    stages = sorted({s for values in medians.values() for s in values})
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["stage", *[f"{t} {c} {f:g}" for t, c, f in keys]])
        for stage in stages:
            cells = [medians[k].get(stage) for k in keys]
            writer.writerow([stage, *["" if c is None else round(c, 2) for c in cells]])


def print_stage_findings(medians: dict, summary: Sequence[dict]) -> None:
    """Print the dominant stages of test A and the extra pauses of the test B settings."""
    test_a = sorted(
        (parse_cores(r["cores"])[1], ("A", r["cores"], r["fraction"]))
        for r in summary
        if r["test"] == "A"
    )
    (low, low_key), (high, high_key) = test_a[0], test_a[-1]
    top = dominant_stages(medians[low_key])
    totals = {(r["test"], r["cores"], r["fraction"]): r["median_s"] for r in summary}
    for cores, key in test_a:
        share = sum(medians[key][s] for s in top) / totals[key]
        print(f"  stages {top} take {share:.0%} of the run at {cores} cores")
    ratios = ", ".join(f"{s}: {medians[low_key][s] / medians[high_key][s]:.2f}" for s in top)
    print(f"  their speedup from {low} to {high} cores: {ratios}")
    for key in sorted(medians):
        if key[0] == "B" and key != ("B", "B:8", key[2]):
            ids, extra = pause_stages(medians[key], medians[high_key])
            print(
                f"  {key[1]}: {len(ids)} stages of 5 s or more that take under 2 s on {high} cores "
                f"of one laptop, {extra:.0f} s extra ({extra / totals[key]:.0%} of the run)"
            )


def write_summary(summary: Sequence[dict], path: str | Path) -> None:
    """Write the per-setting table as CSV."""
    with Path(path).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(summary)


def _pyplot():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _style(ax, title: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK, fontsize=11, loc="left")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.set_axisbelow(True)


def plot_speedup(scaling: dict, path: str | Path) -> None:
    """Measured speedup of test A against the fitted Amdahl curve and ideal speedup."""
    plt = _pyplot()
    cores, base = scaling["cores"], scaling["cores"][0]
    fig, ax = plt.subplots(figsize=(6, 4.2), facecolor=SURFACE)
    _style(ax, "Test A: speedup against cores (one laptop, median of 3 runs)")
    ax.grid(color=GRID, linewidth=0.8)
    ax.plot(cores, [c / base for c in cores], color=MUTED, linewidth=1, linestyle=":",
            label="ideal")  # fmt: skip
    ax.plot(cores, scaling["fitted_speedup"], color=ORANGE, linewidth=2, linestyle="--",
            label=f"Amdahl fit, serial share {scaling['serial_share']:.2f}")  # fmt: skip
    ax.plot(cores, scaling["speedup"], color=BLUE, linewidth=2, marker="o", markersize=8,
            markeredgecolor=SURFACE, label="measured")  # fmt: skip
    for c, s in zip(cores, scaling["speedup"], strict=True):
        ax.annotate(f"{s:.2f}×", (c, s), textcoords="offset points", xytext=(7, -11),
                    color=INK, fontsize=9)  # fmt: skip
    ax.set_xticks(cores)
    ax.set_xlabel("cores", color=MUTED)
    ax.set_ylabel(f"speedup over {base} core" + ("s" if base != 1 else ""), color=MUTED)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def plot_machines(summary: Sequence[dict], rows: Sequence[dict], path: str | Path) -> None:
    """Median run time of every test B setting, with each run as a dot."""
    plt = _pyplot()
    settings = [r for r in summary if r["test"] == "B"]
    if not settings:
        return
    groups = group_runs(rows)
    fig, ax = plt.subplots(figsize=(7, 0.6 * len(settings) + 1.6), facecolor=SURFACE)
    _style(ax, "Test B: run time by setting (bar: median, dots: the 3 runs)")
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    labels = []
    for y, row in enumerate(settings):
        copied = row["same_runs_as"]
        labels.append(row["cores"] + (f"\n(same runs as {copied})" if copied else ""))
        ax.barh(y, row["median_s"] / 60, height=0.55, color=BLUE,
                hatch="///" if copied else None, edgecolor=SURFACE)  # fmt: skip
        runs = groups[(row["test"], row["cores"], row["fraction"])]
        ax.scatter([float(r["total_ms"]) / 60000 for r in runs], [y] * len(runs), s=28,
                   color=INK, edgecolor=SURFACE, linewidth=1, zorder=3)  # fmt: skip
        ax.annotate(f"{row['median_s'] / 60:.1f} min", (row["max_s"] / 60, y),
                    textcoords="offset points", xytext=(8, -3), color=INK, fontsize=9)  # fmt: skip
    ax.set_yticks(range(len(settings)), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(r["max_s"] for r in settings) / 60 * 1.2)
    ax.set_xlabel("run time (minutes)", color=MUTED)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def plot_data_size(summary: Sequence[dict], path: str | Path) -> None:
    """Median run time of test C against the data fraction, next to linear growth."""
    plt = _pyplot()
    points = sorted((r["fraction"], r["median_s"] / 60) for r in summary if r["test"] == "C")
    if not points:
        return
    fractions, minutes = zip(*points, strict=True)
    fig, ax = plt.subplots(figsize=(6, 4.2), facecolor=SURFACE)
    _style(ax, "Test C: run time against data size (median of the runs)")
    ax.grid(color=GRID, linewidth=0.8)
    ax.plot(fractions, [minutes[0] * f / fractions[0] for f in fractions], color=MUTED,
            linewidth=1, linestyle=":", label="linear in data size")  # fmt: skip
    ax.plot(fractions, minutes, color=BLUE, linewidth=2, marker="o", markersize=8,
            markeredgecolor=SURFACE, label="measured")  # fmt: skip
    ax.set_xlabel("fraction of the python subset", color=MUTED)
    ax.set_ylabel("run time (minutes)", color=MUTED)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--runs", default="results/e8_scaling/runs.csv", help="tests A and B")
    parser.add_argument(
        "--data-runs",
        default="results/e8_scaling/runs_data_scaling.csv",
        help="test C; skipped if the file does not exist",
    )
    parser.add_argument("--out", default="results/e8_scaling/analysis", help="output directory")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rows = load_runs(args.runs)
    if Path(args.data_runs).exists():
        rows = rows + load_runs(args.data_runs)
    else:
        print(f"{args.data_runs} does not exist yet: test C is skipped")
    summary = summarise(rows)
    write_summary(summary, out / "summary.csv")
    medians = stage_medians(rows)
    write_stage_table(medians, out / "stages.csv")

    scaling = core_scaling(summary)
    plot_speedup(scaling, out / "speedup_cores.png")
    plot_machines(summary, rows, out / "second_machine.png")
    plot_data_size(summary, out / "data_size.png")

    print(f"serial share (Amdahl fit, test A): {scaling['serial_share']:.3f}")
    for c, t, s, e, f in zip(
        scaling["cores"], scaling["times"], scaling["speedup"], scaling["efficiency"],
        scaling["fitted_speedup"], strict=True,
    ):  # fmt: skip
        print(f"  {c} cores: {t:.1f} s, speedup {s:.2f}, efficiency {e:.0%}, model {f:.2f}")
    print_stage_findings(medians, summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
