"""Build the ``spark-submit`` commands of the scaling experiment E8 (tests A, B and C).

Test A: one executor with 1, 2, 4 or 8 cores, on Laptop B alone (its eight cores are identical).
Test B: one laptop alone (``A:8`` or ``B:8``), then both laptops: ``4+4`` and ``8+8``.
Test C: 10%, 25%, 50% and 100% of the python subset on both laptops (``8+8``).

Which laptop runs a single-laptop setting is decided by the master URL: use the master of a cluster
that has only that laptop's worker. The application must accept ``--fraction`` (share of the data).
"""

import re
from collections.abc import Sequence

TESTS = ("A", "B", "C")
DEFAULT_EVENT_LOG_DIR = "hdfs:///spark-logs"
DEFAULT_APP = "scripts/cluster/core_job.py"
RUNS_PER_SETTING = 3

# The settings of each test, as (cores, fraction) pairs.
PLAN: dict[str, list[tuple[str, float]]] = {
    "A": [("1", 0.25), ("2", 0.25), ("4", 0.25), ("8", 0.25)],
    "B": [("A:8", 0.25), ("B:8", 0.25), ("4+4", 0.25), ("8+8", 0.25)],
    "C": [("8+8", 0.1), ("8+8", 0.25), ("8+8", 0.5), ("8+8", 1.0)],
}

_CORES = re.compile(r"^(?:[AB]:)?(\d+)(?:\+(\d+))?$")


def parse_cores(spec: str) -> tuple[int, int]:
    """Turn a cores setting into ``(executor_cores, total_executor_cores)``.

    ``"4"`` is one executor with 4 cores, ``"A:8"`` is the same on Laptop A, ``"4+4"`` is two
    executors with 4 cores each (8 in total) and ``"8+8"`` is two executors with 8 cores each.
    """
    match = _CORES.match(spec)
    if not match:
        raise ValueError(f"cores must look like 4, A:8, 4+4 or 8+8, got {spec!r}")
    first = int(match.group(1))
    second = match.group(2)
    if first < 1:
        raise ValueError("cores must be at least 1")
    if second is None:
        return first, first
    if int(second) != first:
        raise ValueError(f"both executors must have the same cores, got {spec!r}")
    return first, first + int(second)


def format_fraction(fraction: float) -> str:
    """Format a data fraction for names and arguments (``0.25``, ``1``)."""
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    return f"{fraction:g}"


def log_name(test: str, cores: str, fraction: float, run: int) -> str:
    """Name of one run, used as the Spark application name: ``e8_A_2_0.25_run3``."""
    if test not in TESTS:
        raise ValueError(f"test must be one of {TESTS}, got {test!r}")
    parse_cores(cores)
    return f"e8_{test}_{cores.replace(':', '-')}_{format_fraction(fraction)}_run{run}"


def build_command(
    test: str,
    cores: str,
    fraction: float,
    run: int = 1,
    master: str = "spark://localhost:7077",
    app: str = DEFAULT_APP,
    app_args: Sequence[str] = (),
    event_log_dir: str = DEFAULT_EVENT_LOG_DIR,
) -> list[str]:
    """Build one ``spark-submit`` command; nothing is executed.

    Each run is a fresh application, so Spark is restarted between runs and caching cannot
    help later ones. The event log goes to ``event_log_dir`` and is parsed by
    ``scripts/parse_eventlogs.py``.
    """
    executor_cores, total_cores = parse_cores(cores)
    name = log_name(test, cores, fraction, run)
    return [
        "spark-submit",
        "--master", master,
        "--name", name,
        "--executor-cores", str(executor_cores),
        "--total-executor-cores", str(total_cores),
        "--conf", "spark.eventLog.enabled=true",
        "--conf", f"spark.eventLog.dir={event_log_dir}",
        app,
        "--fraction", format_fraction(fraction),
        *app_args,
    ]  # fmt: skip
