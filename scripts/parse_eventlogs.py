#!/usr/bin/env python3
"""Turn Spark event logs of the E8 runs into one CSV row per run.

Usage (from the repository root)::

    python scripts/parse_eventlogs.py --logs event-logs/ --out results/e8_scaling/runs.csv

``--logs`` takes event-log files or directories (copy them out of HDFS first: ``hdfs dfs -get
/spark-logs event-logs``). The parsing lives in ``src/scaling/eventlogs.py``.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.scaling import eventlogs  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--logs", nargs="+", required=True, help="event-log files or directories")
    parser.add_argument("--out", required=True, help="CSV file to write")
    args = parser.parse_args(argv)
    rows = eventlogs.collect_runs(args.logs)
    eventlogs.write_runs(rows, args.out)
    print(f"{len(rows)} runs written to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
