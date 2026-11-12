"""Run the scaling tests of E8: ``python -m src.scaling.run_scaling --test A --master spark://...``.

Without ``--cores`` and ``--fraction`` the whole plan of the test is run, three times per
setting. Use ``--dry-run`` to print the commands without starting Spark. WS4 runs this on the
cluster (issue #32).
"""

import argparse
import shlex
import subprocess
import sys

from src.scaling import commands


def commands_for(args: argparse.Namespace) -> list[list[str]]:
    """The commands to run for the given arguments, in order."""
    if args.cores:
        settings = [(args.cores, args.fraction or 0.25)]
    else:
        settings = commands.PLAN[args.test]
        if args.fraction:
            settings = [(cores, args.fraction) for cores, _ in settings]
    return [
        commands.build_command(
            args.test, cores, fraction, run, args.master, args.app, args.app_arg, args.event_log_dir
        )
        for cores, fraction in settings
        for run in range(1, args.runs + 1)
    ]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--test", required=True, choices=commands.TESTS, help="A, B or C")
    parser.add_argument("--cores", help="one setting: 4, A:8, 4+4 or 8+8 (default: the whole plan)")
    parser.add_argument("--fraction", type=float, help="data fraction (default: from the plan)")
    parser.add_argument(
        "--runs", type=int, default=commands.RUNS_PER_SETTING, help="runs per setting"
    )
    parser.add_argument("--master", default="spark://localhost:7077", help="Spark master URL")
    parser.add_argument(
        "--app", default=commands.DEFAULT_APP, help="application that accepts --fraction"
    )
    parser.add_argument(
        "--app-arg", action="append", default=[], help="extra argument for the application"
    )
    parser.add_argument("--event-log-dir", default=commands.DEFAULT_EVENT_LOG_DIR)
    parser.add_argument("--dry-run", action="store_true", help="print the commands and exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print or run the commands; stop at the first failure."""
    args = parse_args(argv)
    if args.runs < 1:
        raise SystemExit("--runs must be at least 1")
    for command in commands_for(args):
        print(shlex.join(command))
        if not args.dry_run:
            subprocess.run(command, check=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
