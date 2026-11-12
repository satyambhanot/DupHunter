#!/usr/bin/env python3
"""Verify the Stack Overflow dump and build/run its full-scale ETL commands."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path

EXPECTED_ARCHIVE_SIZE = 68_929_051_847
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_MASTER = "spark://localhost:7077"
DEFAULT_OUTPUT = "hdfs:///data/stackoverflow"


def verify_archive(
    archive_path: str | Path,
    expected_size: int = EXPECTED_ARCHIVE_SIZE,
) -> int:
    """Verify that the raw archive file matches the expected byte size.

    Args:
        archive_path: Path to the archive file on disk.
        expected_size: Expected size in bytes (defaults to 68,929,051,847).

    Returns:
        Actual file size in bytes.

    Raises:
        FileNotFoundError: If the archive path does not exist.
        ValueError: If expected_size is negative or size does not match expected_size.
    """
    if expected_size < 0:
        raise ValueError("expected_size must not be negative")

    path = Path(archive_path)
    actual_size = path.stat().st_size
    if actual_size != expected_size:
        raise ValueError(
            f"Archive size mismatch for {path}: expected {expected_size:,} bytes, "
            f"found {actual_size:,} bytes"
        )
    return actual_size


def build_etl_command(
    script_path: str | Path,
    archive_path: str | Path,
    output_path: str,
    master: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    tag_filter: str | None = None,
) -> list[str]:
    """Construct the spark-submit CLI argument list for an ETL stage.

    Args:
        script_path: Path to the Python ETL script (parse_posts.py or parse_links.py).
        archive_path: Path to the source .7z archive file.
        output_path: Target directory (local or HDFS URI).
        master: Spark master URL (e.g., 'spark://host:7077').
        batch_size: Number of records to stream per batch.
        tag_filter: Optional tag filter (e.g., 'python' for posts).

    Returns:
        List of command-line arguments suitable for subprocess.run.

    Raises:
        ValueError: If arguments are empty or invalid.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")
    if not master:
        raise ValueError("master must not be empty")
    if not output_path:
        raise ValueError("output_path must not be empty")

    command = [
        "spark-submit",
        "--master",
        master,
        str(script_path),
        "--archive",
        str(archive_path),
        "--output",
        output_path,
        "--site",
        "stackoverflow",
        "--batch-size",
        str(batch_size),
    ]
    if tag_filter:
        command.extend(["--tag-filter", tag_filter])
    return command


def run_etl(
    project_root: Path,
    archive_path: Path,
    output_path: str,
    master: str,
    batch_size: int,
    dry_run: bool,
) -> None:
    """Execute or preview the complete posts and links ETL pipeline.

    Args:
        project_root: Path to the repository root directory.
        archive_path: Path to the 7z archive.
        output_path: HDFS destination URI.
        master: Spark master connection string.
        batch_size: Number of rows per streaming batch.
        dry_run: If True, print commands without executing them.

    Raises:
        FileNotFoundError: If archive or required ETL scripts are missing.
        subprocess.CalledProcessError: If an ETL stage execution fails.
    """
    posts_script = project_root / "src" / "etl" / "parse_posts.py"
    links_script = project_root / "src" / "etl" / "parse_links.py"

    commands = [
        build_etl_command(
            posts_script,
            archive_path,
            output_path,
            master,
            batch_size,
            tag_filter="python",
        ),
        build_etl_command(
            links_script,
            archive_path,
            output_path,
            master,
            batch_size,
        ),
    ]

    if dry_run:
        for command in commands:
            print(shlex.join(command))
        return

    archive_size = verify_archive(archive_path)
    print(f"Verified {archive_path}: {archive_size:,} bytes")
    missing_scripts = [script for script in (posts_script, links_script) if not script.is_file()]
    if missing_scripts:
        names = ", ".join(str(script) for script in missing_scripts)
        raise FileNotFoundError(f"Required ETL script(s) are missing: {names}")

    for command in commands:
        started = time.perf_counter()
        subprocess.run(command, check=True)
        elapsed = time.perf_counter() - started
        print(f"Completed {Path(command[3]).name} in {elapsed:.2f} seconds")

    subprocess.run(["hdfs", "dfs", "-du", "-s", output_path], check=True)


def main(argv: list[str] | None = None) -> int:
    """Parse CLI options and run or preview the archive ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="Verify and run the full Stack Overflow ETL on the Spark cluster."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="repository root (defaults to the repository containing this script)",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path("data/raw/stackoverflow.com.7z"),
        help="path to the downloaded Stack Overflow archive",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="HDFS output root")
    parser.add_argument("--master", default=DEFAULT_MASTER, help="Spark master URL")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the ETL commands without checking the archive or running Spark",
    )
    args = parser.parse_args(argv)

    project_root = args.project_root.resolve()
    archive_path = args.archive if args.archive.is_absolute() else project_root / args.archive
    if not args.dry_run and str(project_root).replace("\\", "/").startswith("/mnt/c/"):
        parser.error("run this ETL from the WSL2 filesystem, not under /mnt/c")

    run_etl(
        project_root=project_root,
        archive_path=archive_path,
        output_path=args.output,
        master=args.master,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"ETL failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
