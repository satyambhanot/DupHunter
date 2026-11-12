"""Unit tests for archive verification and ETL command generation (Issue #3)."""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.cluster.verify_archive import (
    EXPECTED_ARCHIVE_SIZE,
    build_etl_command,
    run_etl,
    verify_archive,
)


class VerifyArchiveTests(unittest.TestCase):
    """Unit tests for verifying raw archive integrity and Spark command building."""

    def test_expected_archive_size_matches_issue(self) -> None:
        self.assertEqual(EXPECTED_ARCHIVE_SIZE, 68_929_051_847)

    def test_accepts_exact_size(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "dump.7z"
            archive.write_bytes(b"12345")
            self.assertEqual(verify_archive(archive, expected_size=5), 5)

    def test_rejects_size_mismatch_with_actual_and_expected_sizes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "dump.7z"
            archive.write_bytes(b"12345")
            with self.assertRaisesRegex(ValueError, "expected 4 bytes, found 5 bytes"):
                verify_archive(archive, expected_size=4)

    def test_missing_archive_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                verify_archive(Path(directory) / "missing.7z")

    def test_builds_posts_command_with_python_filter(self) -> None:
        command = build_etl_command(
            "src/etl/parse_posts.py",
            "data/raw/stackoverflow.com.7z",
            "hdfs:///data/stackoverflow",
            "spark://10.0.0.1:7077",
            batch_size=10_000,
            tag_filter="python",
        )
        self.assertEqual(
            command,
            [
                "spark-submit",
                "--master",
                "spark://10.0.0.1:7077",
                "src/etl/parse_posts.py",
                "--archive",
                "data/raw/stackoverflow.com.7z",
                "--output",
                "hdfs:///data/stackoverflow",
                "--site",
                "stackoverflow",
                "--batch-size",
                "10000",
                "--tag-filter",
                "python",
            ],
        )

    def test_builds_links_command_without_tag_filter(self) -> None:
        command = build_etl_command(
            "src/etl/parse_links.py",
            "dump.7z",
            "hdfs:///data/stackoverflow",
            "local[*]",
        )
        self.assertNotIn("--tag-filter", command)

    def test_rejects_invalid_batch_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "batch_size"):
            build_etl_command("etl.py", "dump.7z", "output", "local[*]", batch_size=0)

    def test_dry_run_does_not_require_archive_or_etl_scripts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                run_etl(
                    project_root=root,
                    archive_path=root / "missing_archive.7z",
                    output_path="hdfs:///data/stackoverflow",
                    master="spark://10.0.0.1:7077",
                    batch_size=20_000,
                    dry_run=True,
                )

        commands = output.getvalue().splitlines()
        self.assertEqual(len(commands), 2)
        self.assertIn("parse_posts.py", commands[0])
        self.assertIn("--tag-filter python", commands[0])
        self.assertIn("parse_links.py", commands[1])
        self.assertNotIn("--tag-filter", commands[1])
        self.assertIn("hdfs:///data/stackoverflow", commands[0])


if __name__ == "__main__":
    unittest.main()
