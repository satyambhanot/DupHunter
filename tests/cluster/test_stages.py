"""Tests for core pipeline stage planning, deterministic sampling, and runner."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

from scripts.cluster.stages import (
    STAGES,
    in_sample,
    load_config,
    plan_stages,
    run_pipeline,
    sample_posts,
    stage_paths,
)


class StagePlanningTests(unittest.TestCase):
    """Unit tests for stage ordering and configuration."""

    def test_plan_stages_default_returns_all_in_order(self) -> None:
        self.assertEqual(plan_stages(), ["shingles", "minhash", "lsh", "evaluate"])

    def test_plan_stages_after_shingles(self) -> None:
        self.assertEqual(plan_stages(done={"shingles"}), ["minhash", "lsh", "evaluate"])

    def test_plan_stages_all_done_returns_empty(self) -> None:
        self.assertEqual(plan_stages(done=STAGES), [])

    def test_plan_stages_rejects_unknown_stage(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown stage"):
            plan_stages(done={"shingles", "unknown_stage"})

    def test_full_run_config_contains_required_settings(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        config = load_config(repo_root / "configs" / "full_run.json")
        self.assertEqual(config["fractions"], [0.1, 1.0])
        setting_names = {entry["name"] for entry in config["lsh"]["settings"]}
        self.assertEqual(setting_names, {"default", "chosen"})
        self.assertIn("seed", config)

    def test_stage_paths_separates_fractions_and_settings(self) -> None:
        p10 = stage_paths("hdfs:///out", 0.1, "default")
        p100 = stage_paths("hdfs:///out", 1.0, "chosen")
        self.assertEqual(p10["shingles"], "hdfs:///out/10pct/shingles")
        self.assertEqual(p10["lsh"], "hdfs:///out/10pct/candidates/default")
        self.assertEqual(p100["lsh"], "hdfs:///out/100pct/candidates/chosen")


class SamplingTests(unittest.TestCase):
    """Unit tests for deterministic subset sampling."""

    def test_in_sample_ten_percent_is_subset_of_twenty_five_percent(self) -> None:
        post_ids = range(1, 2000)
        s10 = {pid for pid in post_ids if in_sample(pid, 0.10, seed=42)}
        s25 = {pid for pid in post_ids if in_sample(pid, 0.25, seed=42)}
        s100 = {pid for pid in post_ids if in_sample(pid, 1.0, seed=42)}
        self.assertTrue(s10.issubset(s25))
        self.assertTrue(s25.issubset(s100))
        self.assertEqual(len(s100), len(list(post_ids)))

    def test_in_sample_rejects_invalid_fraction(self) -> None:
        for bad in (0.0, -0.1, 1.1):
            with self.assertRaises(ValueError):
                in_sample(1, bad)


class SparkPipelineTests(unittest.TestCase):
    """Spark-backed tests for deterministic sampling and end-to-end stage execution."""

    spark: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("duphunter-test-stages")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_spark_sample_posts_ten_percent_is_subset_of_twenty_five_percent(self) -> None:
        df = self.spark.createDataFrame([(i,) for i in range(1, 501)], ["post_id"])
        ids_10 = {r["post_id"] for r in sample_posts(df, 0.10, seed=42).collect()}
        ids_25 = {r["post_id"] for r in sample_posts(df, 0.25, seed=42).collect()}
        ids_25_repeat = {r["post_id"] for r in sample_posts(df, 0.25, seed=42).collect()}
        ids_100 = {r["post_id"] for r in sample_posts(df, 1.0, seed=42).collect()}

        self.assertTrue(ids_10.issubset(ids_25))
        self.assertEqual(ids_25, ids_25_repeat)
        self.assertEqual(len(ids_100), 500)

    def test_run_pipeline_executes_stages_and_resumes_from_done(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        config = load_config(repo_root / "configs" / "full_run.json")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            posts_path = str(root / "posts_text")
            meta_path = str(root / "posts_meta")
            answer_key_path = str(root / "answer_key")
            output_root = str(root / "out")
            metrics_csv = str(root / "metrics.csv")

            posts = [
                (1, "python list comprehension syntax", "<p>body</p>"),
                (2, "python list comprehension syntax", "<p>dup</p>"),
                (3, "pandas dataframe merge on key", "<p>body</p>"),
                (4, "django orm filter by date", "<p>body</p>"),
            ]
            meta = [
                (1, "2024-01-01T00:00:00Z"),
                (2, "2024-01-02T00:00:00Z"),
                (3, "2024-01-03T00:00:00Z"),
                (4, "2024-01-04T00:00:00Z"),
            ]
            store: dict[str, tuple[list[tuple], object]] = {}

            def fake_write(df, path: str) -> None:
                store[path] = (df.collect(), df.schema)

            def fake_read(spark: SparkSession, path: str):
                rows, schema = store[path]
                return spark.createDataFrame(rows, schema)

            fake_write(
                self.spark.createDataFrame(posts, ["post_id", "title", "body"]),
                posts_path,
            )
            fake_write(
                self.spark.createDataFrame(meta, ["post_id", "created"]),
                meta_path,
            )
            fake_write(
                self.spark.createDataFrame(
                    [(2, 1, "2024-01-02T00:00:00Z")],
                    ["dup_id", "original_id", "created"],
                ),
                answer_key_path,
            )

            with (
                patch("scripts.cluster.stages._read_parquet", side_effect=fake_read),
                patch("scripts.cluster.stages._write_parquet", side_effect=fake_write),
            ):
                summaries = run_pipeline(
                    self.spark,
                    config,
                    fractions=[1.0],
                    setting_names=["default", "chosen"],
                    posts_text_path=posts_path,
                    posts_meta_path=meta_path,
                    answer_key_path=answer_key_path,
                    output_root=output_root,
                    metrics_out=metrics_csv,
                )
                self.assertEqual(len(summaries), 2)
                self.assertIn(f"{output_root}/100pct/candidates/default", store)
                self.assertIn(f"{output_root}/100pct/candidates/chosen", store)

                with Path(metrics_csv).open(newline="", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 2)

                # Re-run skipping shingles and minhash to verify resume path works cleanly.
                resumed = run_pipeline(
                    self.spark,
                    config,
                    fractions=[1.0],
                    setting_names=["chosen"],
                    done={"shingles", "minhash"},
                    posts_text_path=posts_path,
                    posts_meta_path=meta_path,
                    answer_key_path=answer_key_path,
                    output_root=output_root,
                    metrics_out=metrics_csv,
                )
                self.assertEqual(len(resumed), 1)


if __name__ == "__main__":
    unittest.main()
