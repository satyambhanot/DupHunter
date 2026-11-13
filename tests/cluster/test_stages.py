"""Tests for core pipeline stage planning, deterministic sampling, and runner."""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

from scripts.cluster.stages import (
    STAGES,
    _restrict_answer_key,
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

    def test_sample_posts_rejects_invalid_fraction(self) -> None:
        df = self.spark.createDataFrame([(1,)], ["post_id"])
        for bad in (0.0, -0.1, 1.1):
            with self.assertRaises(ValueError):
                sample_posts(df, bad)

    def test_restrict_answer_key_excludes_out_of_sample_pairs(self) -> None:
        sampled = self.spark.createDataFrame([(1,), (2,)], ["post_id"])
        answer_key = self.spark.createDataFrame(
            [
                (2, 1, datetime(2024, 1, 2, tzinfo=UTC)),
                (4, 3, datetime(2024, 1, 4, tzinfo=UTC)),
                (3, 1, datetime(2024, 1, 3, tzinfo=UTC)),
            ],
            ["dup_id", "original_id", "created"],
        )
        restricted = _restrict_answer_key(answer_key, sampled).collect()
        self.assertEqual([(r["dup_id"], r["original_id"]) for r in restricted], [(2, 1)])

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
                (1, datetime(2024, 1, 1, tzinfo=UTC)),
                (2, datetime(2024, 1, 2, tzinfo=UTC)),
                (3, datetime(2024, 1, 3, tzinfo=UTC)),
                (4, datetime(2024, 1, 4, tzinfo=UTC)),
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
            # Include one in-sample pair (2, 1) and one out-of-sample pair (99, 98);
            # evaluate() must restrict the answer key to sampled posts so n_queries == 1.
            fake_write(
                self.spark.createDataFrame(
                    [
                        (2, 1, datetime(2024, 1, 2, tzinfo=UTC)),
                        (99, 98, datetime(2024, 1, 5, tzinfo=UTC)),
                    ],
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
                per_pair_rows, _ = store[f"{output_root}/100pct/per_pair/chosen"]
                self.assertEqual(len(per_pair_rows), 1)

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

    @unittest.skipIf(
        os.name == "nt" and not os.environ.get("HADOOP_HOME"),
        "Spark Parquet writes on Windows require HADOOP_HOME/winutils.exe",
    )
    def test_run_pipeline_real_parquet_round_trip(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        config = load_config(repo_root / "configs" / "full_run.json")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            posts_path = str(root / "posts_text")
            meta_path = str(root / "posts_meta")
            answer_key_path = str(root / "answer_key")
            output_root = str(root / "out")
            metrics_csv = str(root / "metrics.csv")

            self.spark.createDataFrame(
                [
                    (1, "python list comprehension syntax", "<p>body</p>"),
                    (2, "python list comprehension syntax", "<p>dup</p>"),
                ],
                ["post_id", "title", "body"],
            ).write.parquet(posts_path)
            self.spark.createDataFrame(
                [
                    (1, datetime(2024, 1, 1, tzinfo=UTC)),
                    (2, datetime(2024, 1, 2, tzinfo=UTC)),
                ],
                ["post_id", "created"],
            ).write.parquet(meta_path)
            self.spark.createDataFrame(
                [(2, 1, datetime(2024, 1, 2, tzinfo=UTC))],
                ["dup_id", "original_id", "created"],
            ).write.parquet(answer_key_path)

            summaries = run_pipeline(
                self.spark,
                config,
                fractions=[1.0],
                setting_names=["chosen"],
                posts_text_path=posts_path,
                posts_meta_path=meta_path,
                answer_key_path=answer_key_path,
                output_root=output_root,
                metrics_out=metrics_csv,
            )
            self.assertEqual(len(summaries), 1)
            self.assertTrue((Path(output_root) / "100pct" / "candidates" / "chosen").exists())


if __name__ == "__main__":
    unittest.main()
