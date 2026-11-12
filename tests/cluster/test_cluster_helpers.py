"""Unit tests for Laptop A cluster configuration rendering and HDFS status parsing."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.cluster.cluster_helpers import (
    parse_hdfs_report,
    render_cluster_configs,
    render_conf,
)


class ClusterHelperTests(unittest.TestCase):
    """Unit tests for configuration rendering and HDFS status parsing."""

    def test_render_conf_substitutes_values(self) -> None:
        self.assertEqual(
            render_conf("fs=hdfs://${MASTER_IP}:9000", {"MASTER_IP": "10.0.0.5"}),
            "fs=hdfs://10.0.0.5:9000",
        )

    def test_render_conf_raises_for_missing_value(self) -> None:
        with self.assertRaises(KeyError):
            render_conf("fs=hdfs://${MASTER_IP}:9000", {})

    def test_parse_hdfs_report_returns_live_datanode_count(self) -> None:
        report = """Configured Capacity: 1000 (1000 B)
Live datanodes (2):
Name: 10.0.0.1:9866
"""
        self.assertEqual(parse_hdfs_report(report), 2)

    def test_parse_hdfs_report_rejects_missing_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "Live datanodes"):
            parse_hdfs_report("Configured Capacity: 1000 (1000 B)")

    def test_render_cluster_configs_is_all_or_nothing_for_missing_values(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            hadoop_dir = root / "hadoop-conf"
            spark_dir = root / "spark-conf"

            with self.assertRaises(KeyError):
                render_cluster_configs(
                    repo_root / "configs" / "cluster",
                    hadoop_dir,
                    spark_dir,
                    {"MASTER_IP": "10.0.0.5"},
                )

            self.assertFalse(hadoop_dir.exists())
            self.assertFalse(spark_dir.exists())

    def test_render_cluster_configs_renders_hadoop_and_spark_files(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outputs = render_cluster_configs(
                repo_root / "configs" / "cluster",
                root / "hadoop-conf",
                root / "spark-conf",
                {
                    "MASTER_IP": "10.0.0.5",
                    "HDFS_DATA_DIR": "/home/team/hadoop-data",
                    "REPLICATION": "2",
                },
            )

            self.assertEqual(len(outputs), 4)
            self.assertIn(
                "hdfs://10.0.0.5:9000",
                (root / "hadoop-conf" / "core-site.xml").read_text(encoding="utf-8"),
            )
            spark_defaults = (root / "spark-conf" / "spark-defaults.conf").read_text(
                encoding="utf-8"
            )
            self.assertIn("spark.worker.cores 8", spark_defaults)
            self.assertIn("spark.worker.memory 8g", spark_defaults)


if __name__ == "__main__":
    unittest.main()
