#!/usr/bin/env python3
"""Pure helpers and configuration rendering for the Laptop A cluster."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from string import Template
from typing import Mapping

HADOOP_TEMPLATES = ("core-site.xml", "hdfs-site.xml")
SPARK_TEMPLATES = ("spark-defaults.conf", "spark-env.sh")


def render_conf(template: str, values: Mapping[str, str]) -> str:
    """Render a configuration template with provided variable substitutions.

    Args:
        template: Template string containing ${VAR} placeholders.
        values: Mapping of placeholder names to replacement string values.

    Returns:
        Rendered template string with all placeholders substituted.

    Raises:
        KeyError: If any required placeholder is not present in values.
    """
    return Template(template).substitute(values)


def parse_hdfs_report(report: str) -> int:
    """Extract the count of live DataNodes from an HDFS dfsadmin report.

    Args:
        report: Raw text output from `hdfs dfsadmin -report`.

    Returns:
        Integer number of live DataNodes currently connected to the NameNode.

    Raises:
        ValueError: If the report text does not match the expected pattern.
    """
    match = re.search(r"^\s*Live datanodes\s*\((\d+)\)\s*:", report, re.MULTILINE)
    if match is None:
        raise ValueError("HDFS report does not contain a Live datanodes count")
    return int(match.group(1))


def render_cluster_configs(
    template_dir: Path,
    hadoop_conf_dir: Path,
    spark_conf_dir: Path,
    values: Mapping[str, str],
) -> list[Path]:
    """Render cluster configuration files from templates into their target directories.

    All templates are validated and rendered in memory before any files are written,
    ensuring an all-or-nothing generation without leaving partial configs.

    Args:
        template_dir: Directory containing .template configuration files.
        hadoop_conf_dir: Destination directory for Hadoop XML configs.
        spark_conf_dir: Destination directory for Spark conf files.
        values: Mapping of variable names to their runtime values.

    Returns:
        List of Paths of written configuration files.

    Raises:
        KeyError: If any template contains a variable missing from values.
    """
    rendered: dict[Path, str] = {}
    for template_name in HADOOP_TEMPLATES:
        source = template_dir / f"{template_name}.template"
        target = hadoop_conf_dir / template_name
        rendered[target] = render_conf(source.read_text(encoding="utf-8"), values)
    for template_name in SPARK_TEMPLATES:
        source = template_dir / f"{template_name}.template"
        target = spark_conf_dir / template_name
        rendered[target] = render_conf(source.read_text(encoding="utf-8"), values)

    hadoop_conf_dir.mkdir(parents=True, exist_ok=True)
    spark_conf_dir.mkdir(parents=True, exist_ok=True)
    for target, content in rendered.items():
        target.write_text(content, encoding="utf-8")
    return list(rendered)


def _render_config_command(args: argparse.Namespace) -> int:
    data_dir = args.hdfs_data_dir.rstrip("/")
    values = {
        "MASTER_IP": args.master_ip,
        "HDFS_DATA_DIR": data_dir,
        "REPLICATION": str(args.replication),
    }
    if not data_dir.startswith("/") or data_dir == "/mnt/c" or data_dir.startswith("/mnt/c/"):
        raise ValueError("HDFS_DATA_DIR must be an absolute path on the WSL2 Linux filesystem")
    if args.replication < 1:
        raise ValueError("replication must be greater than zero")

    outputs = render_cluster_configs(
        template_dir=args.template_dir,
        hadoop_conf_dir=args.hadoop_conf_dir,
        spark_conf_dir=args.spark_conf_dir,
        values=values,
    )
    for output in outputs:
        print(f"Rendered {output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    render_parser = commands.add_parser("render-config", help="render Hadoop and Spark configs")
    render_parser.add_argument("--template-dir", type=Path, required=True)
    render_parser.add_argument("--hadoop-conf-dir", type=Path, required=True)
    render_parser.add_argument("--spark-conf-dir", type=Path, required=True)
    render_parser.add_argument("--master-ip", required=True)
    render_parser.add_argument("--hdfs-data-dir", required=True)
    render_parser.add_argument("--replication", type=int, default=2)

    report_parser = commands.add_parser("parse-report", help="parse an HDFS dfsadmin report")
    report_parser.set_defaults(command_handler=lambda _: _parse_report_stdin())
    render_parser.set_defaults(command_handler=_render_config_command)

    args = parser.parse_args(argv)
    try:
        return args.command_handler(args)
    except (OSError, ValueError, KeyError) as error:
        print(f"Cluster helper failed: {error}", file=sys.stderr)
        return 1


def _parse_report_stdin() -> int:
    try:
        count = parse_hdfs_report(sys.stdin.read())
    except ValueError as error:
        print(f"Cluster helper failed: {error}", file=sys.stderr)
        return 1
    print(count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
