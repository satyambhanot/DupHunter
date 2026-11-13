"""Core pipeline stage planning, deterministic sampling, and cluster runner."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.evaluation.evaluate import append_metrics, evaluate, per_pair_table
from src.similarity.lsh import LSHConfig, find_candidates, resolve_bucket_policy
from src.similarity.minhash import MinHashConfig, build_signatures
from src.similarity.shingles import ShingleConfig, build_shingles

logger = logging.getLogger("duphunter.cluster.stages")

STAGES: tuple[str, ...] = ("shingles", "minhash", "lsh", "evaluate")
SAMPLE_BUCKETS: int = 10_000


def plan_stages(done: Collection[str] = ()) -> list[str]:
    """Return remaining pipeline stages in canonical order.

    Args:
        done: Stage names that have already completed.

    Returns:
        Ordered list of stages remaining to execute.

    Raises:
        ValueError: If ``done`` contains any unknown stage name.
    """
    done_set = set(done)
    unknown = done_set.difference(STAGES)
    if unknown:
        raise ValueError(f"Unknown stage(s): {sorted(unknown)}; expected subset of {STAGES}")
    return [stage for stage in STAGES if stage not in done_set]


def validate_fraction(fraction: float) -> float:
    """Validate that a sampling fraction lies in ``(0, 1]``."""
    if not 0.0 < fraction <= 1.0:
        raise ValueError(f"fraction must be in (0, 1], got {fraction!r}")
    return float(fraction)


def sample_posts(
    posts: DataFrame,
    fraction: float,
    seed: int = 0,
    post_id_col: str = "post_id",
) -> DataFrame:
    """Deterministically sample a prefix fraction of ``posts`` using Spark SQL.

    Because filtering depends only on ``(seed, post_id)``, any smaller fraction
    (such as 0.10) is a strict subset of any larger fraction (such as 0.25).
    """
    validate_fraction(fraction)
    if fraction == 1.0:
        return posts
    if post_id_col not in posts.columns:
        raise ValueError(f"posts DataFrame is missing column {post_id_col!r}")
    threshold = int(round(fraction * SAMPLE_BUCKETS))
    bucket = F.pmod(F.xxhash64(F.lit(seed), F.col(post_id_col)), F.lit(SAMPLE_BUCKETS))
    return posts.filter(bucket < F.lit(threshold))


def format_fraction_tag(fraction: float) -> str:
    """Format a fraction as a compact path component such as ``10pct`` or ``100pct``."""
    validate_fraction(fraction)
    pct = fraction * 100.0
    if abs(pct - round(pct)) < 1e-9:
        return f"{int(round(pct))}pct"
    return f"{fraction:g}"


def stage_paths(output_root: str, fraction: float, setting_name: str = "chosen") -> dict[str, str]:
    """Build deterministic output paths for one ``(fraction, setting_name)`` run."""
    root = output_root.rstrip("/")
    frac_dir = f"{root}/{format_fraction_tag(fraction)}"
    return {
        "shingles": f"{frac_dir}/shingles",
        "minhash": f"{frac_dir}/signatures",
        "lsh": f"{frac_dir}/candidates/{setting_name}",
        "per_pair": f"{frac_dir}/per_pair/{setting_name}",
    }


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a JSON pipeline configuration file."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def _write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def _prepare_text_inputs(posts_text: DataFrame, fields: str) -> DataFrame:
    if fields == "title":
        return posts_text.withColumn("body", F.lit(""))
    if fields == "title+body":
        return posts_text
    raise ValueError(f"Unsupported shingles.fields {fields!r}; expected 'title' or 'title+body'")


def _restrict_answer_key(answer_key: DataFrame, sampled_ids: DataFrame) -> DataFrame:
    """Keep only answer-key pairs whose dup_id and original_id are both in ``sampled_ids``."""
    return answer_key.join(
        sampled_ids.select(F.col("post_id").alias("dup_id")),
        on="dup_id",
        how="left_semi",
    ).join(
        sampled_ids.select(F.col("post_id").alias("original_id")),
        on="original_id",
        how="left_semi",
    )


def _resolve_settings(
    config: dict[str, Any],
    requested_names: Sequence[str] | None,
) -> list[dict[str, Any]]:
    settings: list[dict[str, Any]] = list(config["lsh"]["settings"])
    if not requested_names or "all" in requested_names:
        return settings
    by_name = {entry["name"]: entry for entry in settings}
    unknown = [name for name in requested_names if name not in by_name]
    if unknown:
        raise ValueError(f"Unknown LSH setting(s): {unknown}; available: {list(by_name)}")
    return [by_name[name] for name in requested_names]


def run_pipeline(
    spark: SparkSession,
    config: dict[str, Any],
    *,
    fractions: Sequence[float] | None = None,
    setting_names: Sequence[str] | None = None,
    done: Collection[str] = (),
    posts_text_path: str | None = None,
    posts_meta_path: str | None = None,
    answer_key_path: str | None = None,
    output_root: str | None = None,
    metrics_out: str | None = None,
) -> list[dict[str, Any]]:
    """Execute planned core pipeline stages for each requested fraction and LSH setting."""
    planned = plan_stages(done)
    if not planned:
        logger.info("All stages already marked done; nothing to execute.")
        return []

    hdfs_cfg = config.get("hdfs", {})
    posts_text_uri = posts_text_path or hdfs_cfg["posts_text"]
    posts_meta_uri = posts_meta_path or hdfs_cfg.get("posts_meta")
    answer_key_uri = answer_key_path or hdfs_cfg.get("answer_key")
    out_root = output_root or hdfs_cfg["output_root"]
    metrics_csv = metrics_out or config.get("metrics_out", "results/core_pipeline_metrics.csv")

    seed = int(config.get("seed", 42))
    target_tag = config.get("tag")
    target_fractions = [
        validate_fraction(float(f)) for f in (fractions or config.get("fractions", [0.1, 1.0]))
    ]
    lsh_settings = _resolve_settings(config, setting_names)

    shingle_cfg = config.get("shingles", {})
    shingle_fields = shingle_cfg.get("fields", "title")
    shingle_config = ShingleConfig(
        representation=shingle_cfg.get("representation", "word"),
        k=int(shingle_cfg.get("k", 1)),
        drop_common_share=shingle_cfg.get("drop_common_share"),
    )

    minhash_cfg = config.get("minhash", {})
    minhash_config = MinHashConfig(
        n=int(minhash_cfg.get("n", 128)),
        seed=int(minhash_cfg.get("seed", seed)),
    )

    posts_meta_df = _read_parquet(spark, posts_meta_uri) if posts_meta_uri else None
    answer_key_df = (
        _read_parquet(spark, answer_key_uri) if ("evaluate" in planned and answer_key_uri) else None
    )

    summaries: list[dict[str, Any]] = []
    for fraction in target_fractions:
        shared_paths = stage_paths(out_root, fraction)
        n_questions: int | None = None

        if "shingles" in planned:
            t0 = time.perf_counter()
            posts_df = _read_parquet(spark, posts_text_uri)
            if target_tag and "tags" in posts_df.columns:
                posts_df = posts_df.filter(F.array_contains(F.col("tags"), target_tag))
            posts_df = sample_posts(posts_df, fraction, seed=seed)
            posts_df = _prepare_text_inputs(posts_df, shingle_fields)
            shingled = build_shingles(posts_df, shingle_config)
            _write_parquet(shingled, shared_paths["shingles"])
            logger.info(
                "Stage shingles (fraction=%s) finished in %.2fs -> %s",
                fraction,
                time.perf_counter() - t0,
                shared_paths["shingles"],
            )

        if "minhash" in planned:
            t0 = time.perf_counter()
            shingled_df = _read_parquet(spark, shared_paths["shingles"])
            signatures = build_signatures(shingled_df, minhash_config)
            if posts_meta_df is not None and "created" in posts_meta_df.columns:
                signatures = signatures.join(
                    posts_meta_df.select("post_id", "created"),
                    on="post_id",
                    how="left",
                )
            _write_parquet(signatures, shared_paths["minhash"])
            logger.info(
                "Stage minhash (fraction=%s) finished in %.2fs -> %s",
                fraction,
                time.perf_counter() - t0,
                shared_paths["minhash"],
            )

        if "lsh" not in planned and "evaluate" not in planned:
            continue

        for setting in lsh_settings:
            setting_name = setting["name"]
            paths = stage_paths(out_root, fraction, setting_name)
            n_candidates: int | None = None

            if "lsh" in planned:
                t0 = time.perf_counter()
                sigs_df = _read_parquet(spark, paths["minhash"])
                lsh_cfg = LSHConfig(
                    b=int(setting["b"]),
                    r=int(setting["r"]),
                    n=setting.get("n"),
                    bands_per_pass=config.get("lsh", {}).get("bands_per_pass"),
                )
                policy = resolve_bucket_policy(
                    setting.get("bucket_policy", "size_limit"),
                    int(setting.get("bucket_limit", 1000)),
                )
                candidates = find_candidates(sigs_df, config=lsh_cfg, bucket_policy=policy)
                _write_parquet(candidates, paths["lsh"])
                n_candidates = _read_parquet(spark, paths["lsh"]).count()
                logger.info(
                    "Stage lsh (fraction=%s, setting=%s) finished in %.2fs, candidates=%d -> %s",
                    fraction,
                    setting_name,
                    time.perf_counter() - t0,
                    n_candidates,
                    paths["lsh"],
                )

            if "evaluate" in planned and answer_key_df is not None:
                t0 = time.perf_counter()
                sigs_ids = _read_parquet(spark, paths["minhash"]).select("post_id")
                if n_questions is None:
                    n_questions = sigs_ids.count()
                scoped_key = _restrict_answer_key(answer_key_df, sigs_ids)
                candidates_df = _read_parquet(spark, paths["lsh"])
                metrics = evaluate(candidates_df, scoped_key, n_questions=n_questions)
                run_settings = {
                    "fraction": fraction,
                    "setting": setting_name,
                    "b": int(setting["b"]),
                    "r": int(setting["r"]),
                    "n": int(setting.get("n") or int(setting["b"]) * int(setting["r"])),
                }
                append_metrics(
                    metrics_csv,
                    config.get("experiment", "core_pipeline"),
                    run_settings,
                    metrics,
                )
                table = per_pair_table(candidates_df, scoped_key)
                _write_parquet(table, paths["per_pair"])
                logger.info(
                    "Stage evaluate (fraction=%s, setting=%s) finished in %.2fs -> %s",
                    fraction,
                    setting_name,
                    time.perf_counter() - t0,
                    metrics_csv,
                )

            summaries.append(
                {
                    "fraction": fraction,
                    "setting": setting_name,
                    "candidates_path": paths["lsh"],
                    "n_candidates": n_candidates,
                }
            )

    return summaries


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the core pipeline runner."""
    parser = argparse.ArgumentParser(description="Run DupHunter core pipeline stages.")
    parser.add_argument(
        "--config",
        default="configs/full_run.json",
        help="Path to JSON pipeline config (default: configs/full_run.json)",
    )
    parser.add_argument(
        "--fraction",
        action="append",
        type=float,
        help="Data fraction(s) in (0, 1] to run; repeatable (defaults to config fractions)",
    )
    parser.add_argument(
        "--setting",
        action="append",
        help="LSH setting name(s) from config or 'all'; repeatable",
    )
    parser.add_argument(
        "--done",
        action="append",
        default=[],
        help="Stage(s) already completed and skipped; repeatable",
    )
    parser.add_argument("--posts-text", help="Override input posts_text Parquet path")
    parser.add_argument("--posts-meta", help="Override input posts_meta Parquet path")
    parser.add_argument("--answer-key", help="Override answer_key Parquet path")
    parser.add_argument("--output-root", help="Override output root directory/URI")
    parser.add_argument("--metrics-out", help="Override metrics CSV path")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for running the core pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    config = load_config(args.config)
    spark = (
        SparkSession.builder.master(args.master).appName("duphunter-core-pipeline").getOrCreate()
    )
    try:
        summaries = run_pipeline(
            spark,
            config,
            fractions=args.fraction,
            setting_names=args.setting,
            done=args.done,
            posts_text_path=args.posts_text,
            posts_meta_path=args.posts_meta,
            answer_key_path=args.answer_key,
            output_root=args.output_root,
            metrics_out=args.metrics_out,
        )
        print(json.dumps(summaries, indent=2))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
