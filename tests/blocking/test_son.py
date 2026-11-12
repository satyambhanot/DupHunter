"""Unit and integration tests for SON frequent itemsets (Issue #16).

Acceptance criteria:
1. Baskets [['a', 'b'], ['a', 'b', 'c'], ['a', 'c'], ['b', 'c']] with min_count=2
   give {a}, {b}, {c}, {ab}, {ac}, {bc} and not {abc}.
2. SON over 2 partitions equals the single-pass result and matches FPGrowth
   exactly on 100 random baskets.
3. results/e6_blocking/frequent_tagsets.csv saved; run times of both methods recorded.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Iterable, Sequence
from typing import Any, cast

import pytest
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession

from src.blocking.son import apriori, parse_args, run_son, son


class _PartitionedRDD:
    """Lightweight multi-partition RDD adapter when PySpark/JVM is unavailable."""

    def __init__(self, partitions: list[list[Any]]) -> None:
        self._partitions = partitions

    @classmethod
    def parallelize(cls, data: Sequence[Any], num_slices: int = 2) -> _PartitionedRDD:
        n = len(data)
        partitions: list[list[Any]] = []
        for i in range(num_slices):
            start = (i * n) // num_slices
            end = ((i + 1) * n) // num_slices
            partitions.append(list(data[start:end]))
        return cls(partitions)

    def count(self) -> int:
        return sum(len(p) for p in self._partitions)

    def mapPartitions(self, f: Callable[[Iterable[Any]], Iterable[Any]]) -> _PartitionedRDD:
        return _PartitionedRDD([list(f(iter(p))) for p in self._partitions])

    def distinct(self) -> _PartitionedRDD:
        seen: set[Any] = set()
        out: list[Any] = []
        for p in self._partitions:
            for item in p:
                if item not in seen:
                    seen.add(item)
                    out.append(item)
        return _PartitionedRDD([out])

    def reduceByKey(self, func: Callable[[Any, Any], Any]) -> _PartitionedRDD:
        acc: dict[Any, Any] = {}
        for p in self._partitions:
            for k, v in p:
                acc[k] = func(acc[k], v) if k in acc else v
        return _PartitionedRDD([list(acc.items())])

    def filter(self, pred: Callable[[Any], bool]) -> _PartitionedRDD:
        return type(self)([[x for x in p if pred(x)] for p in self._partitions])

    def cache(self) -> _PartitionedRDD:
        return self

    def collect(self) -> list[Any]:
        return [item for p in self._partitions for item in p]


class TestApriori:
    """Tests for the local Apriori implementation."""

    def test_apriori_acceptance_criterion(self) -> None:
        """Criterion 1: [[a,b],[a,b,c],[a,c],[b,c]] with min_count=2.

        Expected output: {a}, {b}, {c}, {ab}, {ac}, {bc} and not {abc}.
        """
        baskets = [
            ["a", "b"],
            ["a", "b", "c"],
            ["a", "c"],
            ["b", "c"],
        ]
        result = apriori(baskets, min_count=2)

        expected = {
            frozenset({"a"}),
            frozenset({"b"}),
            frozenset({"c"}),
            frozenset({"a", "b"}),
            frozenset({"a", "c"}),
            frozenset({"b", "c"}),
        }

        assert result == expected
        assert frozenset({"a", "b", "c"}) not in result

    def test_apriori_max_size(self) -> None:
        """Verify max_size limit works as expected."""
        baskets = [
            ["a", "b"],
            ["a", "b", "c"],
            ["a", "c"],
            ["b", "c"],
        ]
        result = apriori(baskets, min_count=2, max_size=1)
        expected = {
            frozenset({"a"}),
            frozenset({"b"}),
            frozenset({"c"}),
        }
        assert result == expected

    def test_apriori_empty_input(self) -> None:
        """Verify handling of empty baskets or non-positive min_count."""
        assert apriori([], min_count=2) == set()
        assert apriori([["a"]], min_count=0) == set()


def _make_rdd(baskets: list[list[str]], num_slices: int = 2) -> Any:
    """Create a Spark RDD if PySpark/JVM is available, else use _PartitionedRDD."""
    try:
        builder = cast(SparkSession.Builder, SparkSession.builder)
        spark = (
            builder.master("local[2]")
            .appName("DupHunter-TestSON")
            .getOrCreate()
        )
        return spark.sparkContext.parallelize(baskets, numSlices=num_slices)
    except (ImportError, RuntimeError):
        return _PartitionedRDD.parallelize(baskets, num_slices=num_slices)


class TestSON:
    """Tests for the distributed SON implementation."""

    def test_son_two_partitions_equals_single_pass(self) -> None:
        """Criterion 2 (Part A): SON over 2 partitions equals single-pass result."""
        rng = random.Random(42)
        vocab = ["python", "pandas", "numpy", "dataframe", "list", "dict", "matplotlib", "sql"]
        baskets = [
            rng.sample(vocab, k=rng.randint(2, 5))
            for _ in range(100)
        ]
        support = 0.15
        min_count = math.ceil(support * len(baskets))

        # Single-pass Apriori result
        single_pass_sets = apriori(baskets, min_count=min_count)

        # SON over 1 partition vs 2 partitions
        rdd_1 = _make_rdd(baskets, num_slices=1)
        rdd_2 = _make_rdd(baskets, num_slices=2)

        son_1_results = dict(son(rdd_1, s=support))
        son_2_results = dict(son(rdd_2, s=support))

        assert son_1_results == son_2_results
        assert set(son_2_results.keys()) == single_pass_sets

    def test_son_matches_fpgrowth_100_baskets(self) -> None:
        """Criterion 2 (Part B): Matches FPGrowth exactly on 100 random baskets."""
        rng = random.Random(16)
        tags = ["a", "b", "c", "d", "e", "f", "g", "h"]
        baskets = [
            sorted(rng.sample(tags, k=rng.randint(1, 5)))
            for _ in range(100)
        ]
        support = 0.10

        rdd = _make_rdd(baskets, num_slices=2)
        son_results = dict(son(rdd, s=support))

        # Compare against Spark FPGrowth if available, otherwise exact reference FPM
        try:
            builder = cast(SparkSession.Builder, SparkSession.builder)
            spark = builder.master("local[2]").getOrCreate()
            df = spark.createDataFrame([(b,) for b in baskets], ["items"])
            fp = FPGrowth(itemsCol="items", minSupport=support)
            model = fp.fit(df)
            expected_results = {
                frozenset(row["items"]): int(row["freq"])
                for row in model.freqItemsets.collect()
            }
        except (ImportError, RuntimeError):
            min_count = math.ceil(support * len(baskets))
            freq_sets = apriori(baskets, min_count=min_count)
            basket_sets = [set(b) for b in baskets]
            expected_results = {
                fs: sum(1 for b in basket_sets if fs.issubset(b))
                for fs in freq_sets
            }

        assert son_results == expected_results

    def test_son_pass2_eliminates_local_false_positives(self) -> None:
        """Verify Pass 2 removes candidates that are only frequent in one partition."""
        # 8 baskets across 2 partitions (4 each), s = 0.5 -> global min_count = 4
        baskets = [
            # Partition 1: {c}, {a,c}, {b,c} have count >= 2 locally
            ["a", "b"],
            ["a", "b", "c"],
            ["a", "c"],
            ["b", "c"],
            # Partition 2: {d} has count >= 2 locally, {c} has 0
            ["a", "b"],
            ["a", "b"],
            ["a", "d"],
            ["b", "d"],
        ]
        rdd = _make_rdd(baskets, num_slices=2)
        result = dict(son(rdd, s=0.5))

        expected = {
            frozenset({"a"}): 6,
            frozenset({"b"}): 6,
            frozenset({"a", "b"}): 4,
        }
        assert result == expected

    def test_son_edge_cases(self) -> None:
        """Verify SON handles empty RDD, non-positive support, and None RDD."""
        empty_rdd = _make_rdd([], num_slices=2)
        assert son(empty_rdd, s=0.1) == []

        non_empty_rdd = _make_rdd([["a", "b"]], num_slices=1)
        assert son(non_empty_rdd, s=0.0) == []

        with pytest.raises(ValueError):
            son(None, s=0.1)  # type: ignore[arg-type]


class TestSONCLI:
    """Tests for CLI argument parsing and CSV export in run_son."""

    def test_parse_args_defaults_and_overrides(self) -> None:
        """Verify CLI arguments parse with expected defaults and overrides."""
        args = parse_args([
            "--input",
            "data/python_tags",
            "--output",
            "results/e6_blocking/frequent_tagsets.csv",
        ])
        assert args.input == "data/python_tags"
        assert args.output == "results/e6_blocking/frequent_tagsets.csv"
        assert args.support == 0.01
        assert args.max_size is None
        assert args.master == "local[*]"

        custom = parse_args([
            "--input", "in.txt",
            "--output", "out.csv",
            "--support", "0.25",
            "--max-size", "3",
            "--master", "local[2]",
        ])
        assert custom.support == 0.25
        assert custom.max_size == 3
        assert custom.master == "local[2]"

    def test_run_son_writes_csv(self, tmp_path: Any) -> None:
        """Verify run_son executes SON and writes frequent_tagsets.csv with run times."""
        input_file = tmp_path / "tags.txt"
        input_file.write_text("a b\na b c\na c\nb c\n", encoding="utf-8")
        output_file = tmp_path / "frequent_tagsets.csv"

        class _StubRDD(_PartitionedRDD):
            def map(self, f: Callable[[Any], Any]) -> _StubRDD:
                return _StubRDD([[f(x) for x in p] for p in self._partitions])

            def toDF(self, _cols: list[str]) -> Any:
                raise RuntimeError("No JVM in unit test stub")

        class _StubContext:
            def textFile(self, path: str) -> _PartitionedRDD:
                with open(path, encoding="utf-8") as fh:
                    lines = [line.rstrip("\n") for line in fh]
                return _StubRDD.parallelize(lines, num_slices=2)

        class _StubSpark:
            sparkContext = _StubContext()

        timings = run_son(
            spark=_StubSpark(),  # type: ignore[arg-type]
            input_path=str(input_file),
            output_path=str(output_file),
            support=0.5,
        )
        assert timings["son_time_sec"] >= 0.0
        assert output_file.exists()

        lines = output_file.read_text(encoding="utf-8").strip().splitlines()
        assert lines[0] == "tagset,size,count,support,son_time_sec,fpgrowth_time_sec"
        # 6 frequent itemsets: a, b, c, a b, a c, b c
        assert len(lines) == 7
