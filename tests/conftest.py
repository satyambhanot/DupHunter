"""Fixtures shared by every test module."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session: two cores, two shuffle partitions, no UI, no network."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("duphunter-tests")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
