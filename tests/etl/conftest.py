"""Shared fixtures for the ETL tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session; no network, two shuffle partitions."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("duphunter-etl-tests")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
