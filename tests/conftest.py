"""Fixtures shared by every test module."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark():
    """A small local Spark session: two cores, two shuffle partitions, no UI, no network.

    It asks Spark for the session every time and never stops it. Some modules start and stop their
    own session, and a session shared for the whole run would be dead by the time a later test
    uses it; getOrCreate gives a fresh one after a stop and the same one otherwise.
    """
    return (
        SparkSession.builder.master("local[2]")
        .appName("duphunter-tests")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
