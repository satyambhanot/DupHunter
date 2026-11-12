"""One place to create the Spark session used by the profile, answer-key and export scripts."""

from pyspark.sql import SparkSession


def get_spark(master: str = "local[*]", app_name: str = "duphunter-etl") -> SparkSession:
    """Return a Spark session on the given master (``local[*]`` or ``spark://host:7077``)."""
    return SparkSession.builder.master(master).appName(app_name).getOrCreate()
