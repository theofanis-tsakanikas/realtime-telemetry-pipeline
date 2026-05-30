import os
import sys

import pytest
from pyspark.sql import SparkSession

# Point Spark worker processes at the same Python interpreter as the driver.
# Without this, workers default to the system Python (which may be a different
# version) and Spark refuses to run with mismatched minor versions.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("test-sensor-pipeline")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
