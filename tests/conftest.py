import os
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession

# Point Spark worker processes at the same Python interpreter as the driver.
# Without this, workers default to the system Python (which may be a different
# version) and Spark refuses to run with mismatched minor versions.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # Keep Spark/Derby scratch (spark-warehouse/, metastore_db/, derby.log) out
    # of the repo by redirecting both the SQL warehouse and Derby's home to a
    # throwaway temp dir for the duration of the test session.
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    derby_home = tempfile.mkdtemp(prefix="derby-")

    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("test-sensor-pipeline")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_home}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
