from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from src.spark_session import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # one spark for the whole test run. starting the jvm takes a few seconds,
    # doing it per test would make the suite crawl.
    s = get_spark("events-tests")
    yield s
    s.stop()
