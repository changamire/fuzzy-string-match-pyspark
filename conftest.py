""" pytest fixtures that can be resued across tests. the filename needs to be conftest.py
"""

# make sure env variables are set correctly
import findspark  # this needs to be the first import
findspark.init('/opt/spark')
import logging
import pytest
import os
import inspect

from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """

    currentdir = os.path.dirname(os.path.abspath(
        inspect.getfile(inspect.currentframe())))
    parentdir = os.path.dirname(currentdir)

    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())



    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def hive_context(spark_context):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        HiveContext for tests
    """
    return HiveContext(spark_context)


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)