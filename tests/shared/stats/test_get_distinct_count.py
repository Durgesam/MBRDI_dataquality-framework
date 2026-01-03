import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_distinct_count import GetDistinctCount
from tests.unittest_config import UnitTestConfig

class TestGetDistinctCount(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_distinct_count_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check02_fail/'
        )
        distinct_count = 97
        is_distinct_count = distinct_count in GetDistinctCount(df, ['txt']).run().values()
        self.assertEqual(True, is_distinct_count)