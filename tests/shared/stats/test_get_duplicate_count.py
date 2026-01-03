import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_duplicate_count import GetDuplicateCount
from tests.unittest_config import UnitTestConfig


class TestGetDuplicateCount(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_duplicate_count_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check02_fail/'
        )
        results = GetDuplicateCount(df, ['txt']).run()
        self.assertEqual(4, results["txt"])
