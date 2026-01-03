import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_row_count import GetRowCount
from tests.unittest_config import UnitTestConfig


class TestGetRowCount(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_row_count_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        expected_row_count = 100
        calc_row_count = GetRowCount(df).run()
        self.assertEqual(calc_row_count, expected_row_count)
