import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_median_value import GetMedianValue
from tests.unittest_config import UnitTestConfig

class TestGetMedianValue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_median_value_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        median_val = 1
        print(GetMedianValue(df, ['id']).run().values())
        is_median_value = median_val in GetMedianValue(df, ['id']).run().values()
        self.assertEqual(True, is_median_value)
