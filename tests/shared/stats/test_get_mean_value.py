import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_mean_value import GetMeanValue
from tests.unittest_config import UnitTestConfig

class TestGetMeanValue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_mean_value_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        mean_val = 0.55
        print(GetMeanValue(df, ['id']).run().values())
        is_mean_value = mean_val in GetMeanValue(df, ['id']).run().values()
        self.assertEqual(True, is_mean_value)