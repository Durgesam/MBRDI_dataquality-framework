import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_min_value import GetMinValue
from tests.unittest_config import UnitTestConfig

class TestGetMinValue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_min_value_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_min_max_data/check_min_max'
        )
        min_int = 4
        is_min_value = min_int in GetMinValue(df, ['Column_int']).run().values()
        self.assertEqual(True, is_min_value)