import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_max_value import GetMaxValue
from tests.unittest_config import UnitTestConfig

class TestGetMaxValue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_max_value_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + 'check_min_max_data/check_min_max'
        )
        max_int = 750000
        is_max_value =  max_int in GetMaxValue(df, ['Column_int']).run().values()
        self.assertEqual(True, is_max_value)