import unittest

from pyspark.sql import SparkSession

from dqf.shared.stats.get_null_count import GetNullCount
from tests.unittest_config import UnitTestConfig

class TestGetNullCount(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_get_null_count_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/parquet/check02_fail/'
        )
        null_count=4
        is_null_count = null_count in GetNullCount(df, ['txt']).run().values()
        self.assertEqual(True, is_null_count)

    def test_get_null_string_count_pass(self):
        df = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check02_fail/'
        )
        null_count=5
        is_null_count = null_count in GetNullCount(df, ['txt'], False).run().values()
        self.assertEqual(True, is_null_count)

    def test_get_null_string_count_pass2(self):
        df = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check02_fail/'
        )
        null_count=8
        is_null_count = null_count in GetNullCount(df, ['txt'], True).run().values()
        self.assertEqual(True, is_null_count)
