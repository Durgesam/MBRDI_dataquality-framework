import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckUniqueTuple
from tests.unittest_config import UnitTestConfig


class TestCheckUnique(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_unique_tuple_two_columns_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        self.assertEqual(True, CheckUniqueTuple(df, ['txt', 'static_1']).run())

    def test_check_unique_two_columns_fail(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check01_fail/'
        )
        self.assertEqual(False, CheckUniqueTuple(df, ['txt', 'static_1']).run())

    def test_check_unique_two_columns_fail2(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check02_fail/'
        )
        self.assertEqual(False, CheckUniqueTuple(df, ['txt', 'static_1']).run())

    def test_check_unique_tuple_three_columns_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        self.assertEqual(True, CheckUniqueTuple(df, ['txt', 'static_1', 'static_2']).run())

    def test_check_unique_three_columns_fail(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check01_fail/'
        )
        self.assertEqual(False, CheckUniqueTuple(df, ['txt', 'static_1', 'static_2']).run())

    def test_check_unique_three_columns_fail2(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check02_fail/'
        )
        self.assertEqual(False, CheckUniqueTuple(df, ['txt', 'static_1', 'static_2']).run())