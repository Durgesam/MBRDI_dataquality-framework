import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckEmptyString
from tests.unittest_config import UnitTestConfig


class TestCheckEmptyString(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_empty_string_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_empty_strings/check00_ok/'
        )
        self.assertEqual(True, CheckEmptyString(dataframe, ['id', 'txt']).run())

    def test_check_empty_string_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_empty_strings/check01_fail/'
        )
        self.assertEqual(False, CheckEmptyString(dataframe, ['id', 'txt']).run())

    def test_check_empty_string_fail_2(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_empty_strings/check02_fail/'
        )
        self.assertEqual(False, CheckEmptyString(dataframe, ['id', 'txt']).run())
