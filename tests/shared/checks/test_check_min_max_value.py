import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckMaxValue, CheckMinValue
from tests.unittest_config import UnitTestConfig


class TestCheckMinMaxValue(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_max_value_pass_int(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(True, CheckMaxValue(dataframe, ['Column_int'], 999999).run())

    def test_check_max_value_fail_int(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(False, CheckMaxValue(dataframe, ['Column_int'], 2000).run())

    def test_check_max_value_pass_float(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(True, CheckMaxValue(dataframe, ['Column_float'], 10000.89).run())

    def test_check_max_value_fail_float(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(False, CheckMaxValue(dataframe, ['Column_float'], 13.5).run())

    def test_check_min_value_pass_int(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(True, CheckMinValue(dataframe, ['Column_int'], 2).run())

    def test_check_min_value_fail_int(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(False, CheckMinValue(dataframe, ['Column_int'], 2000).run())

    def test_check_min_value_pass_float(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(True, CheckMinValue(dataframe, ['Column_float'], 0.2).run())

    def test_check_min_value_fail_float(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_min_max_data/check_min_max"
        )
        self.assertEqual(False, CheckMinValue(dataframe, ['Column_float'], 1300.5).run())
