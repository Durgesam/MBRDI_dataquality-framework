import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckStringConsistency
from tests.unittest_config import UnitTestConfig


class TestCheckStringConsistency(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_string_consistency_pass_exact(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check00_ok/'
        )
        self.assertEqual(True, CheckStringConsistency(dataframe, ['txt'], 10).run())

    def test_check_string_consistency_fail_exact(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check01_fail/'
        )
        self.assertEqual(False, CheckStringConsistency(dataframe, ['txt'], 10).run())

    def test_check_string_consistency_pass_min(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check00_ok/'
        )
        self.assertEqual(True, CheckStringConsistency(dataframe, ['txt'], 9, "min").run())

    def test_check_string_consistency_fail_min(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check00_ok/'
        )
        self.assertEqual(False, CheckStringConsistency(dataframe, ['txt'], 11, "min").run())

    def test_check_string_consistency_pass_max(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check00_ok/'
        )
        self.assertEqual(True, CheckStringConsistency(dataframe, ['txt'], 15, "max").run())

    def test_check_string_consistency_fail_max(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_string_consistency/check00_ok/'
        )
        self.assertEqual(False, CheckStringConsistency(dataframe, ['txt'], 9, "max").run())
