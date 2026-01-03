import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckEmptyDF
from tests.unittest_config import UnitTestConfig


class CheckEmptyDFTester(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_empty_df_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_empty_df/check00_fail/'
        )
        self.assertFalse(CheckEmptyDF(dataframe).run())

    def test_check_empty_df_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_empty_df/check01_ok/'
        )
        self.assertTrue(CheckEmptyDF(dataframe).run())
