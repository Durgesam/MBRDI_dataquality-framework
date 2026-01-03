import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckUnique
from tests.unittest_config import UnitTestConfig


class TestCheckUnique(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_unique_pass(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check00_ok/'
        )
        self.assertTrue(CheckUnique(df, ['txt']).run())

    def test_check_unique_fail(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check01_fail/'
        )
        self.assertFalse(CheckUnique(df, ['txt']).run())

    def test_check_unique_fail2(self):
        df = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_unique_values/check02_fail/'
        )
        self.assertFalse(CheckUnique(df, ['txt']).run())
