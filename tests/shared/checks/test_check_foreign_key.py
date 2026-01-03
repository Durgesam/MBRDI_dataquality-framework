import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckForeign
from tests.unittest_config import UnitTestConfig


class TestCheckForeign(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_foreign_key_pass(self):
        df_primary = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_foreign_key/check00_primary_ok/'
        )
        df_foreign = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_foreign_key/check00_foreign_ok/'
        )
        self.assertEqual(True, CheckForeign(df_primary, df_foreign, 'id', 'foreign').run())

    def test_check_foreign_key_fail(self):
        df_primary = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_foreign_key/check01_primary_fail/'
        )
        df_foreign = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_foreign_key/check01_foreign_fail/'
        )
        self.assertEqual(False, CheckForeign(df_primary, df_foreign, 'id', 'foreign').run())
