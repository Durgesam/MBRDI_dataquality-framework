import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckEnum
from tests.unittest_config import UnitTestConfig


class TestCheckEnum(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_enum_int_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check00_ok/'
        )
        self.assertEqual(True, CheckEnum(dataframe, ['id'], [1]).run())

    def test_check_enum_string_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check00_ok/'
        )
        self.assertEqual(True, CheckEnum(dataframe, ['txt'], ["string"]).run())

    def test_check_enum_int_fail1(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check01_fail/'
        )
        self.assertEqual(False, CheckEnum(dataframe, ['id'], [1]).run())

    def test_check_enum_string_fail1(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check01_fail/'
        )
        self.assertEqual(False, CheckEnum(dataframe, ['txt'], ["string"]).run())

    def test_check_enum_int_fail2(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check02_fail/'
        )
        self.assertEqual(False, CheckEnum(dataframe, ['id'], [1]).run())

    def test_check_enum_string_fail2(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_enum/parquet/check02_fail/'
        )
        self.assertEqual(False, CheckEnum(dataframe, ['txt'], ["string"]).run())

