import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckNull
from tests.unittest_config import UnitTestConfig


class TestCheckNull(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_null_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/parquet/check00_ok/'
        )
        self.assertEqual(True, CheckNull(dataframe, ['id', 'txt']).run())

    def test_check_null_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/parquet/check01_fail/'
        )
        self.assertEqual(False, CheckNull(dataframe, ['id', 'txt']).run())

    def test_check_null_fail2(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/parquet/check02_fail/'
        )
        self.assertEqual(False, CheckNull(dataframe, ['id', 'txt']).run())

    def test_check_null_string_pass(self):
        dataframe = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check00_ok/'
        )
        self.assertEqual(True, CheckNull(dataframe, ['id', 'txt'], string_check=True).run())

    def test_check_null_string_pass2(self):
        dataframe = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check00_ok/'
        )
        self.assertEqual(True, CheckNull(dataframe, ['id', 'txt'], string_check=False).run())

    def test_check_null_string_fail2(self):
        dataframe = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check01_fail/'
        )
        self.assertEqual(False, CheckNull(dataframe, ['id', 'txt'], string_check="tRue").run())

    def test_check_null_string_fail3(self):
        dataframe = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check02_fail/'
        )
        self.assertEqual(False, CheckNull(dataframe, ['id', 'txt'], string_check=True).run())

    def test_check_null_string_fail4(self):
        dataframe = self.spark.read.format('csv').option('inferSchema', True).option('header', True).load(
            UnitTestConfig.general_test_data_folder.value + '/check_nulls/csv/check02_fail/'
        )
        self.assertEqual(False, CheckNull(dataframe, ['id', 'txt'], string_check=False).run())
