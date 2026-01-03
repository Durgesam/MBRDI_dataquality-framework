import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckMinTimestamp
from tests.unittest_config import UnitTestConfig


class TestCheckMinTimeStamp(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_check_max_timestamp_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/customers/"
        )
        self.assertEqual(True, CheckMinTimestamp(dataframe, ['customer_since'], "1789-01-01").run())

    def test_check_empty_string_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/customers/"
        )
        self.assertEqual(False, CheckMinTimestamp(dataframe, ['birthdate'], "2000-01-01").run())