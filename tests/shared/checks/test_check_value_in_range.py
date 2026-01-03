import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckValueInRange
from tests.unittest_config import UnitTestConfig


class TestCheckValueInRange(unittest.TestCase):
    """
    Test cases for `CheckValueInRange` class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_check_value_in_range_pass(self):
        """
        Checks whether the test case is passing for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckValueInRange(dataframe, "car_id", 0, 10000).run())

    def test_check_value_in_range_fail(self):
        """
        Checks whether the test case is failing for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(False, CheckValueInRange(dataframe, "car_id", 10, 1000).run())

    def test_check_value_in_range_list_columns_pass(self):
        """
        Checks whether the test case is passing for list of columns for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckValueInRange(dataframe, ["car_id", "customer_id"], 0, 10000).run())

    def test_check_value_in_range_list_columns_fail(self):
        """
        Checks whether the test case is failing for list of columns for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(False, CheckValueInRange(dataframe, ["car_id", "customer_id"], 10, 100).run())

    def test_check_value_in_range_tuple_columns_pass(self):
        """
        Checks whether the test case is passing for tuple of columns for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckValueInRange(dataframe, ("car_id", "customer_id"), 0, 10000).run())

    def test_check_value_in_range_tuple_columns_fail(self):
        """
        Checks whether the test case is failing for tuple of columns for given `min_value` and `max_value`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(False, CheckValueInRange(dataframe, ("car_id", "customer_id"), 10, 100).run())
