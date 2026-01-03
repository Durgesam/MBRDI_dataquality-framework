import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckStringPattern
from tests.unittest_config import UnitTestConfig


class TestCheckStringPattern(unittest.TestCase):
    """
    Test cases for `CheckStringPattern` class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_check_string_pattern_default_pass(self):
        """
        Checks whether the test case is passing for default values.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckStringPattern(dataframe, ("car_id", "customer_id"), "", "").run())

    def test_check_string_pattern_starts_with_pass(self):
        """
        Checks whether the test case is passing for given value of `starts_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(True, CheckStringPattern(dataframe, "msg", "str", "").run())

    def test_check_string_pattern_ends_with_pass(self):
        """
        Checks whether the test case is passing for given value of `ends_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(True, CheckStringPattern(dataframe, "msg", "", "ng").run())

    def test_check_string_pattern_starts_with_ends_with_pass(self):
        """
        Checks whether the test case is passing for given value of `starts_with` and `ends_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(True, CheckStringPattern(dataframe, "msg", "Str", "ng").run())

    def test_check_string_pattern_case_sensitive_pass(self):
        """
        Checks whether the test case is passing for `case_sensitive`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(True, CheckStringPattern(dataframe, "msg", "Str", "ng", True).run())

    def test_check_string_pattern_starts_with_fail(self):
        """
        Checks whether the test case is failing for given value of `starts_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(False, CheckStringPattern(dataframe, "msg", "tr", "").run())

    def test_check_string_pattern_ends_with_fail(self):
        """
        Checks whether the test case is failing for given value of `ends_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(False, CheckStringPattern(dataframe, "msg", "", "n").run())

    def test_check_string_pattern_starts_with_ends_with_fail(self):
        """
        Checks whether the test case is failing for given value of `starts_with` and `ends_with`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(False, CheckStringPattern(dataframe, "msg", "tr", "ng").run())

    def test_check_string_pattern_case_sensitive_fail(self):
        """
        Checks whether the test case is failing for `case_sensitive`.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/measurements/"
        )
        self.assertEqual(False, CheckStringPattern(dataframe, "msg", "Str", "Ng", True).run())
