import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks.check_regex import CheckRegex
from tests.unittest_config import UnitTestConfig


class TestCheckRegex(unittest.TestCase):
    """Test CheckRegex"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_check_regex_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_regex_data/check_regex"
        )
        # Check that every row contains either "dog", "eats" or "mouse" should pass
        self.assertEqual(True, CheckRegex(dataframe, ['string'], "dog|eats|mouse").run())

    def test_check_regex_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_regex_data/check_regex"
        )
        # Check that every row contains either "dog" or "eats" should fail
        self.assertEqual(False, CheckRegex(dataframe, ['string'], "dog|eats").run())

    def test_check_regex_invert_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_regex_data/check_regex"
        )
        # Check that every row does not contain "cat" should pass
        self.assertEqual(True, CheckRegex(dataframe, ['string'], "cat", invert_result=True).run())

    def test_check_regex_invert_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_regex_data/check_regex"
        )
        # Check that every row does not contain "dog" should fail
        self.assertEqual(False, CheckRegex(dataframe, ['string'], "dog", invert_result=True).run())
