import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks.check_continuous_samples import CheckContinuousSamples
from tests.unittest_config import UnitTestConfig


class TestCheckContinuousSamples(unittest.TestCase):
    """
    The following situations need to be covered by the tests:
    1. Pass
    2. Fail: start does not match end
    3. Fail: end does not match start
    4. Fail: Both cases 2 and 3
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_check_continuous_samples_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        self.assertEqual(True, CheckContinuousSamples(dataframe, ['channel_id'], "start_ok", "end_ok").run())

    def test_check_continuous_samples_fail_start(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        self.assertEqual(False, CheckContinuousSamples(dataframe, ['channel_id'], "start_fail", "end_ok").run())

    def test_check_continuous_samples_fail_end(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        self.assertEqual(False, CheckContinuousSamples(dataframe, ['channel_id'], "start_ok", "end_fail").run())

    def test_check_continuous_samples_fail_start_end(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        self.assertEqual(False, CheckContinuousSamples(dataframe, ['channel_id'], "start_fail", "end_fail").run())

    def test_check_continuous_samples_pass_not_strict(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        check = CheckContinuousSamples(dataframe, ['channel_id'], "start_ok_not_strict", "end_ok", strict=False)
        self.assertEqual(True, check.run())

    def test_check_continuous_samples_fail_strict(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_continuous_samples_data/check_continuous_samples"
        )
        check = CheckContinuousSamples(dataframe, ['channel_id'], "start_ok_not_strict", "end_ok", strict=True)
        self.assertEqual(False, check.run())
