import unittest

from pyspark.sql import SparkSession

from dqf.shared.checks import CheckDataRelation
from tests.unittest_config import UnitTestConfig


class TestCheckDataRelation(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession \
            .builder \
            .appName("UnitTests") \
            .getOrCreate()

    def test_data_relation_pass(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/customers/"
        )
        self.assertEqual(True, CheckDataRelation(dataframe, 'birthdate', 'customer_since').run())

    def test_data_relation_fail(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_data_relation/check01_fail/"
        )
        self.assertEqual(False, CheckDataRelation(dataframe, 'id', 'txt').run())

    def test_data_relation_fail_2(self):
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "check_data_relation/check02_fail/"
        )
        self.assertEqual(False, CheckDataRelation(dataframe, 'id', 'txt').run())
