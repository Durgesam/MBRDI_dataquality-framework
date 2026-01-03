import unittest

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, TimestampType

from dqf.shared.checks import CheckDatatype
from tests.unittest_config import UnitTestConfig


class CheckDatatypeTester(unittest.TestCase):
    """
    Test cases for `CheckDatatype` class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_check_datatype_int_pass(self):
        """
        Checks whether the test case is passing for `IntegerType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check00_int_ok/"
        )
        self.assertEqual(True, CheckDatatype(dataframe, "column1", "bigint").run())

    def test_check_datatype_int_fail(self):
        """
        Checks whether the test case is failing for `IntegerType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check01_int_fail/"
        )
        self.assertEqual(False, CheckDatatype(dataframe, "column1", "bigint").run())

    def test_check_datatype_str_pass(self):
        """
        Checks whether the test case is passing for `StringType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check02_str_ok/"
        )
        self.assertEqual(True, CheckDatatype(dataframe, "column1", "string").run())

    def test_check_datatype_str_fail(self):
        """
        Checks whether the test case is failing for `StringType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check03_str_fail/"
        )
        self.assertEqual(False, CheckDatatype(dataframe, "column1", "string").run())

    def test_check_datatype_float_pass(self):
        """
        Checks whether the test case is passing for `FloatType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check04_float_ok/"
        )
        self.assertEqual(True, CheckDatatype(dataframe, "column1", "double").run())

    def test_check_datatype_float_fail(self):
        """
        Checks whether the test case is failing for `FloatType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check05_float_fail/"
        )
        self.assertEqual(False, CheckDatatype(dataframe, "column1", "double").run())

    def test_check_datatype_decimal_pass(self):
        """
        Checks whether the test case is passing for `DecimalType` columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "/check_datatype/check04_float_ok/"
        )
        dataframe = dataframe.select(F.col("column1").cast(DecimalType(12, 2)))
        self.assertEqual(True, CheckDatatype(dataframe, "column1", "decimal").run())

    def test_check_datatype_timestamp_date_pass(self):
        """
        Checks whether the test case is passing for `DateType` and `TimestampType` columns.
        """
        schema = "Date", "Timestamp"
        data = [("2020-09-08", "2020-09-08"), ("2020-09-10", "2020-09-08")]
        dataframe = self.spark.createDataFrame(data=data, schema=schema).select(
            F.col("Date").cast(DateType()), F.col("Timestamp").cast(TimestampType())
        )

        self.assertEqual(True, CheckDatatype(dataframe, "Date", "date").run())
        self.assertEqual(True, CheckDatatype(dataframe, "Timestamp", "Timestamp").run())

    def test_check_datatype_list_columns(self):
        """
        Checks whether the test case is passing for a list of columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckDatatype(dataframe, ["car_id", "customer_id"], "int").run())

    def test_check_datatype_tuple_columns(self):
        """
        Checks whether the test case is passing for a tuple of columns.
        """
        dataframe = self.spark.read.parquet(
            UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars/"
        )
        self.assertEqual(True, CheckDatatype(dataframe, ("car_id", "customer_id"), "int").run())

    def test_check_datatype_raises_value_error(self):
        """
        Checks whether ``CheckDatatype`` raises ``ValueError`` if dataframe is of type int.
        """
        self.assertRaises(ValueError, CheckDatatype, 10, "column1", "bigint")
