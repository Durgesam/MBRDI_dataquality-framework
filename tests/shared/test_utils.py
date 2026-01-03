import json
import os
import pathlib
import random
import unittest

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType, TimestampType

from dqf import main
from dqf.shared.utils import (
    DataFrameCache,
    check_table_columns,
    config_check,
    config_third_level_keys_check,
    create_basic_check_profile,
    get_dqf_class_from_name,
    get_configuration,
    get_log_filename,
    read_data,
    handle_str_list_columns,
)
from tests.unittest_config import UnitTestConfig


class UtilsTester(unittest.TestCase):
    """
    Test cases for ``utils.py``
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_get_dqf_class_from_name(self):
        """
        Tests if ``get_dqf_class_from_name`` raises ``ValueError`` if given check is not present.
        """
        self.assertRaises((KeyError, ValueError), get_dqf_class_from_name, "test_check")

    def test_get_log_filename(self):
        """
        Tests if ``get_log_filename`` works for dbfs path.
        """
        self.assertRaises(
            PermissionError, get_log_filename, file_path="/test", profile_name="test_profile", on_dbfs=True
        )

    def test_get_configuration(self):
        """
        Tests if ``get_configuration`` works for dbfs path.
        """
        self.assertEqual(None, get_configuration(config_name="install_path", on_dbfs=True))

    def test_config_check_empty_checks(self):
        """
        Tests if ``config_check`` works if there are no checks.
        """
        input_config = {
            "datasource_path": "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
        }
        self.assertEqual([], config_check(config=input_config))

    def test_config_check_no_type(self):
        """
        Tests if ``config_check`` works if there is no type for a check.
        Note that this is nevertheless an invalid config and the ConfigParser should raise an exception
        """
        input_config = {
            "datasource_path": "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
            "checks": [
                {"kwargs": {"table_path": "cars/", "column_name": "car_id", "d_type": "int"}},
                {
                    "type": "check_datatype",
                    "kwargs": {"table": "cars/", "columns": "customer_id", "d_type": "int"},
                },
            ],
        }
        self.assertListEqual([], config_check(config=input_config))

    def test_config_check_mismatched_keys_in_checks(self):
        """
        Tests if ``config_check`` works if there is no type for a check.
        """
        input_config = {
            "datasource_path": "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {"tabl": "cars/", "columns": "customer_id", "d_type": "int"},
                },
            ],
        }
        self.assertEqual(True, bool(config_check(config=input_config)))

    def test_config_third_level_keys_check_mismatched_check_type(self):
        """
        Tests if ``config_third_level_keys_check`` works if there is no check type in the avaliable check type.
        """
        input_check = {
            "type": "check_datatyp",
            "kwargs": {"table": "cars/", "columns": "customer_id", "d_type": "int"},
        }
        self.assertEqual(True, bool(config_third_level_keys_check(check=input_check)))

    def test_check_table_columns_empty_checks(self):
        """
        Tests if ``check_table_columns`` works if there are no checks.
        """
        input_config = {
            "datasource_path": "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
        }
        self.assertEqual([], check_table_columns(config=input_config))

    def test_check_table_columns_invalid_columns(self):
        """
        Tests if ``check_table_columns`` works if the columns are not present.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
                               + "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_value_in_range",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_i"],
                        "min_value": 0,
                        "max_value": 10000,
                    },
                }
            ],
        }
        self.assertEqual(True, bool(check_table_columns(config=input_config)))

    def test_check_table_columns_invalid_checks_args(self):
        """
        Tests if ``check_table_columns`` works if invalid check arguments are given.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
                               + "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "typ": "check_value_in_range",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_id"],
                        "min_value": 0,
                        "max_value": 10000,
                    },
                },
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_id"],
                        "d_type": "int",
                    },
                },
            ],
        }
        self.assertEqual([], check_table_columns(config=input_config))

    def test_check_table_columns_invalid_table_arg(self):
        """
        Tests if ``check_table_columns`` works if table argument is mispelled.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
                               + "cars_database/customer_cars_dataset_ok/",
            "data_format": "parquet",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "tabl": "cars",
                        "columns": ["car_id"],
                        "d_type": "int",
                    },
                },
            ],
        }
        self.assertEqual([], check_table_columns(config=input_config))

    def test_check_table_columns_raises_runtime_error(self):
        """
        Tests if ``check_table_columns`` raises ``RuntimeError``.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
                               + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "table": "dummy_table",
                        "columns": ["car_id"],
                        "d_type": "int",
                    },
                },
            ],
        }

        main.FILE_FORMAT = ""
        file_path = os.path.join(input_config["datasource_path"], input_config["checks"][0]["kwargs"]["table"])

        if not os.path.exists(file_path):
            os.makedirs(file_path)

        self.assertEqual(True, bool(check_table_columns(config=input_config)[0]))

    def test_read_data_invalid_file_format(self):
        main.FILE_FORMAT = ""
        table_path = UnitTestConfig.general_test_data_folder.value + "invalid_path_that_should_not_exist_3qwui3u49"
        self.assertRaises(RuntimeError, read_data, table_path)

    def test_read_data_parquet(self):
        """
        Test ``read_data`` for parquet files.
        """
        main.FILE_FORMAT = ""
        table_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars"

        # Assert that a dataframe is returned an the file format has been detected correctly
        self.assertIsInstance(read_data(table_path), DataFrame)
        self.assertEqual("parquet", main.FILE_FORMAT, "DQF did not store the detected file format as 'parquet'")

    def test_read_data_orc(self):
        """
        Test ``read_data`` for orc files.
        """
        main.FILE_FORMAT = ""
        table_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars_orc"

        # Create sample data
        schema = "Date", "Timestamp"
        data = [("2020-09-08", "2020-09-08"), ("2020-09-10", "2020-09-08")]
        dataframe = self.spark.createDataFrame(data=data, schema=schema).select(
            F.col("Date").cast(DateType()), F.col("Timestamp").cast(TimestampType())
        )
        dataframe.write.format("orc").save(table_path)

        # Assert that a dataframe is returned an the file format has been detected correctly
        self.assertIsInstance(read_data(table_path), DataFrame)
        self.assertEqual("orc", main.FILE_FORMAT, "DQF did not store the detected file format as 'orc'")

    def test_read_data_csv(self):
        """
        Test ``read_data`` for csv files.
        """
        main.FILE_FORMAT = ""
        table_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/cars_csv"

        # Create sample data
        schema = "Date", "Timestamp"
        data = [("2020-09-08", "2020-09-08"), ("2020-09-10", "2020-09-08")]
        dataframe = self.spark.createDataFrame(data=data, schema=schema).select(
            F.col("Date").cast(DateType()), F.col("Timestamp").cast(TimestampType())
        )
        dataframe.write.format("csv").save(table_path)

        # Assert that a dataframe is returned an the file format has been detected correctly
        self.assertIsInstance(read_data(table_path), DataFrame)
        self.assertEqual("csv", main.FILE_FORMAT, "DQF did not store the detected file format as 'csv'")

    def test_dataframe_cache(self):
        """
        Tests whether ``DataFrameCache`` for ``get_path`` for dbfs is working as excepted.
        """
        dataframe_path = DataFrameCache().get_path("test_dataframe_cache", dbfs_prefix=True)

        self.assertEqual(True, str(dataframe_path)[:5] == "/dbfs")

    def test_create_basic_check_profile(self):
        """
        Test ``create_basic_check_profile``.
        """
        datasource_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/"
        table_path = datasource_path + "customers/"

        # Create check profile
        created_config = create_basic_check_profile(table_path, "parquet")
        self.assertEqual(datasource_path, created_config["datasource_path"])
        self.assertEqual("parquet", created_config["data_format"])

        # Test some of the created checks
        for check in created_config["checks"]:
            # Each check contains a 'table' argument
            self.assertEqual("customers", check["kwargs"]["table"])
        # Check if some default checks are included
        created_check_types = set(c["type"] for c in created_config["checks"])
        self.assertSetEqual({"check_empty_df", "check_null", "check_empty_string", "check_datatype"}, created_check_types)

    def test_create_basic_check_profile_save(self):
        """
        Test that ``create_basic_check_profile`` saves a config file to the specified path.
        """
        table_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/customers/"

        create_basic_check_profile(
            table_path,
            "parquet",
            config_save_path="tests/data_generation/basic_config.json",
        )

        file = pathlib.Path("tests/data_generation/basic_config.json")
        self.assertTrue(file.exists())

        config = json.loads(file.read_text())
        self.assertIsInstance(config, dict)

    def test_create_basic_check_profile_save_dbfs(self):
        """
        Test that ``create_basic_check_profile`` raises an exception when saving to an invalid path (with dbfs)
        """
        table_path = UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/customers/"

        with self.assertRaises(FileNotFoundError):
            create_basic_check_profile(
                table_path,
                "parquet",
                config_save_path="tests/data_generation/basic_config.json",
                on_dbfs=True,
            )

    def test_create_basic_check_profile_csv_kwargs(self):
        """
        Test that ``create_basic_check_profile`` creates a config for csv files with load options.
        """
        table_path = UnitTestConfig.general_test_data_folder.value + "dummy_csv"

        # Create dummy data
        schema = "Date", "Timestamp"
        data = [("2020-09-08", "2020-09-08"), ("2020-09-10", "2020-09-08")]
        dataframe = self.spark.createDataFrame(data=data, schema=schema).select(
            F.col("Date").cast(DateType()), F.col("Timestamp").cast(TimestampType())
        )
        dataframe.write.format("csv").save(table_path)

        # Create check profile
        created_config = create_basic_check_profile(table_path, "csv", inferSchema=True, header=False)

        # Test first-level values in the config
        self.assertEqual(UnitTestConfig.general_test_data_folder.value, created_config["datasource_path"])
        self.assertEqual("csv", created_config["data_format"])
        self.assertDictEqual(dict(inferSchema=True, header=False), created_config["load_options"])

        # Test some of the created checks
        for check in created_config["checks"]:
            # Each check contains a 'table' argument
            self.assertEqual("dummy_csv", check["kwargs"]["table"])
        # Check if some default checks are included
        created_check_types = set(c["type"] for c in created_config["checks"])
        self.assertSetEqual({"check_empty_df", "check_null", "check_empty_string", "check_datatype"}, created_check_types)

    def test_stats_cache_count(self):
        count = random.randint(10, 1000)
        data = [(0, 1)] * count
        df = self.spark.createDataFrame(data)
        self.assertEqual(count, main.cache.get_count(df))

    def test_stats_cache_max_value(self):
        count = random.randint(10, 1000)
        data = [(i, -i) for i in range(count)]
        df = self.spark.createDataFrame(data, ("A", "B"))
        self.assertEqual(count - 1, main.cache.get_max_value(df, "A"))

    def test_stats_cache_min_value(self):
        count = random.randint(10, 1000)
        data = [(i, -i) for i in range(count)]
        df = self.spark.createDataFrame(data, ("A", "B"))
        self.assertEqual(-count + 1, main.cache.get_min_value(df, "B"))

    def test_stats_cache_get_cached_value(self):
        expected = "result"
        df = self.spark.createDataFrame([(0, "a"), (1, "b"), (2, "b"), (1, "c"), (2, "c")], ("A", "B"))

        function_executed = False

        def value_calculation_1():
            nonlocal function_executed
            function_executed = True
            return expected

        # First calculation should call the function
        actual = main.cache._get_cached_value(value_calculation_1, df, 511309, "key3")
        self.assertEqual(expected, actual, f"Cache returned {actual} instead of {expected}")
        self.assertTrue(function_executed, "Cache function has not been called although no cached value exists yet")

        # Second calculation should not call the function
        function_executed = False

        def value_calculation_2():
            nonlocal function_executed
            function_executed = True
            return expected + "another_string"

        actual = main.cache._get_cached_value(value_calculation_2, df, 511309, "key3")
        self.assertEqual(expected, actual, f"Cache returned {actual} instead of {expected}")
        self.assertFalse(function_executed, "Cache function has been called although a cached value should exist")

        # Cache dictionary should have some entries
        self.assertTrue(df.cache_uuid in main.cache._cached_stats, "dataframe not found in cache dict")
        self.assertTrue(511309 in main.cache._cached_stats[df.cache_uuid], "'511309' not found in cache dict")
        self.assertTrue("key3" in main.cache._cached_stats[df.cache_uuid][511309], "'key3' not found in cache dict")

    def test_handle_str_list_columns(self):
        self.assertEqual(["abc"], handle_str_list_columns("abc"))
        self.assertEqual(["abc def"], handle_str_list_columns("abc def"))
        self.assertEqual(["abc", "def"], handle_str_list_columns(["abc", "def"]))
        self.assertEqual(["abc", "def"], handle_str_list_columns(("abc", "def")))
        self.assertRaises(ValueError, handle_str_list_columns, 9)
