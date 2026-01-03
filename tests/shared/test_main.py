import unittest

from pyspark.sql import SparkSession

from dqf.main import CheckProject, dqf_help, get_install_path, get_project_version
from tests.unittest_config import UnitTestConfig

CONFIG_PASS = {
    "datasource_path": UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/",
    "data_format": "parquet",
    "profile_name": "TestProfile",
    "checks": [
        {
            "type": "check_value_in_range",
            "kwargs": {
                "table": "cars",
                "columns": ["car_id"],
                "min_value": 0,
                "max_value": 10000,
            },
        }
    ],
}


CONFIG_FAIL = {
    "datasource_path": UnitTestConfig.general_test_data_folder.value + "cars_database/customer_cars_dataset_ok/",
    "data_format": "parquet",
    "profile_name": "TestProfile",
    "checks": [
        {
            "type": "check_value_in_range",
            "kwargs": {
                "table": "cars",
                "columns": ["car_id"],
                "min_value": 10,
                "max_value": 10000,
            },
        }
    ],
}


class TestMain(unittest.TestCase):
    """
    Test cases for ``main.py``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("UnitTests").getOrCreate()

    def test_main_run_pass(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        check_project = CheckProject(config=CONFIG_PASS, on_dbfs=False, write_results=False)
        self.assertEqual(True, check_project.run())

    def test_main_run_fail(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        check_project = CheckProject(config=CONFIG_FAIL, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_with_dataframe_pass(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        dataframe = self.spark.read.parquet(CONFIG_PASS["datasource_path"] + "cars")
        check_project = CheckProject(config=CONFIG_PASS, on_dbfs=False, write_results=False, dataframe=dataframe)
        self.assertEqual(True, check_project.run())

    def test_main_run_with_dataframe_fail(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        dataframe = self.spark.read.parquet(CONFIG_FAIL["datasource_path"] + "cars")
        check_project = CheckProject(config=CONFIG_FAIL, on_dbfs=False, write_results=False, dataframe=dataframe)
        self.assertEqual(False, check_project.run())

    def test_main_run_config_path(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        self.assertRaises(
            RuntimeError, CheckProject, config="tests/assets/config.json", on_dbfs=False, write_results=False
        )

    def test_main_run_check_exception(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_id"],
                        "d_type": 1,
                    },
                },
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_profile_name_mandatory(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "checks": [
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

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_invalid_kwargs(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_id"],
                        "d_t": "int",
                    },
                },
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_invalid_table_columns(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["dummy_column"],
                        "d_type": "int",
                    },
                },
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_mandatory_data_format(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
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

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_main_run_excepts_config_parser_exception(self):
        """
        Checks whether ``CheckProject`` run is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "checks": [
                {
                    "typ": "check_datatype",
                    "kwargs": {
                        "table": "cars",
                        "columns": ["car_id"],
                        "d_type": "int",
                    },
                },
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_get_project_version(self):
        """
        Checks whether ``get_project_version`` is working as expected.
        """
        self.assertEqual(None, get_project_version())

    def test_get_install_path(self):
        """
        Checks whether ``get_install_path`` is working as expected.
        """
        self.assertEqual(True, bool(get_install_path()))

    def test_dqf_help(self):
        """
        Checks whether ``dqf_help`` is working as expected.
        """
        self.assertEqual(None, dqf_help())

    def test_incremental_load_previous_day(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "previous_day",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_previous_week(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "previous_week",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_previous_month(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "previous_month",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_years_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "years=1",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_months_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
                               + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "months=1",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_weeks_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "weeks=1",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_days_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "days=1",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_date_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "2021-02-01",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(False, check_project.run())

    def test_incremental_load_invalid_column(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "load_ts",
            "incremental_filter": "previous_day",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)
        self.assertEqual(True, check_project.run())

    def test_incremental_load_invalid_column_with_dataframe(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "load_ts",
            "incremental_filter": "previous_day",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        dataframe = self.spark.read.parquet(input_config["datasource_path"] + "customers")

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False, dataframe=dataframe)
        self.assertEqual(True, check_project.run())

    def test_incremental_load_invalid_incremental_filter(self):
        """
        Checks whether ``CheckProject`` run for incremental load is working as expected.
        """
        input_config = {
            "datasource_path": UnitTestConfig.general_test_data_folder.value
            + "cars_database/customer_cars_dataset_ok/",
            "profile_name": "TestProfile",
            "data_format": "parquet",
            "incremental_column": "customer_since",
            "incremental_filter": "previous_year",
            "checks": [
                {
                    "type": "check_empty_df",
                    "kwargs": {
                        "table": "customers",
                    },
                }
            ],
        }

        check_project = CheckProject(config=input_config, on_dbfs=False, write_results=False)

        self.assertRaises(ValueError, check_project.run)
