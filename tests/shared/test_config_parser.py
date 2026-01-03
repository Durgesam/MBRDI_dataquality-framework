import unittest

from dqf.config import ConfigParser, ConfigParserException, InvalidConfigKeyException


class TestConfigParser(unittest.TestCase):
    """
    Test cases for ``ConfigParser``
    """
    def test_parse_identity(self):
        """ Parse a config which is already in expected format """
        input_config = expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {"type": "check_datatype", "kwargs": {"table_path": "cars/", "column_name": "car_id", "d_type": "int"}},
                {
                    "type": "check_datatype",
                    "kwargs": {"table_path": "cars/", "column_name": "customer_id", "d_type": "int"},
                },
                {"type": "check_file_format", "kwargs": {"file_path": "cars/cars_20201005.orc", "file_format": "orc"}},
                {"type": "check_empty_df", "kwargs": {"table_path": "cars/"}},
                {"type": "check_incremental_larger", "kwargs": {"folder_path": "cars/", "file_format": "orc"}},
                {"type": "check_table_format", "kwargs": {"table_path": "cars/", "table_format": "orc"}},
                {"type": "check_whitespace", "kwargs": {"folder_path": "cars/"}},
                {"type": "check_folder_file_name", "kwargs": {"table_name": "cars", "file_name": "cars_20201005.orc"}},
                {
                    "type": "check_time_format",
                    "kwargs": {"file_path": "cars/cars_20201005.orc", "time_format": "YYYYMMDD"},
                },
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_shared_vars_1(self):
        """ Parse a config with a single check element and shared vars """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {
                    "type": "check_datatype",
                    "vars": {"table": "cars"},
                    "kwargs": [
                        {"column_name": "car_id", "d_type": "int"},
                        {"column_name": "customer_id", "d_type": "int"},
                    ],
                }
            ],
        }
        expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "car_id"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "customer_id"}},
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_shared_vars_2(self):
        """ Parse a config with shared vars on the top-most level """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "vars": {"table": "cars", "d_type": "int"},
            "checks": [
                {"type": "check_datatype", "kwargs": [{"column_name": "car_id"}, {"column_name": "customer_id"}]}
            ],
        }
        expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "car_id"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "customer_id"}},
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_nested_config_1(self):
        """ Parse a config with several nested items """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {
                    "type": "check_datatype",
                    "vars": {"table": "cars", "d_type": "int"},
                    "kwargs": [{"column_name": "car_id"}, {"column_name": "customer_id"}],
                }
            ],
        }
        expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "car_id"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "d_type": "int", "column_name": "customer_id"}},
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_type_in_kwargs(self):
        """
        Parse a config for a check with a "type" argument.
        Although there is no check with a "type" argument currently,
        we should ensure that it works as expected to be prepared for more checks in the future
        """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {
                    "type": "check_datatype",
                    "vars": {"table": "cars", "type": "int"},
                    "kwargs": [
                        {
                            "column_name": "car_id",
                        },
                        {
                            "column_name": "customer_id",
                        },
                    ],
                }
            ],
        }
        expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "checks": [
                {"type": "check_datatype", "kwargs": {"table": "cars", "column_name": "car_id", "type": "int"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "column_name": "customer_id", "type": "int"}},
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_complex_config(self):
        """ Parse a config file with multiple levels, shared arguments and many checks """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "data_format": "orc",
            "vars": {"table": "cars"},
            "checks": [
                {
                    # Group checks for one column
                    "vars": {
                        "column_name": "car_id",
                    },
                    "checks": [
                        {"type": "check_datatype", "kwargs": {"d_type": "int"}},
                        {
                            # We can also add another level (although it is not useful in this specific example)
                            "checks": [{"type": "check_unique"}, {"type": "check_null"}]
                        },
                    ],
                },
                {
                    # Group checks for another column
                    "vars": {
                        "table": "cars",
                        "column_name": "customer_id",
                    },
                    "checks": [
                        {"type": "check_datatype", "kwargs": {"d_type": "int"}},
                        # We can also omit the "type" if no more arguments are required
                        "check_unique",
                        "check_null",
                    ],
                },
            ],
        }
        expected_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "data_format": "orc",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "d_type": "int",
                        "table": "cars",
                        "column_name": "car_id",
                    },
                },
                {
                    "type": "check_unique",
                    "kwargs": {
                        "table": "cars",
                        "column_name": "car_id",
                    },
                },
                {
                    "type": "check_null",
                    "kwargs": {
                        "table": "cars",
                        "column_name": "car_id",
                    },
                },
                {
                    "type": "check_datatype",
                    "kwargs": {
                        "d_type": "int",
                        "table": "cars",
                        "column_name": "customer_id",
                    },
                },
                {
                    "type": "check_unique",
                    "kwargs": {
                        "table": "cars",
                        "column_name": "customer_id",
                    },
                },
                {
                    "type": "check_null",
                    "kwargs": {
                        "table": "cars",
                        "column_name": "customer_id",
                    },
                },
            ],
        }
        parser = ConfigParser(input_config)
        parsed_config = parser.parse_config()
        self.assertEqual(expected_config, parsed_config, "The parsed config does not match the expected config")

    def test_parse_typo_type(self):
        input_config = {
            "datasource_path": "/dbfs/mnt/customer_cars_dataset_ok/",
            "profile_name": "cars",
            "data_format": "parquet",
            "checks": [
                {"typ": "check_datatype", "kwargs": {"table": "car", "columns": "car_id", "d_type": "int"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "columns": "car_id", "d_type": "str"}},
                {"type": "check_datatype", "kwargs": {"table": "customer", "columns": "customer_id", "d_type": "int"}},
            ],
        }
        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(ConfigParserException):
            ConfigParser(input_config).parse_config()

    def test_parse_typo_kwargs(self):
        input_config = {
            "datasource_path": "/dbfs/mnt/customer_cars_dataset_ok/",
            "profile_name": "cars",
            "data_format": "parquet",
            "checks": [
                {"type": "check_datatype", "kwargss": {"table": "car", "columns": "car_id", "d_type": "int"}},
                {"type": "check_datatype", "kwargs": {"table": "cars", "columns": "car_id", "d_type": "str"}},
                {"type": "check_datatype", "kwargs": {"table": "customer", "columns": "customer_id", "d_type": "int"}},
            ],
        }
        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(InvalidConfigKeyException):
            try:
                ConfigParser(input_config).parse_config()
            except InvalidConfigKeyException as e:
                self.assertSetEqual({"kwargss"}, e.keys)
                raise e

    def test_parse_typo_vars(self):
        """
        Tests if ``ConfigParser`` throws ``InvalidConfigKeyException`` when ``vars`` is misspelled in ``checks``.
        """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "data_format": "orc",
            "vars": {"table": "cars"},
            "checks": [
                {
                    # Group checks for one column
                    "var": {
                        "column_name": "car_id",
                    },
                    "checks": [
                        {"type": "check_datatype", "kwargs": {"d_type": "int"}},
                        {"checks": [{"type": "check_unique"}, {"type": "check_null"}]},
                    ],
                },
            ],
        }

        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(InvalidConfigKeyException):
            try:
                ConfigParser(input_config).parse_config()
            except InvalidConfigKeyException as e:
                self.assertSetEqual({"var"}, e.keys)
                raise e

    def test_parse_two_typos(self):
        """
        Tests if ``ConfigParser`` throws ``InvalidConfigKeyException`` when ``vars`` is misspelled in ``checks``.
        """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "data_format": "orc",
            "vars": {"table": "cars"},
            "checks": [
                {
                    # Group checks for one column
                    "var": {
                        "column_name": "car_id",
                    },
                    "donald duck": 42,
                    "checks": [
                        {"type": "check_datatype", "kwargs": {"d_type": "int"}},
                        {"checks": [{"type": "check_unique"}, {"type": "check_null"}]},
                    ],
                },
            ],
        }

        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(InvalidConfigKeyException):
            try:
                ConfigParser(input_config).parse_config()
            except InvalidConfigKeyException as e:
                self.assertSetEqual({"var", "donald duck"}, e.keys)
                raise e

    def test_parse_invalid_check_element_keys(self):
        input_config = {
            "datasource_path": "/dbfs/mnt/customer_cars_dataset_ok/",
            "profile_name": "cars",
            "data_format": "parquet",
            "checks": [
                {
                    "type": "check_datatype",
                    "kwargs": {"table": "car", "columns": "car_id", "d_type": "int"},
                    "checks": [],
                },
                {"type": "check_datatype", "kwargs": {"table": "cars", "columns": "car_id", "d_type": "str"}},
                {"type": "check_datatype", "kwargs": {"table": "customer", "columns": "customer_id", "d_type": "int"}},
            ],
        }

        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(InvalidConfigKeyException):
            try:
                ConfigParser(input_config).parse_config()
            except InvalidConfigKeyException as e:
                # Inner "checks" is not allowed
                self.assertSetEqual({"checks"}, e.keys)
                raise e

    def test_parse_invalid_vars_key(self):
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "var": {"table": "cars", "d_type": "int"},
            "checks": [
                {"type": "check_datatype", "kwargs": [{"column_name": "car_id"}, {"column_name": "customer_id"}]}
            ],
        }

        # Assert that the correct exception is raised and that it contains the correct key
        with self.assertRaises(InvalidConfigKeyException):
            try:
                ConfigParser(input_config).parse_config()
            except InvalidConfigKeyException as e:
                # Inner "checks" is not allowed
                self.assertSetEqual({"var"}, e.keys)
                raise e

    def test_parse_checks_not_list(self):
        """
        Tests if ``ConfigParser`` throws ``ConfigParserException`` when ``checks`` is not a list.
        """
        input_config = {
            "datasource_path": "/dbfs/mnt/customer_cars_dataset_ok/",
            "profile_name": "cars",
            "data_format": "parquet",
            "checks": {"type": "check_datatype", "kwargs": {"table": "car", "columns": "car_id", "d_type": "int"}},
        }
        parser = ConfigParser(input_config)
        self.assertRaises(ConfigParserException, parser.parse_config)

    def test_parse_invalid_check_element(self):
        """
        Tests if ``ConfigParser`` throws ``ConfigParserException`` when check_element is not a dict or str.
        """
        input_config = {
            "datasource_path": "customer_cars_dataset_ok/",
            "vars": {"table": "cars", "d_type": "int"},
            "checks": [["type", "check_datatype"]],
        }
        parser = ConfigParser(input_config)
        self.assertRaises(ConfigParserException, parser.parse_config)

    def test_parse_invalid_kwargs(self):
        """
        Tests if ``ConfigParser`` throws ``ConfigParserException`` when ``kwargs`` is not a list or dictionary.
        """
        input_config = {
            "datasource_path": "/dbfs/mnt/customer_cars_dataset_ok/",
            "profile_name": "cars",
            "data_format": "parquet",
            "checks": {"type": "check_datatype", "kwargs": 10},
        }
        parser = ConfigParser(input_config)
        self.assertRaises(ConfigParserException, parser.parse_config)

    def test_invalid_config_key_exception(self):
        """
        Tests ``InvalidConfigKeyException`` representation.
        """
        exception_obj = InvalidConfigKeyException(
            "kwarg", {"type": "check_datatype", "kwarg": [{"column_name": "car_id"}, {"column_name": "customer_id"}]}
        )
        self.assertEqual(True, bool(str(exception_obj)))
        self.assertSetEqual({"kwarg"}, exception_obj.keys)

        exception_obj = InvalidConfigKeyException(
            ["kwarg", "k"], {"type": "check_datatype", "kwarg": [{"column_name": "car_id"}]}
        )
        self.assertSetEqual({"kwarg", "k"}, exception_obj.keys)
