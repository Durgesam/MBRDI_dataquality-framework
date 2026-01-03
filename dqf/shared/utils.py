import json
import os
import pathlib
import re
import uuid
from datetime import date, datetime, timedelta
from functools import reduce
from typing import Dict, List, Set, Tuple, Type, Union, Any, Hashable

import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import TimestampType, BooleanType

from dqf import main
from dqf.shared.base_check import BaseCheck
from dqf.shared.base_stats import BaseStats


class DataFrameCache:
    """
    Caches loaded dataframes and can automatically add the datasource path to relative paths.
    There is one global cache object in main.py (Might change in the future).
    Can be extended to store results of computationally expensive operations.

    Example:
        main.setup_cache(config)  # config contains a key "datasource_path"
        cache = main.cache
        df1 = cache.get_dataframe("cars")  # Automatically adds datasource path from config
        df2 = cache.get_dataframe("cars")  # Returns the same dataframe again
        df3 = cache.get_dataframe(df2)  # Acts as a nop when given a DataFrame
    """

    def __init__(self, config: Dict = None):
        if config is None:
            self._datasource_path = pathlib.Path("")
            self.incremental_column = None
            self.incremental_filter = None
        else:
            self._datasource_path = pathlib.Path(config["datasource_path"])
            self.incremental_column = config.get("incremental_column")
            self.incremental_filter = config.get("incremental_filter")
        self._cached_stats = {}
        self._cached_stats = dict()

    def get_dataframe(self, dataframe: Union[str, pathlib.Path, DataFrame], on_dbfs: bool = False):
        """
        Get dataframe from the path.
        """
        if isinstance(dataframe, DataFrame):
            if self.incremental_column is not None and self.incremental_filter is not None:
                dataframe = self.apply_incremental_load_filter(
                    dataframe, self.incremental_column, self.incremental_filter
                )
            df = dataframe
        elif isinstance(dataframe, (str, pathlib.Path)):
            path = str(self.get_path(dataframe, on_dbfs))
            if path not in self._cached_stats:
                dataframe = read_data(path)
                if self.incremental_column is not None and self.incremental_filter is not None:
                    dataframe = self.apply_incremental_load_filter(
                        dataframe, self.incremental_column, self.incremental_filter
                    )
                self._cached_stats[path] = dataframe
            df = self._cached_stats[path]
        else:
            raise ValueError("dataframe should be str, Path or DataFrame")

        return df

    def get_path(self, path: Union[str, pathlib.Path], dbfs_prefix: bool = False) -> pathlib.Path:
        """
        Get path of the table.
        """
        if dbfs_prefix:
            return pathlib.Path("/dbfs") / self._datasource_path / path
        return self._datasource_path / path

    @staticmethod
    def apply_incremental_load_filter(
            dataframe: DataFrame, incremental_column: str, incremental_filter: str
    ) -> DataFrame:
        """Implements incremental load"""

        if incremental_column not in dataframe.columns:
            return dataframe

        column_types = dict(dataframe.dtypes)

        if column_types[incremental_column] == "string":
            dataframe = dataframe.withColumn(incremental_column, F.col(incremental_column).cast(TimestampType()))

        max_timestamp = date.today()

        incremental_filter = incremental_filter.strip()
        if incremental_filter == "previous_day":
            time_delta = timedelta(days=1)
        elif incremental_filter == "previous_week":
            time_delta = timedelta(days=7)
        elif incremental_filter == "previous_month":
            time_delta = relativedelta(months=1)
        else:
            # Other supported options: days, weeks, months, years and Date format pattern
            time_delta = get_time_delta_from_pattern(incremental_filter)
            if not time_delta:
                raise ValueError(f"Unsupported value for incremental load filter: '{incremental_filter}'")

        start_timestamp = max_timestamp - time_delta
        dataframe = dataframe.filter(F.col(incremental_column) >= start_timestamp)

        return dataframe

    def get_count(self, df: DataFrame) -> int:
        def count_calculation():
            try:
                empty = self._cached_stats[id(df)]["empty"]
                if empty:
                    return 0
            except KeyError:
                pass
            return df.count()

        return self._get_cached_value(count_calculation, df, "count")

    def get_empty(self, df: DataFrame) -> bool:
        def empty_calculation():
            try:
                count = self._cached_stats[id(df)]["count"]
                return count == 0
            except KeyError:
                return df.head() is None

        return self._get_cached_value(empty_calculation, df, "empty")

    def get_distinct_count(self, df: DataFrame, column: str) -> int:
        """ Returns the number of distinct values in the given columns """
        # Use distinct().count() instead of countDistinct() according to https://stackoverflow.com/a/40345377
        return self._get_cached_value(lambda: df.select(F.col(column)).distinct().count(), df, "distinct_count", column)

    def get_max_value(self, df: DataFrame, column: str):
        """ Returns the max value occurring in the given column """
        return self._get_cached_value(lambda: df.agg({column: 'max'}).collect()[0][0], df, "max", column)

    def get_min_value(self, df: DataFrame, column: str):
        """ Returns the min value occurring in the given column """
        return self._get_cached_value(lambda: df.agg({column: 'min'}).collect()[0][0], df, "min", column)

    def get_null_count(self, df: DataFrame, column: str, string_check: bool = True) -> int:
        """ Returns the number of null values in the given column """
        return self._get_cached_value(lambda: df.filter(filter_null(string_check, F.col(column))).count(), df,
                                      "null_count",
                                      column + '_dfq_str_chk' if string_check else column)

    def get_zeros_count(self, df: DataFrame, column: str) -> int:
        """ Returns the number of zeros in the given column """
        return self._get_cached_value(lambda: df.where(df[column] == 0).count(), df, "zeros_count", column)

    def get_max_values_count(self, df: DataFrame, column: str) -> int:
        """ Returns the number of maximum values in the given column"""
        return self._get_cached_value(lambda: df.where(df[column] == self.get_max_value(df, column)).count(), df, "max_values_count", column)

    def get_min_values_count(self, df: DataFrame, column: str) -> int:
        """ Returns the number of minimum values in the given column"""
        return self._get_cached_value(lambda: df.where(df[column] == self.get_min_value(df, column)).count(), df, "min_values_count", column)

    def _get_cached_value(self, calculation, df: DataFrame, *cache_keys: Hashable) -> Any:
        """
        Search for a value stored in the hierarchical cache by using the key sequence
            (df.cache_uuid, cache_keys[0], ..., cache_keys[N])
        or calculate the value and cache it, if it is not found.
        This method internally appends a member variable "cache_uuid" to the dataframe object to identify the same
        dataframe during subsequent calls again.
        @param calculation: A function calculating the value to be cached. Only called when result is not cached yet.
        @param df: Dataframe
        @param cache_keys: Sequence of cache keys for the hierarchical cache. Can be any hashable type
        @return: Any value
        """
        # Create a UUID for the cache and attach it to the dataframe, if not happened already
        try:
            cache_uuid = df.cache_uuid
        except AttributeError:
            cache_uuid = uuid.uuid4()
            df.cache_uuid = cache_uuid

        # Get the cached data for the dataframe
        if cache_uuid not in self._cached_stats:
            self._cached_stats[cache_uuid] = dict()
        cached_stats = self._cached_stats[cache_uuid]

        # Work down the cache hierarchy until the last cache key
        for arg in cache_keys[:-1]:
            if arg not in cached_stats:
                cached_stats[arg] = dict()
            cached_stats = cached_stats[arg]

        # Compute value on demand
        if cache_keys[-1] not in cached_stats:
            cached_stats[cache_keys[-1]] = calculation()

        return cached_stats[cache_keys[-1]]


def get_time_delta_from_pattern(incremental_filter):
    time_delta = None

    # Valid Patterns for incremental filter
    days = re.match(r'days=(\d+)', incremental_filter)
    weeks = re.match(r'weeks=(\d+)', incremental_filter)
    months = re.match(r'months=(\d+)', incremental_filter)
    years = re.match(r'years=(\d+)', incremental_filter)
    timestamp_pattern = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', incremental_filter)

    if days:
        time_delta = timedelta(days=int(days.group(1)))
    elif weeks:
        time_delta = timedelta(weeks=int(weeks.group(1)))
    elif months:
        time_delta = relativedelta(months=int(months.group(1)))
    elif years:
        time_delta = relativedelta(years=int(years.group(1)))
    elif timestamp_pattern:
        time_delta = relativedelta(year=int(timestamp_pattern.group(1)),
                                   month=(int(timestamp_pattern.group(2))),
                                   day=(int(timestamp_pattern.group(3))))
    return time_delta


def read_data(table_path: str) -> DataFrame:
    """
    Loading data based on the file type.

    Args:
        table_path: Path of the table.

    Logic:
        Spark load options method has been made configurable for 'CSV' format

    Returns:
        PySpark DataFrame
    """
    read_path = table_path[5:] if table_path.startswith("/dbfs") else table_path

    if main.FILE_FORMAT == "":
        # Automatically determine file format, e.g. for unit testing when no config is provided
        if list(pathlib.Path(table_path).rglob("**/*.parquet")):
            main.FILE_FORMAT = "parquet"
        elif list(pathlib.Path(table_path).rglob("**/*.orc")):
            main.FILE_FORMAT = "orc"
        elif list(pathlib.Path(table_path).rglob("**/*.csv")):
            main.FILE_FORMAT = "csv"
        else:
            raise RuntimeError(
                f"Could not automatically determine the file format since no orc, parquet or csv files "
                f"were found in path '{table_path}'"
            )

    spark = SparkSession.builder.getOrCreate()
    reader = reduce(lambda x, y: x.option(y[0], y[1]), main.SPARK_LOAD_OPTIONS.items(), spark.read)
    dataframe = reader.format(main.FILE_FORMAT).load(read_path)
    return dataframe


def get_dqf_class_from_name(dqf_class_name: str) -> Union[Type[BaseCheck], Type[BaseStats]]:
    """
    Checks whether the check is present or not.
    """
    import dqf.shared.checks as available_checks
    import dqf.shared.stats as available_stats

    dqf_class = dict()
    for module in (available_checks, available_stats):
        classes = {obj.__module__.split(".")[3]: obj for _, obj in vars(module).items() if isinstance(obj, type)}
        dqf_class.update(classes)

    try:
        return dqf_class[dqf_class_name]
    except KeyError:
        raise ValueError(f"Could not find a check or statistic with name '{dqf_class_name}'")


def handle_str_list_columns(columns: Union[List[str], Tuple[str, ...], str]) -> List[str]:
    """
    Handling `columns` argument so that it can be passed as a list or tuple or as a string.
    `columns` may be either
        - a string containing one column name
        - a list of strings containing column names
        - a tuple of strings containing column names
    """
    if isinstance(columns, str):
        result = [columns]
    elif isinstance(columns, (list, tuple)):
        result = list(columns)
    else:
        raise ValueError(f"Columns must be either a string, a tuple or a list, but got a {type(columns)}")

    return result


def map_datatype(datatype: str) -> Tuple[str, type]:
    """
    Map PySpart dtype to Python dtype and get the type object or tuple of type objects

    Args:
        datatype (str): PySpark dtype.

    Returns:
        Python dtype and dtype object.
    """
    map_type = {
        "bigint": "int",
        "tinyint": "int",
        "smallint": "int",
        "integer": "int",
        "int": "int",
        "string": "str",
        "str": "str",
        "bool": "bool",
        "boolean": "bool",
        "double": "float",
        "float": "float",
        "date": "timestamp",
        "timestamp": "timestamp",
    }

    map_type_obj = {
        "int": int,
        "str": str,
        "bool": bool,
        "float": float,
        "timestamp": (datetime, date)
    }

    dtype = "float" if "decimal" in datatype else map_type.get(datatype.lower(), None)
    dtype_obj = map_type_obj.get(dtype, None)
    return dtype, dtype_obj


def get_log_filename(file_path: Union[pathlib.Path, str], profile_name: str, on_dbfs: bool = True) -> str:
    """
    Get log filename.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{file_path}/dqf_log"

    if on_dbfs:
        file_path = f"/dbfs{file_path}"

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    filename = f"{file_path}/dqf_{profile_name}_{timestamp}.log"

    return filename


def get_configuration(config_name: str, on_dbfs: bool = True) -> str:
    """
    Get `install_path` or `check_results` path or `check_profiles` path.
    """

    install_path = main.get_install_path()

    config_path = f"{install_path}/config/dqf_config.json"

    if on_dbfs:
        config_path = f"/dbfs{config_path}"

    try:
        with open(config_path) as file_handle:
            config = json.load(file_handle)

        return config.get(config_name)
    except FileNotFoundError:
        print(f"{config_path} does not exist.")


def config_check(config: Dict) -> List[Tuple[bool, str]]:
    """
    Checks whether there are any typos, issues with the config file before running the DQF.

    Args:
        config: Configuration of CheckProject.
    Returns: A list of (result, string) pairs with check validation error messages
    """

    checks = config.get("checks")

    if checks is None:
        return []

    outcomes = []
    for check in checks:
        if "type" not in check:
            continue
        failed, information = config_third_level_keys_check(check)
        if failed:
            outcomes.append((failed, information))

    return outcomes


def config_third_level_keys_check(check: Dict) -> Tuple[bool, str]:
    """
    Checks for third level keys in the config.
    Returns a tuple (result, information), where
        result is True when a problem was found and False otherwise, and
        information is a string describing the problem
    """
    check_type = check["type"]

    # Load class or return an error is class does not exist
    try:
        check_class = get_dqf_class_from_name(check_type)
    except ValueError as error:
        return True, str(error)

    # Get arguments or return no error when no arguments exist
    try:
        keyword_args = check["kwargs"]
    except KeyError:
        return False, ""

    # Allowed and mandatory check arguments
    allowed_keys = set(check_class.get_arguments())
    mandatory_keys = set(check_class.get_arguments(False))

    user_args = set(keyword_args.keys())

    # Arguments given by the user which are not supported
    invalid_args = user_args.difference(allowed_keys)
    if len(invalid_args) > 0:
        return True, f"Found invalid arguments in check '{check_type}': {invalid_args}"

    # Required arguments not given by the user
    missing_args = mandatory_keys.difference(user_args)
    if len(missing_args) > 0:
        return True, f"Found missing arguments in check '{check_type}': {missing_args}"
    return False, ""


def is_check_valid(check_type: str, check: Dict) -> bool:
    """
    Checks if a check is valid.

    Args:
        check_type (str): Name of the check.
        check (Dict): Check.

    Returns:
        ``True`` if check is valid else ``False``.
    """
    return check_type is not None and check["kwargs"].keys() >= {"table", "columns"}


def is_keyword_args_table_path(keyword_args: Dict, table_path: str) -> bool:
    """
    Checks if ``kwargs`` and ``table`` arguments exist when check is defined in the config.

    Args:
        keyword_args (Dict): ``kwargs`` arguments of a check.
        table_path (str): table path of the check.

    Returns:
        ``True`` if both ``kwargs`` and ``table`` are given for a check else ``False``.
    """
    return keyword_args is None or table_path is None


def get_col_val(keyword_args: Dict, key: str) -> Set:
    """
    Get column names from ``kwargs``.

    Args:
        keyword_args (Dict): ``kwargs`` arguments of a check.
        key (str): key to get value from ``kwargs``.

    Returns:
        Set of values based on the ``key``.
    """
    return {keyword_args[key]} if isinstance(keyword_args[key], str) else set(keyword_args[key])


def get_tables_columns(checks: List) -> Tuple[List[Tuple[bool, str]], Dict, Dict]:
    """
    Returns a tuple of tables dict and columns dict.
    """

    dataframe_dict: Dict = {}
    column_dict: Dict = {}
    outcomes = []
    for check in checks:
        check_type = check.get("type")
        if not is_check_valid(check_type, check):
            continue
        keyword_args = check.get("kwargs")
        table_path = keyword_args.get("table")
        if is_keyword_args_table_path(keyword_args, table_path):
            continue
        path = main.cache.get_path(table_path) if isinstance(table_path, str) else table_path
        try:
            dataframe_dict[path] = main.cache.get_dataframe(table_path)

            for key in get_dqf_class_from_name(check_type).get_arguments():
                if "column" in key and key in keyword_args:
                    col_val = get_col_val(keyword_args, key)
                    column_dict[path] = column_dict.get(path, set()).union(col_val)
        except RuntimeError as error:
            outcomes.append((True, str(error)))

    return outcomes, dataframe_dict, column_dict


def check_table_columns(config: Dict) -> List[Tuple[bool, str]]:
    """
    Checks whether the given columns are there in the given tables or not.
    """
    checks = config.get("checks")

    if checks is None:
        return []

    outcomes = []
    if config.get("datasource_path") is not None:
        issues, dataframe_dict, column_dict = get_tables_columns(checks)
        outcomes.extend(issues)
        for path in dataframe_dict:
            expected_keys = set(dataframe_dict[path].columns)
            input_keys = column_dict[path]

            mismatched_keys = input_keys.difference(expected_keys)

            if len(mismatched_keys) > 0:
                path_name = path.name if isinstance(path, pathlib.Path) else path
                outcomes.append((True, f"Following columns {mismatched_keys} are not present in '{path_name}'."))

    return outcomes


def columns_with_dtype(data_type: str, column_names: str, column_names_dtypes: Dict[str, str]) -> List[str]:
    """
    Returns a list of columns with same dtype.

    Args:
    data_type (str): Datatype.
    column_names (str): Column names.
    column_names_dtypes (dict): Dict with column names and dtypes.
    """
    return [column for column in column_names if column_names_dtypes[column] == data_type]


def create_basic_check_profile(
        table_path: Union[pathlib.Path, str],
        data_format: str,
        profile_name: str = None,
        config_save_path: Union[str, pathlib.Path] = None,
        on_dbfs: bool = False,
        **kwargs,
) -> Dict:
    """
    Creates a basic CheckProfile based on the schema of the table.

    Args:
        table_path: Table path.
        data_format: Format of the table.
        profile_name: Profile name.
        config_save_path: Path for saving the config.
        on_dbfs: Whether on DBFS or not.
        kwargs: ``inferSchema`` and ``header`` for CSV ``data_format``.
    """
    datasource_path, table_name = os.path.split(os.path.normpath(table_path))

    if profile_name is None:
        profile_name = table_name

    spark = SparkSession.builder.getOrCreate()

    # Load dataframe and consider load options when using CSV files
    loader = spark.read.format(data_format.lower())
    if data_format.lower() == "csv":
        if "inferSchema" not in kwargs:
            kwargs["inferSchema"] = True
        if "header" not in kwargs:
            kwargs["header"] = True
        loader = loader.option("inferSchema", kwargs["inferSchema"]).option("header", kwargs["header"])

    dataframe = loader.load(table_path)

    column_names = dataframe.columns
    column_names_dtypes = dict(dataframe.dtypes)
    unique_dtypes = set(column_names_dtypes.values())

    config = {
        "datasource_path": datasource_path + "/",
        "profile_name": profile_name,
        "data_format": data_format,
        "checks": [
            {"type": "check_empty_df", "kwargs": {"table": table_name}},
            {"type": "check_null", "kwargs": {"table": table_name, "columns": column_names}},
            {
                "type": "check_empty_string",
                "kwargs": {
                    "table": table_name,
                    "columns": columns_with_dtype("string", column_names, column_names_dtypes),
                },
            },
        ],
    }

    if data_format.lower() == "csv":
        config["load_options"] = {"inferSchema": kwargs["inferSchema"], "header": kwargs["header"]}

    for column_type in unique_dtypes:
        config["checks"].append(
            {
                "type": "check_datatype",
                "kwargs": {
                    "table": table_name,
                    "columns": columns_with_dtype(column_type, column_names, column_names_dtypes),
                    "d_type": column_type,
                },
            }
        )

    if config_save_path is not None:
        if on_dbfs:
            config_save_path = f"/dbfs{config_save_path}"
        try:
            with open(config_save_path, "w") as config_file:
                json.dump(config, config_file)
        except (FileNotFoundError, PermissionError) as error:
            print(error)
            print("Saving the config failed")
            raise error

    return config


def get_bool_val(bool_val: Union[bool, str]) -> bool:
    # Converts string value "True" or "False", which is, case-insensitive to boolean
    if isinstance(bool_val, str):
        str_chk = bool_val.strip().lower()
        if str_chk == "true":
            return True
        elif str_chk == "false":
            return False
    elif isinstance(bool_val, bool):
        return bool_val

    # TODO - Check if ValueError can be raised
    print(f"'{bool_val}' is an invalid value for 'string_check'. "
          f"A boolean or case-insensitive 'true' or 'false' value is expected. Setting value to 'False', by default.")
    return False


def filter_null(string_check: bool, column):
    '''
    Filters null or "null" string from the dataset column based on the string_check variable.

    Args:
        string_check: if True, "null" strings are considered along with null values, else only null values
        column: Column object that is compared

    Returns: null filtered rows

    '''
    def is_null(entry):
        if entry is None or (string_check and entry == "null"):
            return True
        return False

    udf_func = F.udf(is_null, returnType=BooleanType())
    # User Defined Function (udf) to filter columns with null entry
    return udf_func(column)