import pathlib
from typing import List, Tuple, Union, Dict, Any

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats
from dqf.shared.utils import handle_str_list_columns


class GetDataframeStatistics(BaseStats):
    """
        Calculates general statistics for the given dataframe.

    Returns nested 'dictionary' having key as a parameter name and value as the parameter value.

    Args:
        table: PySpark DataFrame or Path to the table.

    Returns:
        'dictionary'

    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
    ) -> None:
        super().__init__()

        self.dataframe = main.cache.get_dataframe(table)
        """self.parameters_from_cache = {
            "count": main.cache.get_count,
            "distinct_count": main.cache.get_distinct_count,
            "duplicates_count": self.get_duplicates_count,
        }"""

        self.df_stats: Dict[str, int] = {}

    def run(self) -> Dict[str, int]:
        self.logger.info("{:-^80}".format(self.__class__.__name__))

        self.df_stats["count"] = main.cache.get_count(self.dataframe)
        self.df_stats["distinct_count"] = main.cache.get_distinct_count(self.dataframe, "*")
        self.df_stats["duplicates_count"] = self.get_duplicates_count()

        return self.df_stats

    def get_duplicates_count(self) -> int:
        return self.df_stats["count"] - self.df_stats["distinct_count"]


class GetColumnStatistics(BaseStats):
    """
        Calculates statistic parameters for the given columns of the dataframe.

    Returns nested 'dictionary' having 1st level key as a parameter name and 2nd level key as a column name
    and value as the parameter value in that column.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: Column names
        parameters: List of parameters: if argument is not given

    Returns:
        'dictionary'

    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            columns: Union[List[str], Tuple[str], str],
            parameters: List[str] = []
    ) -> None:
        super().__init__()

        self.dataframe = main.cache.get_dataframe(table)
        self.columns = handle_str_list_columns(columns)
        self.parameters_from_cache = {
            # "count": self.get_count,
            "distinct_count": main.cache.get_distinct_count,
            "max_value": main.cache.get_max_value,
            "min_value": main.cache.get_min_value,
            "null_count": main.cache.get_null_count,
            "zeros_count": main.cache.get_zeros_count,
            "max_values_count": main.cache.get_max_values_count,
            "min_values_count": main.cache.get_min_values_count,
            # "categorical": None,
            # "possible_identifier": None,
            # "outlier": None,
            # "skew": None,
        }
        if parameters:
            self.parameters = parameters
        else:
            self.parameters = self.parameters_from_cache.keys()
        self.stats: Dict[str, Dict[str, Any]] = {}

    def run(self) -> Dict[str, Dict[str, Any]]:
        self.logger.info("{:-^80}".format(self.__class__.__name__))

        for parameter in self.parameters:
            self.calculate_values_of_parameter_for_all_columns(parameter)
        return self.stats

    def calculate_values_of_parameter_for_all_columns(self, parameter: str):
        self.stats[parameter] = {}
        try:
            for column in self.columns:
                self.stats[parameter][column] = self.parameters_from_cache[parameter](self.dataframe, column)
        except KeyError:
            print(f"Parameter {parameter} is not found!!!")


class GetColumnStatisticsInPercents(BaseStats):
    """
        Calculates percentages of raw parameters for the given columns of the dataframe.

    Returns nested 'dictionary' having 1st level key as a parameter name and 2nd level key as a column name
    and value as the parameter value in that column.

    Args:
        columns: Column names
        raw_stats: Dictionary of raw parameters and their values derived from GetColumnStatistics class
        count: Number of rows in dataframe
    Returns:
        'dictionary'

    """

    def __init__(
            self,
            columns: Union[List[str], Tuple[str], str],
            raw_stats: dict,
            count: int
    ) -> None:
        super().__init__()
        self.columns = handle_str_list_columns(columns)
        self.raw_stats = raw_stats
        self.count = count
        self.percent_stats: Dict[str, Dict[str, int]] = {}
        self.percent_vs_raw_stats = {
            "nulls_percentage": "null_count",
            "zeros_percentage": "zeros_count",
            "max_values_percentage": "max_values_count",
            "min_values_percentage": "min_values_count",
        }

    def run(self) -> Dict[str, Dict[str, int]]:
        self.logger.info("{:-^80}".format(self.__class__.__name__))
        for percent_parameter, raw_parameter in self.percent_vs_raw_stats.items():
            self.calculate_values_of_parameter_for_all_columns(percent_parameter, raw_parameter)
        return self.percent_stats

    def calculate_values_of_parameter_for_all_columns(self, percent_parameter: str, raw_parameter: str):
        self.percent_stats[percent_parameter] = {}
        for column in self.columns:
            if self.count == 0:
                value = 0
            else:
                value = int(self.raw_stats[raw_parameter][column] / self.count * 100)
            self.percent_stats[percent_parameter][column] = value
