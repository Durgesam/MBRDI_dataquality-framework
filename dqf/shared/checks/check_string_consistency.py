import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckStringConsistency(ColumnLevelCheck):
    """
    This scenario is related to set string length of column.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns
        string_length: String length

    Return :
        `True` or `False`
    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        string_length: int = 100, method: str = "exact"
    ):
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.string_length = string_length
        self.method = method

    @staticmethod
    def get_dataframe_from_method(check_df: DataFrame, column: str,
                                  calc_method: str, string_length: int) -> DataFrame:

        if calc_method == "min":
            return check_df.filter(F.length(F.col(column)) < string_length)
        elif calc_method == "max":
            return check_df.filter(F.length(F.col(column)) > string_length)
        else:
            return check_df.filter(F.length(F.col(column)) != string_length)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        string_chars = []

        for column in self.columns:
            inconsistency_df = self.get_dataframe_from_method(self.dataframe, column, self.method, self.string_length)

            if inconsistency_df.head() is None:
                string_chars.append(True)
                self.log_success(f"There are no inconsistent strings", self.table, column)
            else:
                mismatches = inconsistency_df.count()
                string_chars.append(False)
                self.append_failed_data(columns=column, dataframe=inconsistency_df, row_count=mismatches)
                msg = f"There are {mismatches} inconsistent strings in '{self.table}'"
                self.log_fail(msg, self.table, column, inconsistency_df)
        return all(string_chars)
