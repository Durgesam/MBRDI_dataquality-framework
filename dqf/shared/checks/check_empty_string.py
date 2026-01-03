import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckEmptyString(ColumnLevelCheck):
    """
    Checks whether any column has empty strings.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns

    Returns:
        `True` or `False`
    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame], columns: Union[List[str], Tuple[str], str]) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        string_values = []

        for column in self.columns:
            empty_string_df = self.dataframe.filter(F.col(column) == "")

            if empty_string_df.head() is None:
                string_values.append(True)
                self.log_success("There are no empty strings", self.table, column)
            else:
                empty_string_count = empty_string_df.count()
                self.append_failed_data(columns=column, dataframe=empty_string_df, row_count=empty_string_count)
                string_values.append(False)
                msg = f"There are {empty_string_count} empty strings"
                self.log_fail(msg, self.table, column, empty_string_df)
        return all(string_values)
