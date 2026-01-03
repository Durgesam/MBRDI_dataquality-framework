import pathlib
from typing import List, Tuple, Union, Optional

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns, get_bool_val, filter_null


class CheckNull(ColumnLevelCheck):
    """
    This scenario is related to check if any null values are existed or not with respect to columns.

    Returns `True` if the column of the dataframe contains no null values.

    Args:
        table: PySpark DataFrame or Path to the table.
        column: List of columns

    Returns:
        `True` or `False`

    """

    def __init__(self,
                 table: Union[str, pathlib.Path, DataFrame],
                 columns: Union[List[str], Tuple[str], str],
                 string_check: Optional[Union[bool, str]] = False) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.string_check = get_bool_val(string_check)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        null_values = []

        for column in self.columns:
            null_count = main.cache.get_null_count(self.dataframe, column, self.string_check)

            if null_count == 0:
                null_values.append(True)
                self.log_success("There are no null values", self.table, column)
            else:
                null_df = self.dataframe.filter(filter_null(self.string_check, F.col(column)))
                self.append_failed_data(columns=column, dataframe=null_df, row_count=null_count)
                null_values.append(False)
                msg = f"There are {null_count} null values"
                self.log_fail(msg, self.table, column, null_df)
        return all(null_values)
