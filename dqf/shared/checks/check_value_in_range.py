import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckValueInRange(ColumnLevelCheck):
    """
    Checks whether the values of a column are in a certain range.
    Args:
       table: PySpark DataFrame or Path to the table.
       columns: List of columns.
       min_value: Minimum value of the range.
       max_value: Maximum value of the range.
    Returns:
        `True` or `False`
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            columns: Union[List[str], Tuple[str], str],
            min_value: int,
            max_value: int,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.min_value = min_value
        self.max_value = max_value

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        range_values = []

        for column in self.columns:
            range_df = self.dataframe.filter(~((F.col(column) >= self.min_value) & (F.col(column) <= self.max_value)))

            if range_df.head() is None:
                range_values.append(True)
                self.log_success(f"All values fall in range({self.min_value}, {self.max_value})", self.table, column)
            else:
                range_count = range_df.count()
                range_values.append(False)
                self.append_failed_data(columns=column, dataframe=range_df, row_count=range_count)
                msg = f"{range_count} values do not fall in range({self.min_value}, {self.max_value})"
                self.log_fail(msg, self.table, column, range_df)
        return all(range_values)
