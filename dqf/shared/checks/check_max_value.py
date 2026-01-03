import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckMaxValue(ColumnLevelCheck):
    """
    Checks whether a value is higher than the given value.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns
        value: value as limiter (int, double, etc.)

    Returns:
        `True` or `False`
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            columns: Union[List[str], Tuple[str], str],
            value: Union[int, float],
    ) -> None:

        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.value = value

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        max_values = []

        for column in self.columns:
            max_value = main.cache.get_max_value(self.dataframe, column)

            if max_value <= self.value:
                max_values.append(True)
                self.log_success(f"There are no entries which are larger than {self.value}", self.table, column)
            else:
                max_value_df = self.dataframe.filter(F.col(column) > self.value)
                max_value_count = max_value_df.count()
                self.append_failed_data(columns=column, dataframe=max_value_df, row_count=max_value_count)
                max_values.append(False)
                msg = f"There are {max_value_count} entries which are larger than {self.value}"
                self.log_fail(msg, self.table, column, max_value_df)
        return all(max_values)
