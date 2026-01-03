import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckUnique(ColumnLevelCheck):
    """
    Checks whether the given columns contain only unique values or not.

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

        unique_values = []

        row_count = self.df_count
        for column in self.columns:
            distinct_count = main.cache.get_distinct_count(self.dataframe, column)
            duplicate_count = row_count - distinct_count
            if duplicate_count == 0:
                unique_values.append(True)
                self.log_success("There are no duplicates", self.table, column)
            else:
                duplicate_df = self.dataframe.groupBy(self.dataframe[column]).count().where(F.col("count") > 1)
                self.append_failed_data(columns=column, dataframe=duplicate_df, row_count=duplicate_count)
                unique_values.append(False)
                msg = f"There are {duplicate_count} duplicates"
                self.log_fail(msg, self.table, column, duplicate_df)
        return all(unique_values)
