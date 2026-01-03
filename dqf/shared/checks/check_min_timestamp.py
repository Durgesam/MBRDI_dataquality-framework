import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckMinTimestamp(ColumnLevelCheck):
    """
    Checks whether a timestamp is lower than the timestamp which is given.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns
        timestamp: timestamp to check

    Returns:
        `True` or `False`
    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        timestamp: Union[DateType, str],
    ) -> None:

        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.timestamp = timestamp

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        time_values = []

        for column in self.columns:
            min_timestamp_df = self.dataframe.filter(F.col(column) < self.timestamp)

            if min_timestamp_df.head() is None:
                time_values.append(True)
                self.log_success(f"There are no timestamps before {self.timestamp}", self.table, column)
            else:
                min_timestamp_count = min_timestamp_df.count()
                self.append_failed_data(columns=column, dataframe=min_timestamp_df, row_count=min_timestamp_count)
                time_values.append(False)
                msg = f"There are {min_timestamp_count} timestamps which are before {self.timestamp}"
                self.log_fail(msg, self.table, column, min_timestamp_df)
        return all(time_values)
