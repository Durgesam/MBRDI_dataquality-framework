import pathlib
from datetime import date
from typing import List, Optional, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckMaxTimestamp(ColumnLevelCheck):
    """
    Checks whether a timestamp is higher than the timestamp which is given.
    If no timestamp is given, the check will always compare to current timestamp during check

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns.
        timestamp: Timestamp to check. Default = current_timestamp.

    Returns:
        `True` or `False`
    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        timestamp: Optional[Union[DateType, str]] = date.today(),
    ) -> None:

        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)

        if timestamp == "" or timestamp == "today":
            self.timestamp = date.today()
        else:
            self.timestamp = timestamp

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        time_values = []

        for column in self.columns:
            max_timestamp_df = self.dataframe.filter(F.col(column) > self.timestamp)

            if max_timestamp_df.head() is None:
                time_values.append(True)
                self.log_success(f"There are no timestamps which are after {self.timestamp}", self.table, column)
            else:
                max_timestamp_count = max_timestamp_df.count()
                self.append_failed_data(columns=column, dataframe=max_timestamp_df, row_count=max_timestamp_count)
                time_values.append(False)
                msg = f"There are {max_timestamp_count} timestamps which are after {self.timestamp}"
                self.log_fail(msg, self.table, column, max_timestamp_df)
        return all(time_values)
