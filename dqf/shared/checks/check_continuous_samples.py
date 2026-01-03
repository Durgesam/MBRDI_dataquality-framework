import pathlib
from typing import List, Optional, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckContinuousSamples(ColumnLevelCheck):
    """
    Checks whether there are only continuous samples in a dataframe.
    First, the DF is partitioned on the `sample_key` columns and sorted by `column1`.
    Then, for each partition and each row, the following must hold:
    - The `column2` value of row n is equal to (or less than) `column1` value of the next row n+1
    - The `column1` value of row n is equal to (or larger than) the `column2` value of the previous row n-1
    If this is the case, the check returns True, otherwise it returns False.

    Args:
        `table`: PySpark DataFrame or Path to the table.
        `sample_key`: List of columns used to partition the data into separate sample partitions
        `column1`: Column that should equal `column2` of the next row. `column1` is also used for sorting
        `column2`: Column that should equal `column1` of the previous row
        `strict`: Default is True. If False, `column1` values may also be larger than `column2` values

    Returns:
        `True` or `False`
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            sample_key: Union[List[str], Tuple[str], str],
            column1: str,
            column2: str,
            strict: Optional[bool] = True,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.sample_key = handle_str_list_columns(sample_key)
        self.column1 = column1
        self.column2 = column2
        self.strict = strict

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        def show_error(df: DataFrame, c1: str, c2: str, txt: str):
            """
            Logs the error message.
            """
            count = df.count()
            self.append_failed_data(columns=f'column1 {c1} & column2 {c2}', dataframe=df, row_count=count)
            msg = f"There are {count} rows in '{self.table}' where the value in '{c1}' does not match the value " \
                  f"in {c2} in the {txt} row"
            self.log_fail(msg, df=df)

        channel_window_asc = Window.partitionBy(self.sample_key).orderBy(F.col(self.column1))
        channel_window_desc = Window.partitionBy(self.sample_key).orderBy(F.col(self.column1).desc())

        df2 = self.dataframe.withColumn("_desc_idx", F.row_number().over(channel_window_desc))
        df2 = df2.withColumn("_asc_idx", F.row_number().over(channel_window_asc))
        df2 = df2.withColumn("_prev_end", F.lag(self.column2).over(channel_window_asc))
        df2 = df2.withColumn("_next_start", F.lead(self.column1).over(channel_window_asc))

        if self.strict:
            # Check whether the start value is the same as the end value of the previous sample
            # Fail condition: (start != previous end) or (previous end is null but not the first sample)
            cond = (F.col(self.column1) != F.col("_prev_end")) | (F.isnull("_prev_end") & (F.col("_asc_idx") != 1))
        else:
            # Check whether the start value is the same as or larger than the end value of the previous sample
            # Fail condition: (start < previous end) or (previous end is null but not the first sample)
            cond = (F.col(self.column1) < F.col("_prev_end")) | (F.isnull("_prev_end") & (F.col("_asc_idx") != 1))
        df3 = df2.filter(cond)
        if df3.head() is not None:
            show_error(df3, self.column1, self.column2, "previous")
            return False

        if self.strict:
            # Check whether the end value is the same as the start value of the next sample
            # Condition: (end != next start) or (next start is null but not the last sample)
            cond = (F.col(self.column2) != F.col("_next_start")) | (F.isnull("_next_start") & (F.col("_desc_idx") != 1))
        else:
            # Check whether the end value is the same as or smaller than the start value of the next sample
            # Condition: (end > next start) or (next start is null but not the last sample)
            cond = (F.col(self.column2) > F.col("_next_start")) | (F.isnull("_next_start") & (F.col("_desc_idx") != 1))
        df4 = df2.filter(cond)
        if df4.head() is not None:
            show_error(df4, self.column2, self.column1, "following")
            return False

        self.log_success(table=self.table)

        return True
