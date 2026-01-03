import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckUniqueTuple(ColumnLevelCheck):
    """
    This scenario is related to check whether the given columns contain only unique values or not.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns

    Returns:
        `True` or `False`
    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame], columns: Union[List[str], Tuple[str]]) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        temp_dqf_col_name = 'dqf_tup_count_temp'
        # Workaround: Replace 'count' column name to temp name, as group by creates a count column of its own.
        cols = [temp_dqf_col_name if col == 'count' else col for col in self.columns]
        df = self.dataframe.withColumnRenamed('count', temp_dqf_col_name)

        duplicate_counts_df = df.groupBy(cols).count().where('count > 1')
        duplicate_count = duplicate_counts_df.select(F.sum('count')).collect()[0][0]

        if not duplicate_count:
            self.log_success(f"There are no duplicates in combination of columns '{self.columns}'", self.table)
            return True
        else:
            # Condition passed when joining two dataframes.
            # It ensures even null values are valid entries for join.
            cond = None
            for count, column in enumerate(cols):
                if count == 0:
                    cond = df[column].eqNullSafe(duplicate_counts_df[column])
                else:
                    cond = df[column].eqNullSafe(duplicate_counts_df[column]) & cond

            duplicates_df = df.alias("first_df").join(duplicate_counts_df,
                                                      cond).select("first_df.*").withColumnRenamed(temp_dqf_col_name,
                                                                                                   'count')

            self.append_failed_data(columns=self.columns, dataframe=duplicates_df, row_count=duplicate_count)
            msg = f"There are {duplicate_count} duplicates in combination of columns '{self.columns}'"
            self.log_fail(msg, self.table, self.columns)
            return False
