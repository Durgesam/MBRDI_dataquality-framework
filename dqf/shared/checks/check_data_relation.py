import pathlib
from typing import Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck


class CheckDataRelation(ColumnLevelCheck):
    """
    Checks whether values in column2 are always at least as large as values in column1

    Args:
       table: PySpark DataFrame or Path to the table.
       column1: Name of the column that should always have values less than or equal to column2
       column2: Name of the column that should always have values larger than or equal to column1

    Returns:
        `True` or `False`
    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame], column1: str, column2: str) -> None:
        super().__init__()

        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.column1 = column1
        self.column2 = column2

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        df2 = self.dataframe.withColumn(
            "Difference", F.when(self.dataframe[self.column2] >= self.dataframe[self.column1], 0).otherwise(1)
        )
        value = df2.groupby().sum().collect()[0][-1]

        if value > 0:
            failed_df = df2.filter(F.col('Difference') == 1).drop('Difference')
            failed_df_count = failed_df.count()
            self.append_failed_data(columns=f'{self.column1} & {self.column2}', dataframe=failed_df,
                                    row_count=failed_df_count)
            msg = f"There are {value} values coming after '{self.column2}' column"
            self.log_fail(msg, df=failed_df)
            return False

        self.log_success(f"There are no values coming after '{self.column2}' column")
        return True
