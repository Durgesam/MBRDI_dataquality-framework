import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats
from dqf.shared.utils import handle_str_list_columns


class GetDuplicateCount(BaseStats):
    """
    Calculates number of duplicates in a specific column.

    Returns 'dictionary' having key as column name and value as the total number of duplicate values.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: Column name

    Returns:
        'dictionary'
    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame], columns: Union[List[str], Tuple[str], str]) -> None:
        super().__init__()
        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.columns = handle_str_list_columns(columns)

    def run(self) -> dict:
        self.logger.info("{:-^80}".format(self.__class__.__name__))

        for column in self.columns:
            duplicate_df = self.dataframe.groupBy(self.dataframe[column]).count().where(F.col("count") > 1)
            self.value[column] = duplicate_df.agg({"count": "sum"}).collect()[0][0]
            self.data[column] = duplicate_df

        return self.value
