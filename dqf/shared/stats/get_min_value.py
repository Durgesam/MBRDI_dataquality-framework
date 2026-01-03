import pathlib
from typing import List, Tuple, Union

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats
from dqf.shared.utils import handle_str_list_columns


class GetMinValue(BaseStats):
    """
        Calculates the minimum value in a column.

    Returns 'dictionary' having key as column name and value as the minimum value in that column.

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
            self.value[column] = main.cache.get_min_value(self.dataframe, column)

        return self.value
