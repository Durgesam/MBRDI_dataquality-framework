import pathlib
from typing import List, Tuple, Union, Optional

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats
from dqf.shared.utils import handle_str_list_columns, get_bool_val


class GetNullCount(BaseStats):
    """
    Calculates number of null values in a specific column.

    Returns 'dictionary' having key as column name and value as the number of null values in that column.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: Column name

    Returns:
        'dictionary'

    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame], columns: Union[List[str], Tuple[str], str],
                 string_check: Optional[Union[bool, str]] = False) -> None:
        super().__init__()
        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.columns = handle_str_list_columns(columns)
        self.string_check = get_bool_val(string_check)

    def run(self) -> dict:
        self.logger.info("{:-^80}".format(self.__class__.__name__))

        for column in self.columns:
            self.value[column] = main.cache.get_null_count(self.dataframe, column, self.string_check)

        return self.value
