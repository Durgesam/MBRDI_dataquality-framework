import pathlib
from typing import Union

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import TableLevelCheck


class CheckEmptyDF(TableLevelCheck):
    """
    Checks whether the DataFrame is empty or not.

    Returns `True` if the DataFrame is not empty.

    Args:
        table: PySpark DataFrame or Path to the table.

    Returns:
        `True` or `False`
    """

    def __init__(self, table: Union[str, pathlib.Path, DataFrame]) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        not_empty = not main.cache.get_empty(self.dataframe)

        if not_empty:
            self.log_success(f"'{self.table}' is not empty", table=self.table)
        else:
            self.log_fail(f"'{self.table}' is empty", self.table)
        return not_empty
