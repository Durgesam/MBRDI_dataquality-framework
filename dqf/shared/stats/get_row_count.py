import pathlib
from typing import Union

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats


class GetRowCount(BaseStats):
    """
    Calculates the number of rows of the dataframe/table.

    Returns `int` which is the total number of rows in a table.

    Args:
        table: PySpark DataFrame or Path to the table.

    Returns:
        `int`
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame]
    ) -> None:
        super().__init__()
        self.dataframe = main.cache.get_dataframe(table)

    def run(self) -> int:
        self.logger.info("{:-^80}".format(self.__class__.__name__))
        self.value = main.cache.get_count(self.dataframe)
        return self.value
