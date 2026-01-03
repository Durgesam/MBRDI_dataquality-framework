import pathlib
from typing import List, Tuple, Union

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_stats import BaseStats
from dqf.shared.utils import handle_str_list_columns


class GetMedianValue(BaseStats):
    """
    Calculates the median value in a column.

    Returns dictionary having key as column name and value as the corresponding median

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: Column name

    Returns:
        'dictionary'
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            columns: Union[List[str], Tuple[str], str]
    ) -> None:

        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.columns = handle_str_list_columns(columns)

    def run(self) -> dict:
        self.logger.info("{:-^80}".format(self.__class__.__name__))

        for column in self.columns:
            if str(self.dataframe.schema[column].dataType) in ['IntegerType', 'LongType', 'FloatType']:
                median_list_val = self.dataframe.approxQuantile(column, [0.5], relativeError=0)
                self.value[column] = median_list_val[0]
            else:
                self.logger.info(f" {column} should be either IntegerType, LongType or FloatType")

        return self.value
