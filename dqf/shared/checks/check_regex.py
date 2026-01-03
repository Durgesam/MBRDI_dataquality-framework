import pathlib
from typing import List, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckRegex(ColumnLevelCheck):
    """
    Checks whether a column has any values that do not match the given pattern.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: List of columns.
        regex: String pattern.

    Returns:
        `True` or `False`
    """

    def __init__(
            self,
            table: Union[str, pathlib.Path, DataFrame],
            columns: Union[List[str], Tuple[str], str],
            regex: str,
            invert_result: bool = False,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.regex = regex
        self.invert_result = invert_result

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        results = []

        for column in self.columns:
            with_regex = self.dataframe.withColumn("_regex_match", F.col(column).rlike(self.regex))
            if self.invert_result:
                not_matching = with_regex.filter(F.col("_regex_match"))
            else:
                not_matching = with_regex.filter(~F.col("_regex_match"))

            if not_matching.head() is None:
                results.append(True)
                self.log_success("All rows match '{self.regex}'", self.table, column)
            else:
                regex_count = not_matching.count()
                self.append_failed_data(columns=column, dataframe=not_matching, row_count=regex_count)
                results.append(False)
                msg = f"There are {regex_count} rows not matching the pattern '{self.regex}'"
                self.log_fail(msg, self.table, column, not_matching)
        return all(results)
