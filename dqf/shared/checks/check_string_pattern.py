import pathlib
from typing import List, Optional, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckStringPattern(ColumnLevelCheck):
    """
    Checks whether string values start or end with certain pattern.

    Args:
       table: PySpark DataFrame or Path to the table.
       columns: List of columns.
       starts_with: Starts with pattern. Default: `""`
       ends_with: Ends with pattern. Default: `""`
       case_sensitive: Case sensitive or not. Default: `False`

    Returns:
        `True` or `False`

    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        starts_with: str = "",
        ends_with: str = "",
        case_sensitive: Optional[bool] = False,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.starts_with = starts_with
        self.ends_with = ends_with
        self.case_sensitive = case_sensitive

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        if self.starts_with == "" and self.ends_with == "":
            self.logger.info("Did not change default values for 'starts_with' and 'ends_with'.")
            return True

        start_pattern = f"^{self.starts_with}"
        end_pattern = f"{self.ends_with}$"

        if not self.case_sensitive:
            start_pattern = f"(?i){start_pattern}"
            end_pattern = f"(?i){end_pattern}"

        pattern_values = []

        for column in self.columns:
            pattern_df = self.dataframe.filter(
                ~((F.col(column).rlike(start_pattern)) & (F.col(column).rlike(end_pattern)))
            )

            if pattern_df.head() is None:
                pattern_values.append(True)
                self.log_success(f"All values start with '{self.starts_with}' and end with '{self.ends_with}'",
                                 self.table, column)
            else:
                pattern_count = pattern_df.count()
                self.append_failed_data(columns=column, dataframe=pattern_df, row_count=pattern_count)
                pattern_values.append(False)
                msg = f"There are {pattern_count} values that do not start with '{self.starts_with}' and end with " \
                      f"'{self.ends_with}'"
                self.log_fail(msg, self.table, column, pattern_df)
        return all(pattern_values)
