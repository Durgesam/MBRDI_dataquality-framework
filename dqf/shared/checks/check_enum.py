import pathlib
from typing import List, Optional, Tuple, Union

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns


class CheckEnum(ColumnLevelCheck):
    """
    Check whether there are valid entries in the given columns.

    Args:
       table: PySpark DataFrame or Path to the table.
       columns: List of columns.
       enum: List of valid values.
       case_sensitive: Case sensitive or not. Default: `False`

    Returns:
        `True` or `False`

    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        enum: Union[List[float], Tuple[float], float, List[str], Tuple[str], str, List[bool], Tuple[bool], bool],
        case_sensitive: Optional[bool] = False,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.enum = enum
        self.case_sensitive = case_sensitive

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        enum_values = []

        if isinstance(self.enum, str):
            if not self.case_sensitive:
                self.enum = f"(?i){self.enum}"

        # Changing enum tuple to list for comparing column values to enum values provided
        if isinstance(self.enum, tuple):
            self.enum = list(self.enum)

        for column in self.columns:
            enum_df = self.dataframe.filter(~(F.col(column).isin(self.enum)))
            if enum_df.head() is None:
                enum_values.append(True)
                self.log_success(f"All values in the column are valid as provided in the enum list'",
                                 self.table, column)
            else:
                enum_count = enum_df.count()
                self.append_failed_data(columns=column, dataframe=enum_df, row_count=enum_count)
                enum_values.append(False)
                msg = f"There are {enum_count} invalid values that are not part of '{self.enum}'"
                self.log_fail(msg, self.table, column, enum_df)
        return all(enum_values)
