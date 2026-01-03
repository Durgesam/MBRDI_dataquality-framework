import pathlib
from typing import List, Tuple, Union

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

from dqf import main
from dqf.shared.base_check import ColumnLevelCheck
from dqf.shared.utils import handle_str_list_columns, map_datatype


class CheckDatatype(ColumnLevelCheck):
    """
    Checks whether the data type of the column is what we expected.

    Returns `True` if the data type is same.

    Args:
        table: PySpark DataFrame or Path to the table.
        columns: Column name
        d_type: Expected data type of the column

    Returns:
        `True` or `False`
    """

    def __init__(
        self,
        table: Union[str, pathlib.Path, DataFrame],
        columns: Union[List[str], Tuple[str], str],
        d_type: str,
    ) -> None:
        super().__init__()

        self.table = table
        self.dataframe = main.cache.get_dataframe(table)
        self.df_count = main.cache.get_count(self.dataframe)
        self.columns = handle_str_list_columns(columns)
        self.d_type, self.d_type_obj = map_datatype(d_type)

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))
        print(f'{self.df_count} rows processed.')
        self.logger.info(f'{self.df_count} rows processed.')

        result = []

        d_type_obj = self.d_type_obj
        def filter_unmatched_row(row_val):
            if d_type_obj == int:
                # Workaround, Since bool is a subclass of int, it will always pass for int type
                if isinstance(row_val, bool):
                    return True
            return not isinstance(row_val, d_type_obj)

        # User Defined Function (udf) to filter unmatched data type records
        udf_func = udf(filter_unmatched_row, returnType=BooleanType())
        for column in self.columns:
            column_result, expected_dtype, current_dtype = self._get_result(column)
            if not column_result:
                dtype_mismatch_df = self.dataframe.filter(udf_func(col(column)))
                dtype_mismatch_count = dtype_mismatch_df.count()
                self.append_failed_data(columns=column, dataframe=dtype_mismatch_df, row_count=dtype_mismatch_count)
            result.append(column_result)
            self._logger_message(column, column_result, expected_dtype, current_dtype)

        return all(result)

    def _get_result(self, column: str) -> Tuple[bool, str, str]:
        datatype = [dtype for column_name, dtype in self.dataframe.dtypes if column_name == column][0]
        current_dtype, current_dtype_obj = map_datatype(datatype)
        return current_dtype == self.d_type, self.d_type, current_dtype

    def _logger_message(self, column: str, result: bool, expected_dtype: str, current_dtype: str) -> None:
        if result:
            self.log_success(table=self.table, column=column)
        else:
            msg = f"Expected '{expected_dtype}' but got '{current_dtype}'"
            self.log_fail(msg, self.table, column)
