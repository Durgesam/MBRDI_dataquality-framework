import pathlib
from typing import Union

from pyspark.sql.dataframe import DataFrame

from dqf import main
from dqf.shared.base_check import DatabaseLevelCheck


class CheckForeign(DatabaseLevelCheck):
    """
    This scenario will check if foreign key column has same rows existed in primary key column or not
    Args:
       primary_table: Primary PySpark DataFrame or Path to the primary table.
       foreign_table: Foreign PySpark DataFrame or Path to the foreign table.
       primary_key_column: Primary key column name
       foreign_key_column: Foreign key column name

    Returns:
        `True` or `False`
    """

    def __init__(
        self,
        primary_table: Union[str, pathlib.Path, DataFrame],
        foreign_table: Union[str, pathlib.Path, DataFrame],
        primary_key_column: str,
        foreign_key_column: str,
    ) -> None:
        super().__init__()

        self.primary = primary_table
        self.foreign = foreign_table
        self.primary_table = main.cache.get_dataframe(primary_table)
        self.foreign_table = main.cache.get_dataframe(foreign_table)
        self.primary_key = primary_key_column
        self.foreign_key = foreign_key_column

    def replace_dataframe(self, check_config, dataframe, foreign_dataframe=None):
        check_config["kwargs"]["primary_table"] = dataframe
        check_config["kwargs"]["foreign_table"] = foreign_dataframe

    def run(self) -> bool:
        self.logger.info("{:-^80}".format(self.name()))

        df_res = self.foreign_table.join(
            self.primary_table,
            [self.foreign_table[self.foreign_key] == self.primary_table[self.primary_key]],
            how="left_anti",
        )

        if df_res.head() is None:
            self.log_success(
                f"All values in {self.foreign}.{self.foreign_key} are in {self.primary}.{self.primary_key}."
            )
            return True

        df_res_count = df_res.count()
        self.append_failed_data(columns=self.primary_key, dataframe=df_res, row_count=df_res_count)
        msg = f"Not all values in {self.foreign}.{self.foreign_key} are in {self.primary}.{self.primary_key}."
        self.log_fail(msg, df=df_res)
        return False
