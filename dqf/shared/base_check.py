from typing import Optional

from pyspark.sql.dataframe import DataFrame

from dqf.shared.base import Base


class BaseCheck(Base):
    """
    Base Check class
    """

    def __init__(self, *args, **kwargs):
        """
        Class will directly initialized by derived classes
        """
        self.failed_data = []
        self.logging_message: str = ""

    def run(self) -> bool:
        """
        Should be implemented by the derived class
        """
        raise NotImplementedError

    @staticmethod
    def replace_dataframe(check_config, dataframe, foreign_dataframe=None):
        """
        Will be implemented by derived classes if necessary
        """
        pass

    def append_failed_data(self, columns, dataframe, row_count):
        """
        Append failed data dictionary containing necessary information for each failed check.

        Args:
            columns: column(s) that failed to pass the check
            dataframe: failed records in the dataframe
            row_count: number of records failed

        Returns: None

        """
        self.failed_data.append({"for_column": columns,
                                 "dataframe": dataframe,
                                 "row_count": row_count})

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    def log_success(self,
                    msg: Optional[str] = None,
                    table: Optional[str] = None,
                    column: Optional[str] = None):
        # Build the log message
        elems = ["Success", self.name()]
        if table is not None:
            elems.append(f"table {table}")
        if column is not None:
            elems.append(f"column {column}")
        if msg is not None:
            elems.append(msg)
        log_message = ": ".join(elems)

        # Log message
        self.logger.info(log_message)

        # Save for later reference
        if len(self.logging_message) == 0:
            self.logging_message = log_message
        else:
            self.logging_message += f"\n{log_message}"

    def log_fail(self,
                 msg: str,
                 table: Optional[str] = None,
                 column: Optional[str] = None,
                 df: Optional[DataFrame] = None):
        # Build the log message
        elems = ["Failed", self.name()]
        if table is not None:
            elems.append(f"table {table}")
        if column is not None:
            elems.append(f"column {column}")
        elems.append(msg)
        log_message = ": ".join(elems)

        # Log message
        self.logger.info(log_message)

        # Save for later reference
        if len(self.logging_message) == 0:
            self.logging_message = log_message
        else:
            self.logging_message += f"\n{log_message}"

        if df is not None:
            self.logger.info("You can see some failed rows below:")
            self.logger.info(str(df.head(10)))


class FileLevelCheck(BaseCheck):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TableLevelCheck(BaseCheck):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DatabaseLevelCheck(BaseCheck):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # replace dataframe for check_foreign_key directly implemented in check because too specific


class ColumnLevelCheck(BaseCheck):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def replace_dataframe(check_config, dataframe, foreign_dataframe=None):
        check_config["kwargs"]["table"] = dataframe
