from abc import ABC, abstractmethod
from typing import Dict, Tuple, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from dqf.shared.stats import GetDataframeStatistics, GetColumnStatistics, GetColumnStatisticsInPercents


class DataProfiler(ABC):

    @abstractmethod
    def get_full_profile(self):
        pass

    @abstractmethod
    def get_statistics(self):
        pass

    @abstractmethod
    def get_metrics(self):
        pass


class DataFrameProfiler(DataProfiler):
    """
    Main class called by user to derive statistics and metrics of a given dataframe
    Example of usage:

        input_df =
        df_profiler = DataFrameProfiler(input_df)
        df_statistics, df_metrics = df_profiler.get_full_profile()

    """

    def __init__(self, df: DataFrame):
        self.df = df
        self.df_stats = {}

    def get_full_profile(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Collect the Dataframe statistics, columns statistics and dataframe metrics,
        convert them to dataframes and return them
        @return: Tuple with (dataframe_statistics, columns_statistics, dataframe_metrics)
        """

        spark = SparkSession.builder.getOrCreate()
        df_stats, columns_stats = self.get_statistics()
        metrics = self.get_metrics(df_stats, columns_stats)

        # Convert all dictionaries to dataframes
        # Start with the simple ones: df_stats and metrics
        header = list(df_stats.keys())
        df_stats_df = spark.createDataFrame([[df_stats[x] for x in header]], header)

        header = list(metrics.keys())
        metrics_df = spark.createDataFrame([[metrics[x] for x in header]], header)

        # Convert columns_stats to a dataframe looking like the following example
        # Column        | nulls_percentage  | zeros_percentage  | ...
        # -----------------------------------------------------------
        # col_name_1    | x1                | y1                | ...
        # col_name_2    | x2                | y2                | ...

        # Header / result df column names
        result_header_row = ["Column"] + list(columns_stats.keys())

        # First column in each row: names of columns with calculated metrics
        column_names = set()
        for i in columns_stats.values():
            column_names.update(i.keys())
        num_columns = len(column_names)
        column_names = list(column_names)

        # Size of result df
        result_size = len(result_header_row), num_columns

        # Create lists and fill in the data
        data: List[List[Any]] = [[None for _ in range(result_size[0])] for _ in range(result_size[1])]

        # First column
        for i in range(num_columns):
            data[i][0] = column_names[i]

        for row in range(result_size[1]):
            column_name = column_names[row]
            for col in range(1, result_size[0]):
                metric_name = result_header_row[col]
                try:
                    # TODO Use int where applicable
                    data[row][col] = str(columns_stats[metric_name][column_name])
                except KeyError:
                    pass

        # Create dataframe
        columns_stats_df = spark.createDataFrame(data, result_header_row)

        return df_stats_df, columns_stats_df, metrics_df

    def get_statistics(self) -> Tuple[Dict[str, int], Dict[str, Dict[str, Any]]]:
        df_stats = self.get_general_df_statistics()
        columns_stats = self.get_columns_statistics(df_stats["count"])
        return df_stats, columns_stats

    def get_general_df_statistics(self) -> Dict[str, int]:
        """
        Returns a dictionary with dataframe statistics
        containing the row count, distinct count and duplicate count
        """
        df_stats_generator = GetDataframeStatistics(self.df)
        df_stats = df_stats_generator.run()
        return df_stats

    def get_columns_statistics(self, count: int) -> Dict[str, Dict[str, Any]]:
        col_raw_stats_generator = GetColumnStatistics(self.df, self.df.columns)
        col_raw_stats = col_raw_stats_generator.run()
        percent_stats_generator = GetColumnStatisticsInPercents(self.df.columns, col_raw_stats, count)
        col_percent_stats = percent_stats_generator.run()
        columns_stats: Dict[str, Dict[str, Any]] = {**col_percent_stats, **col_raw_stats}
        return columns_stats

    def get_metrics(self, df_stats: Dict, columns_stats: Dict) -> Dict[str, int]:
        metrics_generator = DataFrameMetrics(df_stats, columns_stats)
        return metrics_generator.compute_all_metrics()


class DataFrameMetrics:
    """
    Derives data quality metrics from given statistics.
    Metrics in A22 are (as of August 2021):
        timeliness: All data records reflect the current status
        uniqueness: Each data record can be interpreted uniquely. There are no duplicates within the data records
        accuracy: All data records have the required precision
        validity: All data records correspond to a pre-defined, consistent scope of application
        integrity: All data records are free of contradictions. Includes aspects of the completeness, accuracy and consistency criteria
        consistency: A data record does not contradict itself or other data records
        reasonability: The data records must meet the notions and expectations of the information recipients with regard to terminology and structure
        completeness: A data record contains all of the necessary attributes

    Uniqueness and completeness are computed automatically.
    To define own rules, you can derive an own class and overwrite the respective methods.
    """

    def __init__(self, df_stats: Dict[str, int], columns_stats: Dict[str, Dict[str, Any]]):
        self.df_stats = df_stats
        self.columns_stats = columns_stats

    def compute_all_metrics(self) -> Dict[str, int]:
        """ Returns a dictionary containing the metrics names and values """
        metrics_funcs = {
            "timeliness": self.get_timeliness,
            "uniqueness": self.get_uniqueness,
            "accuracy": self.get_accuracy,
            "validity": self.get_validity,
            "integrity": self.get_integrity,
            "consistency": self.get_consistency,
            "reasonability": self.get_reasonability,
            "completeness": self.get_completeness,
        }

        all_metrics = {}
        if self.df_stats["count"] == 0:
            return all_metrics

        for metric, function in metrics_funcs.items():
            try:
                all_metrics[metric] = function()
            except NotImplementedError:
                pass

        return all_metrics

    def get_timeliness(self):
        """
        Timeliness can refer to and define a number of status descriptions for the data. In
        general, data can change over a certain time period, such as throughout the day
        (currency exchange rates), or during overnight processing. To determine timeliness,
        the time frame plays a key role, because otherwise old data could be processed, analyzed,
        and used
        """
        raise NotImplementedError

    def get_uniqueness(self) -> int:
        """
        Uniqueness measures the level at which a data record is in fact unique, i.e. there are no
        duplicates. For this purpose, the uniqueness of the data records must be clearly identifiable,
        such as with a customer number or other reference.
        The default implementation computes the percentage of distinct rows.
        """
        return int(round(self.df_stats["distinct_count"] / self.df_stats["count"] * 100, 0))

    def get_accuracy(self) -> int:
        """
        The criterion of accuracy refers to how well the data meets the required degree of precision.
        This criterion is often a bit tougher to measure if the required properties of an exact date
        are not available. Often, reliable reference data is used to compare and measure accuracy
        """
        raise NotImplementedError

    def get_validity(self) -> int:
        """
        To evaluate and successfully measure valid data, a scope must be defined. Validity depends
        on how consistent the data is with the defined validity values. Validity can be based on a
        collection of permitted values (such as from a reference table), a defined value area, or on
        rules. The validity of the data can also be viewed in a limited time frame, e.g. in the case
        of data that changes rapidly, at the time the data was analyzed
        """
        raise NotImplementedError

    def get_integrity(self) -> int:
        """
        The criterion of integrity is primarily concerned with coherence of the data and combines the
        basic principles of the other criteria – completeness, accuracy, and consistency.
        It looks at whether certain technical qualities and categories of the data are ensured and coherent.
        This is very important – for instance when data is stored and accessed in databases that must
        be reliable, trustworthy, and complete
        """
        raise NotImplementedError

    def get_consistency(self) -> int:
        """
        Consistency means that all data attributes in and/or outside of a data record are correct and
        do not contradict one another. A data record cannot contradict itself or other data records.
        To this end, the value area of the data that is stored must be accurate and correspond to the
        definition of the reference data. To evaluate the consistency, there are dependencies with
        other criteria such as validity, accuracy, and uniqueness
        """
        raise NotImplementedError

    def get_reasonability(self) -> int:
        """
        Reasonability or relevance is used to check how well certain data patterns meet the stated expectations.
        The criterion is not part of the classic criteria, and is used often to evaluate and analyze data
        """
        raise NotImplementedError

    def get_completeness(self) -> int:
        """
        The criterion of completeness refers to whether all necessary attributes for a data record are in place.
        Completeness can be checked and measured in various ways, such as using the total data volume and/or the
        number of columns or attributes. In order to measure correctly, we must first define what a complete
        data record looks like and what it contains.
        The default implementation computes the percentage of non-null values in the dataframe
        """
        result = 0
        for column_name, null_percentage in self.columns_stats["nulls_percentage"].items():
            result += 100 - null_percentage
        result = result // len(self.columns_stats["nulls_percentage"].keys())
        return result

    def get_score(self):
        raise NotImplementedError


class DataQualityScore:
    """
    0 - 100
    Superior, Excellent, Good, Bad, Very Bad, Ahh!
    """
    pass
