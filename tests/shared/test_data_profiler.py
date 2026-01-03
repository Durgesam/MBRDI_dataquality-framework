# how to run pyspark tests : https://towardsdatascience.com/testing-pyspark-dataframe-transformations-d3d16c798a84
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from dqf.data_profiler import *
from .test_data_profiler_utils import *


class TestDataFrameProfiler(PySparkTestCase):

    def setUp(self) -> None:
        # TODO Add tests for different data types
        input_df = self.spark.createDataFrame(
            data=[[None, None, 1],
                  ['Bill', None, 2],
                  ['Bill', "John", 0]],
            schema=['colA', 'colB', 'colC'])

        self.dfs = DataFrameProfiler(input_df)

    def test_get_general_df_statistics(self):
        computed_result = self.dfs.get_general_df_statistics()
        expected_result = {
            "count": 3,
            "distinct_count": 3,
            "duplicates_count": 0
        }
        self.assertDictEqual(computed_result, expected_result)

    def test_get_columns_statistics(self):
        computed_result = self.dfs.get_columns_statistics(3)
        expected_result = {
            "distinct_count": {"colA": 2, "colB": 2, "colC": 3},
            "max_value": {"colA": "Bill", "colB": "John", "colC": 2},
            "min_value": {"colA": "Bill", "colB": "John", "colC": 0},
            "null_count": {"colA": 1, "colB": 2, "colC": 0},
            "zeros_count": {"colA": 0, "colB": 0, "colC": 1},
            "max_values_count": {"colA": 2, "colB": 1, "colC": 1},
            "min_values_count": {"colA": 2, "colB": 1, "colC": 1},
            "nulls_percentage": {"colA": 33, "colB": 66, "colC": 0},
            "zeros_percentage": {"colA": 0, "colB": 0, "colC": 33},
            "max_values_percentage": {"colA": 66, "colB": 33, "colC": 33},
            "min_values_percentage": {"colA": 66, "colB": 33, "colC": 33},
        }

        self.assertListEqual(list(sorted(computed_result.keys())), list(sorted(expected_result.keys())))
        for key in expected_result.keys():
            self.assertDictEqual(computed_result[key], expected_result[key])

    def test_get_metrics(self):
        computed_result = self.dfs.get_metrics(*self.dfs.get_statistics())
        expected_result = {
            "uniqueness": 100,
            "completeness": 67,
        }
        self.assertDictEqual(computed_result, expected_result)

    def test_empty_df(self):
        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('text', StringType(), True)
        ])
        df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)


class TestDataFrameMetrics(unittest.TestCase):

    def setUp(self) -> None:
        input_df_stats = {
            "count": 3,
            "distinct_count": 2,
            "duplicates_count": 1
        }

        input_columns_stats = {
            "distinct_count": {"colA": 2, "colB": 2, "colC": 3},
            "max_value": {"colA": "Bill", "colB": "John", "colC": 2},
            "min_value": {"colA": "Bill", "colB": "John", "colC": 0},
            "null_count": {"colA": 1, "colB": 2, "colC": 0},
            "zeros_count": {"colA": 0, "colB": 0, "colC": 1},
            "max_values_count": {"colA": 2, "colB": 1, "colC": 1},
            "min_values_count": {"colA": 2, "colB": 1, "colC": 1},
            "nulls_percentage": {"colA": 33, "colB": 66, "colC": 0},
            "zeros_percentage": {"colA": 0, "colB": 0, "colC": 33},
            "max_values_percentage": {"colA": 66, "colB": 33, "colC": 33},
            "min_values_percentage": {"colA": 66, "colB": 33, "colC": 33},
        }

        self.df_metrics = DataFrameMetrics(input_df_stats, input_columns_stats)

    def test_get_completeness(self):
        result = self.df_metrics.get_completeness()
        expected_result = 67
        self.assertAlmostEqual(result, expected_result)

    def test_get_uniqueness(self):
        result = self.df_metrics.get_uniqueness()
        expected_result = 67
        self.assertAlmostEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
