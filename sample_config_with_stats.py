import dqf
import pyspark
from dqf.main import CheckProject
from pprint import pprint
from dqf.shared import utils
from pyspark.sql import SparkSession, dataframe

# spark = SparkSession \
#         .builder \
#         .appName("TestValidation") \
#         .getOrCreate()

#WITH DELTA TEST
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("Test DQF Utilites") \
    .master("spark://spark-master:7077") \

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

#######

from dqf.shared.checks import *

def test_single_check():

    tablePath = "/opt/spark-data/tests/data_generation/check_continuous_samples_data/check_continuous_samples"

    df = spark.read.format("parquet").load(tablePath)
    df.show(2000, False)

    check = CheckContinuousSamples(df, ['channel_id'], "start_ok_not_strict", "end_ok", strict=True)

    result = check.run()


    print(check.failed_data)

    check.failed_data[0]["dataframe"].show()

    spark.stop()


def test_with_config():

    config = {
    "datasource_path": "/opt/spark-data/tests/TestData/multi_data_automation/",
    "data_format": "delta",
    "profile_name": "NewProfile_withStats",
    "checks": [
        {
        "type": "check_unique",
        "kwargs": {
            "table": "customers",
            "columns": ["first_name", "last_name"]
            }
        },
        {
        "type": "check_max_value",
        "kwargs": {
            "table": "customers",
            "columns": ["car_value"],
            "value":999939.78
            }
        }
    ],
    "stats": [
        {
      "type": "get_duplicate_count",
      "kwargs": {
        "table":"customers",
        "columns":["first_name", "last_name"]
      }
    },
    {
      "type": "get_distinct_count",
      "kwargs": {
        "table":"customers",
        "columns":["first_name", "last_name"]
      }
    },
    {
      "type": "get_row_count",
      "kwargs": {
        "table":"customers"
      }
    },
    {
      "type": "get_max_value",
      "kwargs": {
        "table":"customers",
        "columns":["car_value"]
      }
    }
    ]
    }

    checkProj = CheckProject(config=config, on_dbfs=False, write_results=False, html_write_report=True)

    checkProj.run()

    return checkProj


df = spark.read.format("parquet").load("/opt/spark-data/tests/TestData/multi_data_automation/customers")

check = test_with_config()

check.show_failed_data()

# print("####RESULTS OF Checks#########")
# pprint(check.check_results)

# fn_data = check.check_results[0]["failed_data"][0]["dataframe"]
# print(f'Row Count of failed data = {fn_data.count()}')


# ln_data = check.check_results[0]["failed_data"][1]["dataframe"]
# print(f'Row Count of failed data = {ln_data.count()}')

# print("####RESULTS OF STATS#########")
# pprint(check.stats_results)


print("END OF PROGRAMM")