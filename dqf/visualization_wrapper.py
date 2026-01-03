import json

import plotly.express as px
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from dqf.main import CheckProject


class WrapperChecks():

    def __init__(self):
        super().__init__()

    def visualization_schema(self, config):
        check_project = CheckProject(config)
        check_results = check_project.run()
        results = [[elt['type'], elt['datasource_path'], json.dumps(elt['kwargs']),
                    'pass' if elt['Result'] else 'fail', 'null', 'null'] for
                   elt in check_results]
        check_columns = ['test_case', 'source_path', 'table_path', 'test_status', 'record_count',
                         'table_partition']
        spark = SparkSession.builder.getOrCreate()
        check_df = spark.createDataFrame(data=results, schema=check_columns)
        check_df = check_df.withColumn("load_ts", F.current_timestamp())
        check_df = check_df.withColumn("id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        check_df.printSchema()
        check_df.show(truncate=False)
        check_df.write.format("delta").mode('append').saveAsTable("default.data_quality_framework")
        check_df = spark.table("data_quality_framework")

        # Enable Arrow-based columnar data transfers
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        # Convert the Spark DataFrame back to a pandas DataFrame using Arrow
        result_df = check_df.select("*").toPandas()
        fig = px.scatter(result_df, x="load_ts", y="test_case", color="test_status",
                         hover_data=['table_path', 'source_path'])

        fig.show()
        # rerun pipeline
