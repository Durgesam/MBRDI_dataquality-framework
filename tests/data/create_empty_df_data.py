import random
import string

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from tests.data.base_generator import BaseGenerator


class EmptyDFGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the empty dataframe check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_empty_df/"

    def generate_data(self):
        # Create test data set for check empty df
        num_rows = 100

        schema = StructType([
            StructField('id', IntegerType(), True),
            StructField('text', StringType(), True)
        ])

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, case in enumerate([0, 1]):
            # Create random data with 2 columns

            if case == 0:
                data = self.spark.sparkContext.emptyRDD()

            else:
                data = [[random.randint(-10, 10), random_string()] for _ in range(num_rows)]

            df = self.spark.createDataFrame(data, schema)

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if case > 0 else "fail"
            # check_result = str(num_empty_strings) + "_duplicates"
            df.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_" + check_result)
