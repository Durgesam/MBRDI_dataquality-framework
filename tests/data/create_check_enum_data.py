import random
import string

from tests.data.base_generator import BaseGenerator


class EnumDataGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the enum-data check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_enum/parquet/"

    def generate_data(self):
        # Create test data set for check_nulls
        num_rows = 100

        for check_counter, num_empty_strings in enumerate([0, 1, 4]):
            # Create random data with 2 columns
            data = [[1, "string"] for _ in range(num_rows)]

            for i in random.sample(range(num_rows), num_empty_strings):
                data[i][0] = 2
                data[i][1] = "str"

            df = self.spark.createDataFrame(data, ['id', 'txt'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if num_empty_strings == 0 else "fail"
            df.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_" + check_result)
