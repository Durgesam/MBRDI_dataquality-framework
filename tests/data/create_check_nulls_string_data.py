import random
import string

from tests.data.base_generator import BaseGenerator


class NullStringDataGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the null-data check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_nulls/csv/"

    def generate_data(self):
        # Create test data set for check_nulls
        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, num_empty_strings in enumerate([0, 4, 8]):
            # Create random data with 2 columns
            data = [[random.randint(-10, 10), random_string()] for _ in range(num_rows)]

            for count, i in enumerate(random.sample(range(num_rows), num_empty_strings)):
                if count > 4:
                    data[i][1] = "null"
                else:
                    data[i][1] = None

            df = self.spark.createDataFrame(data, ['id', 'txt'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if num_empty_strings == 0 else "fail"

            df.write.mode('overwrite').option("header", "true").option("inferSchema", "true").csv(
                self.output_folder + "/" + check_name + "_" + check_result)
