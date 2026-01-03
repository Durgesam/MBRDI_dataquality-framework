import random
import string

from tests.data.base_generator import BaseGenerator


class UniqueValuesGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the unique-values check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_unique_values/"

    def generate_data(self):
        # Create test data set for check_unique_values
        num_rows = 100
        random.seed(42)

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, num_empty_strings in enumerate([0, 2, 4]):
            # Create random data with 2 columns
            # For check_unique_tuple, data extended with two static colums
            data = [[random.randint(-10, 10), random_string(), "Static_Key1", "Static_Key2"] for _ in range(num_rows)]

            for i in random.sample(range(num_rows), num_empty_strings):
                data[i][1] = "SOMECAT"

            df = self.spark.createDataFrame(data, ['id', 'txt', 'static_1', 'static_2'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if num_empty_strings == 0 else "fail"
            df.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_" + check_result)
