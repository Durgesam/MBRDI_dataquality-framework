import random
import string

from tests.data.base_generator import BaseGenerator


class DataTypeGenerator(BaseGenerator):
    """
        class to generate data for the data-type check

        Args:
            output_folder: folder to save the generated files
        """

    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_datatype/"

    def generate_data(self):

        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, (expected_type, actual_type) in enumerate(
                [(int, int), (int, str), (str, str), (str, float), (float, float), (float, int)]):
            # Create random data with 1 column
            if actual_type == int:
                data = [[random.randint(-10, 10)] for _ in range(num_rows)]
            elif actual_type == float:
                data = [[random.random() * 15.1 - 7.7] for _ in range(num_rows)]
            elif actual_type == str:
                data = [[random_string()] for _ in range(num_rows)]

            df = self.spark.createDataFrame(data, ['column1'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if expected_type == actual_type else "fail"
            folder_name = self.output_folder + "/" + check_name + "_" + expected_type.__name__ + "_" + check_result
            df.coalesce(1).write.format('parquet').mode("overwrite").save(folder_name)
