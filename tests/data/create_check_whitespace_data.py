import random
import string

from tests.data.base_generator import BaseGenerator


class WhitespaceGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the whitespace check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_whitespace_data/"

    def generate_data(self):
        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, has_whitespaces in enumerate([False, True]):
            # Create random data with 2 columns
            data = [[random.randint(-10, 10), random_string()] for _ in range(num_rows)]

            df = self.spark.createDataFrame(data, ['id', 'txt'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if not has_whitespaces else "fail"
            folder_name = self.output_folder + "/" + check_name
            folder_name += " " if has_whitespaces else ""
            folder_name += "_" + check_result
            df.coalesce(1).write.format("parquet").mode("overwrite").save(folder_name)
