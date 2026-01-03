import pathlib
import random
import string

from tests.data.base_generator import BaseGenerator


class FileFormatGenerator(BaseGenerator):
    """
        class to generate data for the file-format check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_file_format/"

    def generate_data(self):
        # Create test data set for check_incremental_larger
        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, (expected_type, actual_type) in enumerate([("parquet", "parquet"),
                                                                      ("parquet", "orc"),
                                                                      ("orc", "orc"),
                                                                      ("orc", "mysterious_file_format")]):
            # Create random data with 2 columns
            data = [[random.randint(-10, 10), random_string()] for _ in range(num_rows)]

            df = self.spark.createDataFrame(data, ['id', 'txt'])
            check_name = f"check{check_counter:02d}"
            check_result = "ok" if expected_type == actual_type else "fail"
            folder_name = self.output_folder + "/" + check_name + "_" + expected_type + "_" + check_result
            if actual_type in ["parquet", "orc"]:
                df.coalesce(1).write.format(actual_type).mode("overwrite").save(folder_name)
            else:
                pathlib.Path(folder_name).mkdir(exist_ok=True, parents=True)
                pathlib.Path(folder_name + "/file." + actual_type).write_text("Very useful file content")
