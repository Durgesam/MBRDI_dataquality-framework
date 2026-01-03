import random
import string

from tests.data.base_generator import BaseGenerator


class IncrementalLargerGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the incremental-larger check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_incremental_larger/"

    def generate_data(self):
        # Create test data set for check_incremental_larger
        num_rows = 1000

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, is_incremental_larger in enumerate([True, False]):

            # Create data multiple times for the same directory
            for file_id in range(2):
                # Create random data with 2 columns
                n = num_rows
                if file_id > 0:
                    n += 500 if is_incremental_larger else -500
                data = [[random.randint(-10, 10), random_string()] for _ in range(n)]

                df = self.spark.createDataFrame(data, ['id', 'txt'])

                check_name = f"check{check_counter:02d}"
                check_result = "fail" if is_incremental_larger else "ok"
                mode = "overwrite" if file_id == 0 else "append"
                df.coalesce(1).write.format('parquet').mode(mode).save(
                    self.output_folder + "/" + check_name + "_" + check_result)
