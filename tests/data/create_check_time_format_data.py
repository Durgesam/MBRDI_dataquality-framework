import pathlib
import random
import string

from tests.data.base_generator import BaseGenerator


class TimeFormatGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the time-format check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_time_format/"

    def generate_data(self):
        # Create test data set for check_time_format
        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, (time_format, pass_check) in enumerate([("YYYYMMDD", True), ("YYYYMMDD", False),
                                                                   ("YYYYMMDDHHMMSS", True),
                                                                   ("YYYYMMDDHHMMSS", False)]):
            # Create random data with 2 columns
            data = [[random.randint(-10, 10), random_string()] for _ in range(num_rows)]

            df = self.spark.createDataFrame(data, ['id', 'txt'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if pass_check else "fail"
            folder_name = self.output_folder + "/" + check_name + "_" + time_format + "_" + check_result
            df.coalesce(1).write.format('parquet').mode("overwrite").save(folder_name)

            # Rename the file
            files = list(pathlib.Path(folder_name).glob("*.parquet"))
            for f in files:
                if pass_check:
                    time_str = {"YYYYMMDD": "20201202", "YYYYMMDDHHMMSS": "20201202152345"}[time_format]
                else:
                    time_str = {"YYYYMMDD": "201202", "YYYYMMDDHHMMSS": "20201202"}[time_format]
                f.rename(pathlib.Path(folder_name + f"/file_{time_str}.parquet"))
