import random
import string

from tests.data.base_generator import BaseGenerator


class StringConsistencyGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the string consistency check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_string_consistency/"

    def generate_data(self):
        # Create test data set for string consistency with 10 Chars
        num_rows = 20

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, num_empty_strings in enumerate([0, 1]):
            # Create random data with 2 columns
            data = [[x + 1, random_string()] for x in range(num_rows)]

            for i in random.sample(range(num_rows), num_empty_strings):
                data[i][1] = "QUITETOOLONGSTINGTOBECONSISTENT"

            df = self.spark.createDataFrame(data, ['id', 'txt'])

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if num_empty_strings == 0 else "fail"
            df.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_" + check_result)
