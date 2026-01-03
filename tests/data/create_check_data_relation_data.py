import random
import string

from tests.data.base_generator import BaseGenerator


class DataRelationGenerator(BaseGenerator):
    """
    class to generate data in parquet-format for the data-relation check

    Args:
        output_folder: folder to save the generated files
    """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_data_relation/"

    def generate_data(self):
        num_rows = 100

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, fails in enumerate([0, 1, 4]):
            # Create random data with 2 columns
            data = [[i + 823412, random_string()] for i in range(num_rows)]
            # Swap some rows
            for i in range(10, 10 + fails):
                tmp = data[i]
                data[i] = data[i - 1]
                data[i - 1] = tmp

            df = self.spark.createDataFrame(data, ['id', 'txt'])
            check_name = f"check{check_counter:02d}"
            check_result = "ok" if fails == 0 else "fail"
            folder_name = self.output_folder + "/" + check_name + "_" + check_result
            df.coalesce(1).write.format("parquet").mode("overwrite").save(folder_name)
