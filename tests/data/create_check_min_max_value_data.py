import random


from tests.data.base_generator import BaseGenerator


class MinMaxGenerator(BaseGenerator):
    """
        class to generate data for the data-type check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_min_max_data/"

    def generate_data(self):

        num_rows = 100

        min_int = 4
        max_int = 750000
        min_float = 0.5
        max_float = 9999.9876

        # Create random data with 2 columns int and float

        data = [[random.randint(min_int, max_int), random.uniform(min_float, max_float)] for _ in range(num_rows)]

        min_data = [min_int, min_float]
        max_data = [max_int, max_float]

        data.append(min_data)
        data.append(max_data)

        df = self.spark.createDataFrame(data, ['column_int', 'column_float'])

        df.show()

        check_name = f"check_min_max"
        folder_name = self.output_folder + "/" + check_name
        df.coalesce(1).write.format('parquet').mode("overwrite").save(folder_name)