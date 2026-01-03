import random
import string

from tests.data.base_generator import BaseGenerator


class ForeignKeyGenerator(BaseGenerator):
    """
        class to generate data in parquet-format for the foreign-key check

        Args:
            output_folder: folder to save the generated files
        """
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_foreign_key/"

    def generate_data(self):
        # Create test data set for check_nulls
        num_rows = 10

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

        for check_counter, num_empty_strings in enumerate([0, 1]):
            # Create random data with 2 columns
            data = [[x + 1, random_string()] for x in range(num_rows)]
            data2 = [[x + 100, x + 1] for x in range(num_rows)]

            for i in random.sample(range(num_rows), num_empty_strings):
                data2[i][1] = 13

            df = self.spark.createDataFrame(data, ['id', 'txt'])
            print("Foreign Key DataSet 1")
            df.show()

            df2 = self.spark.createDataFrame(data2, ['id', 'foreign'])
            print("Foreign Key DataSet2")
            df2.show()

            check_name = f"check{check_counter:02d}"
            check_result = "ok" if num_empty_strings == 0 else "fail"
            df.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_primary_" + check_result)

            df2.coalesce(1).write.format('parquet').mode("overwrite").save(
                self.output_folder + "/" + check_name + "_foreign_" + check_result)
