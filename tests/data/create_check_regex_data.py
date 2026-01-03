import random
import string

from tests.data.base_generator import BaseGenerator


class RegexGenerator(BaseGenerator):
    """
    class to generate data for the regex samples check

    Args:
        output_folder: folder to save the generated files
    """

    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_regex_data/"

    def generate_data(self):
        num_rows = 10000

        def random_string():
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(0, 10)))

        data = []
        for i in range(num_rows):
            txt = random_string()
            txt += random.choice(["dog", "eats", "mouse"])
            txt += random_string()
            data.append([i, txt])

        df = self.spark.createDataFrame(data, ['id', 'string'])
        df.show()

        check_name = f"check_regex"
        folder_name = self.output_folder + "/" + check_name
        df.coalesce(1).write.format('parquet').mode("overwrite").save(folder_name)
