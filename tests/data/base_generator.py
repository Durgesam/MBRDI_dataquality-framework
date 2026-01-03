import random
import string


class BaseGenerator:
    """
        Base Generator class for data creation

        Args:
            output_folder: location to save the data

        """

    def __init__(self, output_folder, spark_session):
        self.output_folder = output_folder

        """
        Creation of Spark Session for all derived classes
        """

        self.spark = spark_session

    def generate_data(self):
        """
                Should be implemented by the derived class
        """
        raise NotImplementedError

    @staticmethod
    def random_string():
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
