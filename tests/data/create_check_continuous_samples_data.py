import random

from tests.data.base_generator import BaseGenerator


class ContinuousSamplesGenerator(BaseGenerator):
    """
    class to generate data for the continuous samples check

    Args:
        output_folder: folder to save the generated files
    """

    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "check_continuous_samples_data/"

    def generate_data(self):

        num_channels = 100

        # Columns:
        # channel: Used for partitioning
        # start_ok: Start values for the test which shall pass
        # end_ok: End values for the test which shall pass
        # start_fail: Start values for the test which shall fail
        # end_fail: End values for the test which shall fail

        data = []
        for channel_idx in range(num_channels):
            start = random.randint(0, 10000)
            for sample_idx in range(random.randint(1, 100)):
                end = start + random.randint(2, 100)
                data.append([channel_idx, start, end, start, end, start + random.choice((0, 1))])
                start = end

        n = len(data)

        # Set an invalid start value
        while True:
            # Continue until a row which is not the first one of a channel_id is selected
            i = random.randint(1, n - 1)
            if data[i][0] != data[i - 1][0]:
                continue
            data[i][3] += random.randint(1, 10) * random.choice((1, -1))
            break

        # Set an invalid end value
        while True:
            # Continue until a row which is not the last one of a channel_id is selected
            i = random.randint(0, n - 2)
            if data[i][0] != data[i + 1][0]:
                continue
            data[i][4] += random.randint(1, 10) * random.choice((1, -1))
            break

        df = self.spark.createDataFrame(data, ['channel_id', 'start_ok', 'end_ok', 'start_fail', 'end_fail',
                                               'start_ok_not_strict'])
        df.show()

        check_name = f"check_continuous_samples"
        folder_name = self.output_folder + "/" + check_name
        df.coalesce(1).write.format('parquet').mode("overwrite").save(folder_name)
