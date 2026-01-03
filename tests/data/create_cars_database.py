import datetime
import os
import pathlib
import random
import string

from pyspark.sql.functions import col, when
from pyspark.sql.types import *

from tests.data.base_generator import BaseGenerator


class CarDatabaseGenerator(BaseGenerator):
    def __init__(self, output_folder, spark_session):
        super().__init__(output_folder, spark_session)

        self.output_folder = self.output_folder + "cars_database/"

    def create_customers_df(self):
        """
        Creates a dataframe with the following columns:
        - customer_id, int, unique, non-null, primary key
        - first_name, str, non-null
        - last_name, str, non-null
        - country, str, non-null,
        - birthdate, datetime, non-null,
        - customer_since, datetime, non-null, larger than birthdate
        """
        n_rows = 1000
        first_names = [f"first_name_{i}" for i in range(100)]
        last_names = [f"last_name_{i}" for i in range(100)]
        countries = [f"country_{i}" for i in range(10)]

        def random_date():
            return datetime.datetime.strptime(
                f"{random.randrange(1920, 2000)}-{random.randrange(1, 13)}-{random.randrange(1, 29)}", "%Y-%m-%d"
            )

        schema = StructType(
            [
                StructField("customer_id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("birthdate", DateType(), True),
                StructField("customer_since", DateType(), True),
            ]
        )

        data = []
        for row_idx in range(n_rows):
            birthdate = random_date()
            customer_since_date = birthdate + datetime.timedelta(days=random.randrange(1000, 365 * 20))
            data.append(
                (
                    row_idx,
                    first_names[random.randint(0, len(first_names) - 1)],
                    last_names[random.randint(0, len(last_names) - 1)],
                    countries[random.randint(0, len(countries) - 1)],
                    birthdate,
                    customer_since_date,
                )
            )

        df = self.spark.createDataFrame(data, schema)
        return df

    def create_cars_df(self, customers_df):
        """
        Creates a dataframe with the following columns:
        - car_id, int, unique, non-null, primary key
        - customer_id, int, non-null, foreign key
        """
        schema = StructType(
            [StructField("car_id", IntegerType(), True), StructField("customer_id", IntegerType(), True)]
        )
        data = []
        row_idx = 0

        customers = customers_df.collect()
        for customer_idx in range(customers_df.count()):
            # Some customers might have multiple cars
            more_cars = True
            while more_cars:
                data.append((row_idx, customers[customer_idx].customer_id))
                row_idx += 1
                more_cars = random.random() > 0.9

        df = self.spark.createDataFrame(data, schema)
        return df

    def create_measurements_df(self, cars_df):
        """
        Creates a dataframe with the following columns:
        - measurement_id, int, unique, non-null, primary key
        - car_id, int, non-null, foreign key
        - measurement_time, int, non-null
        - sensor_1, float, non-null
        - sensor_2, float, non-null,
        - msg, str
        """
        schema = StructType(
            [
                StructField("measurement_id", IntegerType(), True),
                StructField("car_id", IntegerType(), True),
                StructField("measurement_time", IntegerType(), True),
                StructField("sensor_1", FloatType(), True),
                StructField("sensor_2", FloatType(), True),
                StructField("msg", StringType(), True),
            ]
        )
        data = []
        row_idx = 0

        cars = cars_df.collect()
        for car_idx in range(cars_df.count()):
            # Some cars might have multiple measurements
            more_measurements = True
            while more_measurements:
                data.append(
                    (
                        row_idx,  # measurement_id
                        cars[car_idx].car_id,  # car_id
                        random.randint(946684800, 1609459200),  # measurement_time
                        random.random() * 10.0,  # sensor_1
                        random.random() * 50.0 - 25.0,  # sensor_2
                        "String",  # msg
                    )
                )
                row_idx += 1
                more_measurements = random.random() > 0.5

        df = self.spark.createDataFrame(data, schema)
        return df

    def rename_table_files(self, folder_name: str, format_type: str, time_back: int = 100):
        """ Rename the table files to match the pattern <table_name>_<time>.parquet """
        table_name = str(pathlib.Path(folder_name).name)
        files = sorted(list(pathlib.Path(folder_name).glob(f"part-*.{format_type}")))
        file_date = datetime.date.today() - datetime.timedelta(days=time_back)
        one_day_delta = datetime.timedelta(days=1)
        for f in files:
            time_str = file_date.strftime("%Y%m%d")
            new_path = pathlib.Path(f"{folder_name}/{table_name}_{time_str}.{format_type}")
            f.rename(new_path)
            file_date += one_day_delta

    def save_files(self, df, folder: str, format_type: str, id_col: str):
        # for file in pathlib.Path(folder).iterdir():
        #     file.unlink()
        row_count = df.count()
        n = int(row_count * 0.2)
        initial_df = df.where(col(id_col).between(0, n))
        initial_df.show()
        initial_df.coalesce(1).write.format(format_type).mode("append").save(folder)
        self.rename_table_files(folder, format_type, 100)

        incremental_df = df.where(col(id_col).between(n + 1, 100000000000))
        incremental_df.write.format(format_type).mode("append").save(folder)
        self.rename_table_files(folder, format_type, 99)

    def create_dataset(self, folder: str, format_type: str, corrupted=False):
        customers_df = self.create_customers_df()
        cars_df = self.create_cars_df(customers_df)
        measurements_df = self.create_measurements_df(cars_df)

        if corrupted:
            # Remove a customer
            customers_df = customers_df.where(f"customer_id != {random.randint(0, customers_df.count() - 1)}")

            # Set name to null
            customer_id = customers_df.collect()[random.randint(0, customers_df.count() - 1)].customer_id
            customers_df = customers_df.withColumn(
                "first_name", when(col("customer_id") == customer_id, None).otherwise(col("first_name"))
            )

            # Make column non-unique
            i = random.randint(0, measurements_df.count() - 2)
            old_measurement_id = measurements_df.collect()[i].measurement_id
            new_measurement_id = measurements_df.collect()[i + 1].measurement_id
            measurements_df = measurements_df.withColumn(
                "measurement_id",
                when(col("measurement_id") == old_measurement_id, new_measurement_id).otherwise(col("measurement_id")),
            )

            # Change to wrong column type
            measurements_df = measurements_df.withColumn("sensor_2", col("sensor_2").cast(StringType()))

        self.save_files(customers_df, folder + "/customers", format_type, "customer_id")

        self.save_files(cars_df, folder + "/cars", format_type, "car_id")

        self.save_files(measurements_df, folder + "/measurements", format_type, "measurement_id")

    def generate_data(self):
        self.create_dataset(self.output_folder + "/customer_cars_dataset_ok", format_type="parquet")
        self.create_dataset(self.output_folder + "/customer_cars_dataset_fail", format_type="parquet", corrupted=True)
