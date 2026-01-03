# import sys
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession

from tests.data import *
from tests.data.create_check_continuous_samples_data import ContinuousSamplesGenerator
from tests.data.create_check_regex_data import RegexGenerator
from tests.unittest_config import UnitTestConfig


# sys.path.append(r'/opt/spark-apps/DQF/')


def generate_all(list_of_gens: List[BaseGenerator]):
    """
    Runs all the checks provided by the list of generators
    Args:
        list_of_gens: Array of Check Generators to be applied
    """
    for gen in list_of_gens:
        gen.generate_data()


if __name__ == '__main__':
    # output_path = str(pathlib.Path().absolute()) + "/tests/generated_Data/"
    # output_path = "src/DQF_testData/"
    output_path = UnitTestConfig.general_test_data_folder.value
    if Path(output_path).exists():
        Path(output_path).rmdir()
    print(output_path)
    spark = SparkSession \
        .builder \
        .appName("TestDataCreation") \
        .getOrCreate()

    gen_list = [
        DataRelationGenerator(output_path, spark),
        DataTypeGenerator(output_path, spark),
        EmptyStringDataGenerator(output_path, spark),
        FileFormatGenerator(output_path, spark),
        ForeignKeyGenerator(output_path, spark),
        IncrementalLargerGenerator(output_path, spark),
        NullDataGenerator(output_path, spark),
        NullStringDataGenerator(output_path, spark),
        StringConsistencyGenerator(output_path, spark),
        TimeFormatGenerator(output_path, spark),
        UniqueValuesGenerator(output_path, spark),
        WhitespaceGenerator(output_path, spark),
        CarDatabaseGenerator(output_path, spark),
        EmptyDFGenerator(output_path, spark),
        MinMaxGenerator(output_path, spark),
        ContinuousSamplesGenerator(output_path, spark),
        RegexGenerator(output_path, spark),
        EnumDataGenerator(output_path, spark)
    ]

    generate_all(gen_list)
