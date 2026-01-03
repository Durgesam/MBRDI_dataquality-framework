import enum


class UnitTestDataFolders(enum.Enum):
    pass


class UnitTestConfig(enum.Enum):
    """
    For Azure Pipeline always use "tests/data_generation/"
    """
    general_test_data_folder = "tests/data_generation/"
