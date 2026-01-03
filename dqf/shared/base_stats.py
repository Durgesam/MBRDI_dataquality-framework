from dqf.shared.base import Base


class BaseStats(Base):
    """
    Base Statistics Class
    """

    def __init__(self, *args, **kwargs):
        """
        Class will directly initalized by derived classes
        """
        self.value = {}
        self.data = {}

    def run(self) -> bool:
        """
        Should be implemented by the derived class
        """
        raise NotImplementedError

    @staticmethod
    def replace_dataframe(check_config, dataframe, foreign_dataframe=None):
        check_config["kwargs"]["table"] = dataframe
