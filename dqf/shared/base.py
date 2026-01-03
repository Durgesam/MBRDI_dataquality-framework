import inspect
import logging
from typing import List


class Base:
    """
    Base Class
    """

    def run(self):
        """
        Should be implemented by the derived class
        """
        raise NotImplementedError

    @property
    def logger(self):
        """
        Should be implemented by the derived class
        """
        return logging.getLogger("DQF")

    @staticmethod
    def replace_dataframe(check_config, dataframe, foreign_dataframe=None):
        """
        Will be implemented by derived classes if necessary

        """
        pass

    @classmethod
    def get_arguments(cls, optionals=True) -> List[str]:
        """
        Returns a list of arguments (i.e. the __init__ arguments except self)
        @param optionals: Whether to include optionals or not (i.e. argument with a default value in __init__)
        """
        spec = inspect.getfullargspec(cls)
        args = [a for a in spec.args if a != "self"]
        # Ignore optionals if desired and default values exist
        if not optionals and spec.defaults is not None:
            args = args[:-len(spec.defaults)]
        return args
