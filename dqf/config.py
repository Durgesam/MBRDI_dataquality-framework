from typing import Dict, List, Set, Union, Iterable


class ConfigParserException(Exception):
    pass


class InvalidConfigKeyException(ConfigParserException):
    def __init__(self, keys: Union[str, Iterable[str]], elem: dict, message: str = "Invalid config keys"):
        if isinstance(keys, str):
            self.keys = {keys}
        else:
            self.keys = set(keys)
        self.elem = elem
        self.message = message

    def __str__(self):
        return f"{self.message}: {self.keys} in {self.elem}"


class ConfigParser:
    """ Configuration parser. Refer to test_config_parser for config examples """

    def __init__(self, config: dict):
        self.config = config
        self.parsed_checks = []

    def parse_config(self) -> Dict:
        self.parsed_checks = []

        allowed_first_level_keys = {
            "datasource_path",
            "profile_name",
            "data_format",
            "vars",
            "checks",
            "load_options",
            "incremental_column",
            "incremental_filter",
            "stats",
        }
        illegal_keys = set(self.config.keys()).difference(allowed_first_level_keys)
        if len(illegal_keys) > 0:
            raise InvalidConfigKeyException(illegal_keys, self.config, "Invalid config key on first level")

        vars_ = self.config["vars"] if "vars" in self.config else {}

        self._parse_checks(self.config["checks"], vars_=vars_)
        #TODO: Parsing for Stats has to be implemented accordingly
        
        # Copy everything except "vars" from top-most config level
        parsed_config = self.config.copy()
        parsed_config["checks"] = self.parsed_checks
        
        if "vars" in parsed_config:
            del parsed_config["vars"]

        self.parsed_checks = []
        return parsed_config

    def _build_check_config(self, element, type_=None, vars_=None):
        check_config = {}
        check_config["type"] = element["type"] if "type" in element else type_
        check_config["kwargs"] = {} if vars_ is None else vars_.copy()
        check_config["kwargs"].update(element)

        self.parsed_checks.append(check_config)

    def _parse_checks(self, checks: List, type_: str = None, vars_: dict = None):
        """ Parse a checks list containing several check elements """
        if not isinstance(checks, list):
            raise ConfigParserException("checks should be a list")
        for c in checks:
            self._parse_check_element(c, type_, vars_)

    def _parse_check_element(self, check_element: Union[Dict, str], type_: str = None, vars_: dict = None):
        """
        Parse a check element which is either 1) a dictionary or 2) a string.

        1) Dictionary in one of the following settings:
        1.1)
        - with a "type" key, specifying the check type as a string
        - optionally with a "kwargs" key, specifying keyword arguments as a dict or as a list of dicts
        - optionally with a "vars" key, specifying shared arguments as a dict
        - no other keys are allowed

        1.2)
        - with a "checks" key, specifying a list of more check elements
        - optionally with a "vars" key, specifying shared arguments as a dict
        - no other keys are allowed

        If kwargs in check element: parse kwargs
        If checks list in check element: parse checks
        Otherwise: build check config

        2) string
        A string <string> is converted to a dictionary with {"type": <string>}
        """
        if isinstance(check_element, str):
            check_element = dict(type=check_element)

        if not isinstance(check_element, dict):
            raise ConfigParserException("check element should be a dictionary or a string")

        def check_dict_keys(x: dict, keys: Set) -> Set:
            return set(x.keys()).difference(keys)

        # Check if illegal keys are used
        if "type" in check_element:
            # Setting 1.1: kwargs and vars allowed
            illegal_keys = check_dict_keys(check_element, {"type", "kwargs", "vars"})
            if len(illegal_keys) > 0:
                raise InvalidConfigKeyException(illegal_keys, check_element)
            type_ = check_element["type"]
        elif "checks" in check_element:
            # Setting 1.2:  vars allowed
            illegal_keys = check_dict_keys(check_element, {"checks", "vars"})
            if len(illegal_keys) > 0:
                raise InvalidConfigKeyException(illegal_keys, check_element)
        else:
            raise ConfigParserException(f"Check element must contain a 'type' or a 'checks' key in {check_element}")

        # Update "vars_"
        vars_ = {} if vars_ is None else vars_.copy()
        if "vars" in check_element:
            vars_.update(check_element["vars"])

        # Further parsing
        if "kwargs" in check_element:
            self._parse_kwargs(check_element["kwargs"], type_, vars_)
        elif "checks" in check_element:
            self._parse_checks(check_element["checks"], type_, vars_)
        else:
            self._build_check_config({}, type_, vars_)

    def _parse_kwargs(self, kwargs_: Union[List, Dict], type_: str = None, vars_: dict = None):
        if isinstance(kwargs_, list):
            for k in kwargs_:
                self._parse_kwargs_element(k, type_, vars_)
        elif isinstance(kwargs_, dict):
            self._parse_kwargs_element(kwargs_, type_, vars_)
        else:
            raise ConfigParserException(f"Invalid kwargs: {kwargs_}. Should be a list or a dict")

    def _parse_kwargs_element(self, kwargs_: Dict, type_: str = None, vars_: dict = None):
        self._build_check_config(kwargs_, type_, vars_)
