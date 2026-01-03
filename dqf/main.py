import glob
import hashlib
import json
import logging
import os
import pathlib
from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import shutil

import importlib_metadata
import pyspark.sql.functions as F
import pyspark.sql.utils
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import when, concat, lit, regexp_replace, col

from dqf.config import ConfigParser, ConfigParserException
from dqf.shared import utils
from dqf.shared.utils import (
    DataFrameCache,
    check_table_columns,
    config_check,
    get_dqf_class_from_name,
    get_configuration,
    get_log_filename,
)
from dqf.shared.html_template import (
    html_template_homepage,
    html_template_failed_data,
    html_template_checkresult
)

cache = DataFrameCache()
FILE_FORMAT = ""
SPARK_LOAD_OPTIONS = ""


def setup_config(config: Dict):
    """
    Setup configuration of DQF.
    """
    global cache
    cache = DataFrameCache(config)


class CheckProject:
    """
    Example json config:
    {
                "datasource_path": "customer_cars_dataset_ok/",
                "checks": [
                        {
                                "type": "check_file_format",
                                "kwargs": {
                                        "file_path": "cars/cars_20201005.orc",
                                        "file_format": "orc"
                                }
                        },
                        {
                                "type": "check_datatype",
                                "kwargs": {
                                        "table_path": "cars/",
                                        "column_name": "car_id",
                                        "d_type": "str"
                                }
                        }
                ]
    }
    """

    def __init__(
            self,
            config: Union[dict, str, pathlib.Path],
            on_dbfs: bool = True,
            write_results: Optional[bool] = True,
            html_write_report: Optional[bool] = False,
            dataframe: Optional[DataFrame] = None,
            foreign_dataframe: Optional[DataFrame] = None,
            result_format: Optional[str] = None,
            html_report_path: Optional[str] = None

    ):
        try:
            if isinstance(config, (str, pathlib.Path)):
                if on_dbfs:
                    config = f"/dbfs{config}"
                config = json.loads(pathlib.Path(config).read_text())
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Cannot decode the JSON config file. Error: {error}")

        self.config = config
        self.on_dbfs = on_dbfs
        self.dataframe = dataframe
        self.foreign_dataframe = foreign_dataframe
        self.write_results = write_results
        self.html_write_report = html_write_report
        self.result_format = result_format
        self.logger = logging.getLogger("DQF")
        self.check_results = {}
        self.stats_results = {}
        self.html_report_path = html_report_path

    @staticmethod
    def fetch_check_type_arguments(check_results):
        """
        Returns the arguments of a check.
        """
        check_type_arguments = []
        for c, l in enumerate(check_results, start=0):
            for key, value in l["kwargs"].items():
                h = hashlib.sha512(str(l["kwargs"]).encode()).hexdigest()
                if isinstance(value, list):
                    if len(value) == 1:
                        check_type_arguments.append([key, value[0], "null", f"{h}"])
                        continue
                    for i in value:
                        check_type_arguments.append([key, i, str(value.index(i) + 1), f"{h}"])
                else:
                    check_type_arguments.append([key, value, "null", f"{h}"])

        return check_type_arguments

    def export_args_result(self, check_results, sub_df, check_results_path, profile_name):
        """This function is used to create check arguments delta table.
        1. Parameters getting passed -
        -check_results:
        -sub_df: a data frame used to generate unique args_id() for each check (type,kwargs) pair starting from 0
        -check_results_path: the config file which has export path location for delta table
        -profile_name: delta table name

        2. check if the any value in 'kwargs' dictionary is of type 'DataFrame'.
                a. If yes, replace those values with user defined string 'user-defined dataframe'
        3.  check if the any value in 'kwargs' dictionary is of type 'list'.
                a. If yes, start the index position of values from 0 or else make it null

        4. check_arguments_df is the data frame which has below columns -
         - key: key names inside "kwargs" dictionary
         - value: values inside "kwargs" dictionary
         - list_index : null if a key has single value,starts from 0 if not
         - args_id: This should match with args_id in check results delta table for unique check (type,kwargs) pair
         - args_hashcode: Unique identifier for each check (type,kwargs) pair

         5. dqf_check_arguments_path is the path where delta table will get created or appended if it already exists
        """

        check_type_arguments = CheckProject.fetch_check_type_arguments(check_results)

        check_argument_schema = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
                StructField("list_index", StringType(), True),
                StructField("args_hashcode", StringType(), True)
            ]
        )
        spark = SparkSession.builder.getOrCreate()
        check_arguments_df = spark.createDataFrame(data=check_type_arguments, schema=check_argument_schema)
        check_arg_df = (
            check_arguments_df.join(sub_df, check_arguments_df.args_hashcode == sub_df.idx, how="left")
                .drop(sub_df.idx)
                .distinct()
                .sort("args_id")
                .select("key", "value", "list_index", "args_id", "args_hashcode")
        )
        dqf_check_arguments_path = check_results_path["result_output_path"] + "/" + profile_name + "/args"

        try:
            if self.result_format is None:
                dqf_check_arguments_format = check_results_path["result_format"].lower()
            else:
                dqf_check_arguments_format = self.result_format.lower()

            get_arguments_df = spark.read.format(check_results_path["result_format"]).option('inferSchema', True) \
                .option('header', True).load(dqf_check_arguments_path + "/" + dqf_check_arguments_format)
            check_arg_df = check_arg_df.join(get_arguments_df, ["args_hashcode"], "leftanti")

        except pyspark.sql.utils.AnalysisException:
            # No arguments table exists yet, so no join required
            pass

        check_arg_df = check_arg_df.sort(check_arg_df.args_id.asc())

        check_arg_df.write.format(dqf_check_arguments_format).option('inferSchema', True) \
            .option('header', True).mode("append").save(dqf_check_arguments_path + "/" + dqf_check_arguments_format)

        result = check_arg_df.filter(check_arg_df.key == "table").select("key", "value", "args_hashcode")
        return result

    def export_check_results(self, config, check_results, log_filename):
        """Function used to build check results delta table
        1. check_results and log file name is created from run method which contains config details

        2. creating "check_results_df" data frame which contains below columns :
            -"check_name": name of the check
            -"runtime[hh:mm:ss:ms]": total time taken by check to complete
            -"result": pass or fail
            -"source_path": the path for source files
            -"message": log file details
            -"args_hashcode": Unique identifier for each check (type,kwargs) pair
            -"time": current date time
            -"profile_name": name of the delta table which comes from config file

        3. Left join to make sure we get new unique args_id column for a dataframe which has same (type,kwargs) pair

        4. Sorting the args_id in ascending order

        5. dqf_check_results_path is the path where delta table will get created or appended if it already exists

        6. export_args_result method is used to get "check arguments" delta table
        """

        profile_name = config["profile_name"]
        for dictionary in check_results:
            for key, value in dictionary["kwargs"].items():
                if isinstance(value, DataFrame):
                    dictionary["kwargs"][key] = "user-defined dataframe"
        check_type_results = [
            [
                elt["type"],
                elt["time_taken"],
                "pass" if elt["Result"] else "fail",
                elt["datasource_path"],
                "check run successful" if elt["Result"] else f"kindly look into the log file - {log_filename}",
                hashlib.sha512(str(elt["kwargs"]).encode()).hexdigest(),
            ]
            for elt in check_results
        ]
        check_columns = ["check_name", "runtime[hh:mm:ss:ms]", "result", "source_path", "message", "args_hashcode"]
        spark = SparkSession.builder.getOrCreate()
        check_results_df = spark.createDataFrame(data=check_type_results, schema=check_columns)
        check_results_df = check_results_df.withColumn("time", F.current_timestamp())
        check_results_df = check_results_df.withColumn("profile_name", F.lit(profile_name))
        kwargs_list = [json.dumps(x["kwargs"]) for x in check_results]
        list_ids = [{v: k for k, v in enumerate(OrderedDict.fromkeys(kwargs_list))}[n] for n in kwargs_list]
        idx = [hashlib.sha512(str(elt["kwargs"]).encode()).hexdigest() for elt in check_results]
        sub_schema = StructType([StructField("idx", StringType(), True), StructField("args_id", StringType(), True)])
        sub_df = spark.createDataFrame(zip(idx, list_ids), schema=sub_schema)
        check_result_df = (
            check_results_df.join(sub_df, check_results_df.args_hashcode == sub_df.idx, how="left")
                .drop(sub_df.idx)
                .distinct()
                .sort("args_id")
                .select(
                "profile_name",
                "check_name",
                "runtime[hh:mm:ss:ms]",
                "result",
                "source_path",
                "message",
                "time",
                "args_id",
                "args_hashcode",
            )
        )
        if self.on_dbfs:
            install_path = "/dbfs" + get_install_path()
        else:
            install_path = get_install_path()

        files = glob.glob(install_path + "/config/dqf_config.json", recursive=True)
        check_results_path = None

        if len(files) != 0:
            with open(files[0], "r") as json_file:
                check_results_path = json.load(json_file)
        dqf_check_results_path = check_results_path["result_output_path"] + "/" + profile_name + "/results"
        check_result_df = check_result_df.sort(check_result_df.args_id.asc())

        # Calling the results arguments method
        result = self.export_args_result(check_results, sub_df, check_results_path, profile_name)

        check_result_df = (
            check_result_df.join(result, check_result_df.args_hashcode == result.args_hashcode, how="left")
                .drop(check_result_df.args_hashcode)
                .distinct()
                .sort("args_id")
                .select(
                col("profile_name"),
                col("check_name"),
                col("runtime[hh:mm:ss:ms]"),
                col("result"),
                col("source_path"),
                col("value").alias("table"),
                col("message"),
                col("time"),
                col("args_id"),
                col("args_hashcode"),
            )
        )

        if self.result_format is None:
            dqf_check_result_format = check_results_path["result_format"].lower()

        elif self.result_format.lower() in ["csv", "delta", "json", "parquet"]:
            dqf_check_result_format = self.result_format.lower()

        else:
            print("Please enter a valid file type for output")
            return None

        check_result_df.write.format(dqf_check_result_format).option('inferSchema', True).option('header', True). \
            mode("append").save(dqf_check_results_path + "/" + dqf_check_result_format)


    def show_failed_data(self, disp_func=None):
        """
            Function to show all the failed data of the checks

            1. disp_func: optional parameter that expects a display function from Databricks Notebook
        """
        print(f'\nShowing failed data for quality checks:')
        self.logger.info(f'\nShowing failed data for quality checks:')

        if disp_func and not callable(disp_func):
            print(f'\nWARNING: Display function provided is not callable: `{disp_func}`')
            disp_func = None

        results = self.check_results
        for count, result in enumerate(results):
            check_type = result["type"]
            check_args = result["kwargs"]
            check_result = result["Result"]
            failed_data_lst = result["failed_data"]

            check_count = count + 1
            if not check_result:
                print(f'\n{check_count}. Failed data for `{check_type}` with arguments {check_args}')
                self.logger.info(f'\n{check_count}. Failed data for {check_type} with arguments {check_args}')

                if failed_data_lst:
                    for f_count, fd in enumerate(failed_data_lst):
                        cols_affected = fd["for_column"]
                        failed_df = fd["dataframe"]
                        failed_rows_count = fd["row_count"]
                        print(f"\n{check_count}.{f_count + 1}. Column(s) Affected: '{cols_affected}'")
                        self.logger.info(f"\n{check_count}.{f_count + 1}. Column(s) Affected: '{cols_affected}'")

                        if disp_func:
                            try:
                                # To display dataframe in Databricks notebook by passing the display function
                                disp_func(failed_df)
                            except Exception:
                                print(f'Unable to use the display function provided `{disp_func}`')
                                disp_func = None

                        if not disp_func:
                            # To be able to display dataframe in all environments
                            failed_df.show()

                        self.logger.info("You can see the sample failed rows below:")
                        self.logger.info(str(failed_df.head(10)))

                        print(f'{failed_rows_count} row(s) failed the check.')
                        self.logger.info(f'{failed_rows_count} row(s) failed the check.')
                else:
                    print(f'\nEmpty dataset.')
                    self.logger.info(f'\nEmpty dataset.')
            else:
                if not failed_data_lst:
                    print(f'\n{check_count}. No failed data for `{check_type}` with arguments {check_args} '
                          f'\nCheck executed successfully.')
                    self.logger.info(f'\n{check_count}. No failed data for `{check_type}` with arguments {check_args} '
                                     f'\nCheck executed successfully.')
                else:
                    pass  # TODO: Erroneous case
            print('\n', '*' * 10)
            self.logger.info('\n', '*' * 10)

    def _process_stats(self, config) -> List[Dict]:
        """
        Processes all the given statistics from the config file.
        Duration will be also measured for each single stats run with the arguments
        """

        stats_results = []
        if "stats" not in config:
            return stats_results

        for stats_count, stats_config in enumerate(config["stats"]):
            stat_exception_msg = ""
            result = False  # TODO:REVIEW
            name = stats_config["type"]
            kwargs = stats_config["kwargs"]
            stats_class = utils.get_dqf_class_from_name(name)
            stat = stats_class(**kwargs)
            start_stat = datetime.now()
            try:
                print(f'Statistic {stats_count + 1}:')
                result = stat.run()
            except Exception as error:
                stat_exception_msg = str(error)
                self.logger.exception(stat_exception_msg, exc_info=True)
            end_check = datetime.now()
            total_time = end_check - start_stat
            info = stats_config.copy()
            info["datasource_path"] = self.config["datasource_path"]
            info["Result"] = result
            info["time_taken"] = str(total_time)
            info["stat_exception_message"] = stat_exception_msg if stat_exception_msg else "no exception"
            info["result_value"] = stat.value
            info["result_data"] = stat.data
            stats_results.append(info)
            print(f"Result of {name} with {kwargs} is {result} (Statistic took {str(total_time)}[hh:mm:ss])\n")

        return stats_results

    def _process_checks(self, config) -> List[Dict]:
        """
        Processes all the given checks from the config file.
        Duration will be also measured for each single check run with the arguments
        """
        check_results = []
        if "checks" not in config:
            return check_results

        for check_count, check_config in enumerate(config["checks"]):
            check_exception_msg = ""
            result = False
            name = check_config["type"]
            kwargs = check_config["kwargs"]
            check_class = utils.get_dqf_class_from_name(name)
            check = check_class(**kwargs)
            start_check = datetime.now()
            try:
                print(f'Check {check_count + 1}:')
                result = check.run()
            except Exception as error:
                check_exception_msg = str(error)
                self.logger.exception(check_exception_msg, exc_info=True)
            end_check = datetime.now()
            total_time = end_check - start_check
            info = check_config.copy()
            info["datasource_path"] = self.config["datasource_path"]
            info["Result"] = result
            info["time_taken"] = str(total_time)
            info["check_exception_message"] = check_exception_msg if check_exception_msg else "no exception"
            info["failed_data"] = check.failed_data
            check_results.append(info)
            print(f"Result of {name} with {kwargs} is {result} (Check took {str(total_time)}[hh:mm:ss])\n")

        return check_results

    def _validate_config(self) -> bool:
        """
        Returns True when the config is valid, otherwise returns False
        """
        fix_message = "\nPlease fix all the above issues with the config file to run the DQF."
        outcomes: List[Tuple[bool, str]] = []

        try:
            global cache
            global FILE_FORMAT
            global SPARK_LOAD_OPTIONS

            # Parse config
            self.config = ConfigParser(self.config).parse_config()

            FILE_FORMAT = self.config["data_format"]
            SPARK_LOAD_OPTIONS = self.config["load_options"] if "load_options" in self.config else {}
            setup_config(self.config)

            third_level_keys_result = config_check(self.config)
            # TODO:implementation for stats accordingly
            if len(third_level_keys_result) > 0:
                outcomes.extend(third_level_keys_result)

            # Continue with checks if no config issue was found
            if not any(result[0] for result in third_level_keys_result):
                table_columns_result = check_table_columns(self.config)
                # TODO:implementation for stats accordingly maybe inside function check_table_columns
                if len(table_columns_result) > 0:
                    outcomes.extend(table_columns_result)
        except ConfigParserException as error:
            outcomes.append((True, str(error)))
        except KeyError as error:
            outcomes.append((True, f"{error} is mandatory. Please provide it in the config."))

        for _, info in outcomes:
            self.logger.info(info)
            print(info)

        if any(failed for failed, _ in outcomes):
            self.logger.info(fix_message)
            print(fix_message)
            return False
        else:
            return True

    def _setup_logger(self, filename):
        self.logger.setLevel(logging.DEBUG)

        log_formatter = logging.Formatter("%(message)s")

        log_handler = logging.FileHandler(filename)
        log_handler.setFormatter(log_formatter)

        self.logger.addHandler(log_handler)

    @staticmethod
    def pass_fail(c, check_type, key, value, col_check_type_arguments, check_failed_data, result, table_value):
        """
            Returns list of check column arguments with their pass/fail status
                    """
        flag = False
        for e in check_failed_data:
            if isinstance(e, list):
                flag = True
        if key == "columns":
            if isinstance(value, list):
                if len(value) == 1:
                    col_check_type_arguments.append([c, check_type, table_value, key, value[0],
                                                     "Fail" if value[0] in check_failed_data else "Pass"])
                else:
                    if flag:

                        col_check_type_arguments.append([c, check_type, table_value, key, ' & '.join([str(n) for n in value]),
                                                         "Fail" if value in check_failed_data else "Pass"])
                    else:
                        for i in value:
                            col_check_type_arguments.append([c, check_type, table_value, key, i,
                                                             "Fail" if i in check_failed_data else "Pass"])
            else:
                col_check_type_arguments.append([c, check_type, table_value, key, value,
                                                 "Fail" if value in check_failed_data else "Pass"])
        elif not key == "columns" and result == "Pass":
            col_check_type_arguments.append([c, check_type, table_value, key, value, result])

        return col_check_type_arguments

    @staticmethod
    def check_runs_column_table_args(check_results):
        """
            Returns a dataframe with column/table arguments of a check for Level 2 (Check_run status for a selected
            config) of HTML report
            """
        col_check_type_arguments = []
        for c, l in enumerate(check_results, start=0):
            result = "Pass" if l["Result"] else "Fail"
            check_failed_data = []
            for key, value in l["kwargs"].items():
                if "table" in key:
                    table_value=value
            for data in l["failed_data"]:
                for key, value in data.items():
                    if key == "for_column":
                        check_failed_data.append(value)
                    elif key == "for_columns":
                        col_check_type_arguments.append([c, l["type"], table_value, "columns", value, "Fail"])

            # For column level checks - calling pass_fail() for getting their result status
            if any("column" in string for string in l["kwargs"].keys()):
                for key, value in l["kwargs"].items():
                    if "column" in key:
                        col_check_type_arguments = CheckProject.pass_fail(c, l["type"], key, value,
                                                                          col_check_type_arguments, check_failed_data,
                                                                          result,table_value)

            # For Table level checks - table argument with their result status
            else:
                col_check_type_arguments.append([c, l["type"], table_value, "table", l["kwargs"]["table"], result])

        col_check_argument_schema = StructType(
            [
                StructField("Check_No", StringType(), True),
                StructField("Check_Name", StringType(), True),
                StructField("Table_Name", StringType(), True),
                StructField("Key", StringType(), True),
                StructField("value", StringType(), True),
                StructField("Result", StringType(), True)
            ]
        )
        spark = SparkSession.builder.getOrCreate()
        col_check_arguments_df = spark.createDataFrame(data=col_check_type_arguments, schema=col_check_argument_schema)

        return col_check_arguments_df

    @staticmethod
    def write_html_report(config, on_dbfs, html_report_path, overall_result, check_results):
        """Function used to create folders and files for HTML reporting

             Three Level of HTML reporting is defined
                 Level 1: Home Page containing all the configs that has run
                 Level 2: Drill down html page for selected config, it contains all the check runs for that config
                 Level 3: Further drill down html page on failed column from a check, it contains the sample failed data
                    """

        # Checking the path to be created for HTML report folder
        if html_report_path is None:
            html_folder_path = get_install_path() + "/check_results/HTML_Report"
            if not os.path.exists(html_folder_path):
                os.makedirs(html_folder_path)
        else:
            html_folder_path = html_report_path.lower()

        if on_dbfs:
            html_folder_path = "/dbfs" + html_folder_path

        valid_path = os.path.exists(html_folder_path)
        if not valid_path:
            return False

        profile_name = config["profile_name"]
        html_file_path = html_folder_path + "/Check_Runs/"

        # Creating folder for css and javascript files in html report location
        src_dir = os.path.dirname(__file__)
        src = src_dir + '/shared/html'
        dst = html_folder_path + '/html'
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

        if not overall_result:
            final_result = "Fail"
        else:
            final_result = "Pass"

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        dqf_html_path = html_file_path + profile_name + "-" + final_result + "-" + timestamp

        # Creating HTML report folders for individual config runs
        if not os.path.exists(dqf_html_path):
            os.makedirs(dqf_html_path)

        # Creating Html files for failed data - Level 3
        failed_data_html_path = dqf_html_path + "/Check_Details"
        if not os.path.exists(failed_data_html_path):
            os.makedirs(failed_data_html_path)

        for c, l in enumerate(check_results, start=0):
            check_name = l["type"]
            data = l["failed_data"]
            k = l["kwargs"]
            p = []
            for key, value in k.items():
                if not key.startswith('column'):
                    p.append([key, value])
            for fd in data:
                for key, value in fd.items():
                    if key in ["for_column", "for_columns"]:
                        if isinstance(value, list):
                            value = '&'.join([str(n) for n in value])
                        col = value.replace(" ", "")
                df = fd["dataframe"]
                html_filename = f"{failed_data_html_path}/{check_name}_{col}_{c}.html"
                checkrun_html_file = profile_name + "_" + timestamp + ".html"
                file = open(html_filename, "w+")
                file.write(html_template_failed_data(df, p, col, check_name, checkrun_html_file))
                file.close()

        # Creating html file for check runs in a config - Level 2
        df_checkruns = CheckProject.check_runs_column_table_args(check_results)
        link_failed_data = concat(lit("<a href=Check_Details/"), df_checkruns.Check_Name, lit("_"),
                                  regexp_replace(df_checkruns.value, " ", ""), lit("_"), df_checkruns.Check_No,
                                  lit(".html>"), df_checkruns.Check_Name, lit("</a>"))

        df_checkruns = df_checkruns.withColumn("Check_Name", when(df_checkruns.Result == "Fail", link_failed_data)
                                               .otherwise(df_checkruns.Check_Name))

        html_filename = f"{dqf_html_path}/{profile_name}_{timestamp}.html"
        file = open(html_filename, "w+")
        file.write(html_template_checkresult(df_checkruns, datetime.strptime(timestamp, '%Y%m%d%H%M%S')))
        file.close()

        # Creating home page with details of config runs - Level 1
        config_name = "Config Name"
        df = pd.DataFrame(columns=[config_name, "Overall Result", "Time"])
        config_folders = []

        directory_contents = os.listdir(html_file_path)

        for item in directory_contents:
            config_folders.append(item)
            f = item.split("-")
            f[2] = datetime.strptime(f[2], '%Y%m%d%H%M%S')
            to_append = f
            df_length = len(df)
            df.loc[df_length] = to_append

        check_run_file = [os.path.basename(x) for x in glob.glob(f'{html_file_path}**/*.html')]

        df["check_link"] = check_run_file
        df["config_link"] = config_folders
        df[config_name] = "<a href =Check_Runs/" + df["config_link"] + "/" + df["check_link"] + ">" + df[
            config_name] + "</a>"

        homepage_filename = f"{html_folder_path}/index.html"
        file = open(homepage_filename, "w+")
        file.write(html_template_homepage(df))
        file.close()

        return True

    def run(self) -> bool:
        """
        Run CheckProject based on the configuration.
        It returns ``True`` in case if all checks pass or else it will return ``False``.
        """
        start = datetime.strptime(datetime.now().replace(microsecond=0).isoformat(" "), "%Y-%m-%d %H:%M:%S")

        try:
            filename = get_log_filename(
                get_configuration("result_output_path", self.on_dbfs), self.config["profile_name"], self.on_dbfs
            )
        except KeyError as error:
            print(f"{error} is mandatory. Please provide it in the config.")
            return False

        # Setup the logger for DQF
        self._setup_logger(filename)

        # Write Log Info and Console for checking config file
        self.logger.info("Checking for issues with the config file...\n")
        print("Checking for issues with the config file...\n")

        # Validate the current config-file and return ``False``` if failures appear
        if not self._validate_config():
            return False

        # Write Log Info and Console for checking config file succeeded
        self.logger.info("\nNo issues found with the config file. Running the DQF now...\n\n")
        print("\nNo issues found with the config file. Running the DQF now...\n\n")

        # Replace the table-content of config if a dataframe is passed into DQF CheckProject
        if self.dataframe is not None:
            for check in self.config["checks"]:
                check_to_replace = get_dqf_class_from_name(check["type"])
                check_to_replace.replace_dataframe(
                    check_config=check, dataframe=self.dataframe, foreign_dataframe=self.foreign_dataframe
                )

        # Iterate through the check configuration
        self.check_results = self._process_checks(self.config)

        # Iterate through the stats configuration
        self.stats_results = self._process_stats(self.config)

        # if argument write_results is true (standard) results will be written
        if self.write_results:
            self.export_check_results(self.config, self.check_results, filename)

        # Calculate the end time of the check run and print out
        end = datetime.strptime(datetime.now().replace(microsecond=0).isoformat(" "), "%Y-%m-%d %H:%M:%S")
        print(f" Total Time taken by check run is {end - start}[hh:mm:ss]")

        if self.html_write_report:
            overall_result = all(dictionary["Result"] for dictionary in self.check_results)

            # Writing HTML report for the run
            if self.write_html_report(self.config, self.on_dbfs, self.html_report_path, overall_result, self.check_results):
                # Write Log Info and Console for checking if HTML file writing succeeded
                self.logger.info("\n\nSuccessfully created the HTML report.\n\n")
                print("\n\nSuccessfully created the HTML report.\n\n")
            else:
                self.logger.info("\n\nHTML report creation failed, please enter a valid path.\n\n")
                print("\n\nHTML report creation failed, please enter a valid path.\n\n")

        # Remove Logger Handler to finish log-writing
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)

        # Return a consumption of all checks -> if all checks are true, returns true,
        # if one or more checks are false this will return false
        return all(dictionary["Result"] for dictionary in self.check_results)


def get_project_version() -> str:
    """
    Get DQF version.
    """
    for dist in importlib_metadata.distributions():
        try:
            relative = pathlib.Path(__file__).relative_to(dist.locate_file(""))
        except ValueError:
            pass
        else:
            if pathlib.Path(relative) in [pathlib.Path(i) for i in dist.files]:
                return dist.metadata["Version"]


def get_install_path() -> str:
    """
    Get DQF installation path.
    """
    dqf_install_path = os.environ.get("DQF_INSTALL_PATH")

    if not dqf_install_path:
        print("No installation found, please check your Cluster or Installation of Data Quality Framework")

    return dqf_install_path


def dqf_help() -> None:
    """
    Get help on DQF like DQF version and source code repo.
    """
    print("Data Quality Framework")
    print(get_project_version())
    print("https://git.daimler.com/cocbigdatard/DataQualityFramework")
