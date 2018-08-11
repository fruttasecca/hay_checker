"""
Configuration file reader, the configuration file is a json file
which describes all parameters for the script, from specifying the input file
to specific data conditions to be checked for quality.
"""

import json
import os  # to check on paths
import re  # to check on values for date or time formats


class Config(object):
    # format : regex that the value must match
    __allowed_date_formats = {"dd/mm/YYYY": "\d\d/\d\d/\d\d\d\d"}
    __allowed_time_formats = {"HH:MM:ss": "\d\d:\d\d:\d\d"}
    __allowed_operators = ["eq", "gt", "lt"]

    def __init__(self, config_path):
        # import dict
        self._config = json.load(open(config_path, "r"))

        self._args = dict()  # arguments, like table path, delimiter, etc

        assert type(self._config) is dict, "Config file should contain a dictionary."

        # store _args in self._args, metrics in self.metrics
        self._process__args(self._config)
        self._process_metrics(self._config["metrics"])
        self._args["metrics"] = self._config["metrics"]

        if self._args["verbose"]:
            self._announce_config()

    def _announce_config(self):
        print("Running with the following configuration:")
        for arg in sorted(self._args):
            print("%s: %s" % (arg, self._args[arg] if arg != "metrics" else len(self._args[arg])))

    @staticmethod
    def _process_metrics(metrics):
        """
        Given a list of metrics wich are in dicts,  check for their correctness, this will rise
        an assertion error if any incorrectness is met
        :param metrics: List of metrics in dict form.
        """
        allowed_metrics = ["completeness", "freshness", "timeliness", "deduplication", "constraint", "rule",
                           "groupRule"]
        for i, metric in enumerate(metrics):
            # check that is has a metric name and that the name is allowed
            assert type(metric) is dict and "metric" in metric, "Metric %i has no 'metric' field" % i
            name = metric["metric"]
            assert name in allowed_metrics, "Metric %i '%s' is unknown." % (i + 1, name)

            error_msg = "Erroneous definition in metric %i" % (i + 1)
            if name == "completeness":
                Config._completeness_check(metric, error_msg)
            elif name == "freshness":
                Config._freshness_check(metric, error_msg)
            elif name == "timeliness":
                Config._timeliness_check(metric, error_msg)
            elif name == "deduplication":
                Config._deduplication_check(metric, error_msg)
            elif name == "constraint":
                Config._constraint_check(metric, error_msg)
            elif name == "rule":
                Config._rule_check(metric, error_msg)
            elif name == "groupRule":
                Config._grouprule_check(metric, error_msg)

    @staticmethod
    def _completeness_check(metric, error_msg):
        """
        Check the definition of a completeness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Completeness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert len(metric) == 1 or (len(metric) == 2 and "columns" in metric), error_msg
        if len(metric) == 2:
            columns = metric["columns"]
            assert type(columns) is list or type(columns) is str, error_msg
            if type(columns) is list:
                for col in columns:
                    assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _freshness_check(metric, error_msg):
        """
        Check the definition of a freshness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Freshness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert len(metric) == 3 and "columns" in metric and (
                "dateFormat" in metric or "timeFormat" in metric), error_msg

        # check columns for validity
        columns = metric["columns"]
        assert type(columns) is list or type(columns) is str, error_msg
        if type(columns) is list:
            for col in columns:
                assert type(col) is int or type(col) is str, error_msg

        # check date/time format for validity
        if "dateFormat" in metric:
            assert metric["dateFormat"] in Config.__allowed_date_formats, error_msg
        else:
            assert metric["timeFormat"] in Config.__allowed_time_formats, error_msg

    @staticmethod
    def _timeliness_check(metric, error_msg):
        """
        Check the definition of a timeliness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Timeliness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert len(metric) == 4 and "columns" in metric and (
                "dateFormat" in metric or "timeFormat" in metric) and "value" in metric, error_msg

        # check columns for validity
        columns = metric["columns"]
        assert type(columns) is list or type(columns) is str, error_msg
        if type(columns) is list:
            for col in columns:
                assert type(col) is int or type(col) is str, error_msg

        # check date/time format and value for validity
        if "dateFormat" in metric:
            assert metric["dateFormat"] in Config.__allowed_date_formats, error_msg
            assert bool(re.match(Config.__allowed_date_formats[metric["dateFormat"]], metric["value"])), error_msg
        else:
            assert metric["timeFormat"] in Config.__allowed_time_formats, error_msg
            assert bool(re.match(Config.__allowed_time_formats[metric["timeFormat"]], metric["value"])), error_msg

    @staticmethod
    def _deduplication_check(metric, error_msg):
        """
        Check the definition of a deduplication metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Deduplication metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert len(metric) == 1 or (len(metric) == 2 and "columns" in metric), error_msg
        if len(metric) == 2:
            columns = metric["columns"]
            assert type(columns) is list or type(columns) is str, error_msg
            if type(columns) is list:
                for col in columns:
                    assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _constraint_check(metric, error_msg):
        """
        Check the definition of a Constraint metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Constraint metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert "when" in metric and "then" in metric, error_msg
        assert len(metric) == 3 or (len(metric) == 4 and "conditions" in metric), error_msg

        # check on when field
        when = metric["when"]
        assert type(when) is list, error_msg
        for w in when:
            assert type(w) is int or type(w) is str, error_msg

        # check on then field
        then = metric["then"]
        assert type(then) is list, error_msg
        for t in then:
            assert type(t) is int or type(t) is str, error_msg

        # check on conditions if there are
        if "conditions" in metric:
            conditions = metric["conditions"]
            assert type(conditions) is list, error_msg
            for cond in conditions:
                assert type(cond) is dict and len(cond) == 3, error_msg
                assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
                assert "operator" in cond and (
                            type(cond["operator"]) is str and cond["operator"] in Config.__allowed_operators), error_msg
                assert "value" in cond and (type(cond["value"]) is str or type(cond["value"]) is int)

    @staticmethod
    def _rule_check(metric, error_msg):
        """
        Check the definition of a rule metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: rule metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert len(metric) == 2 and "conditions" in metric, error_msg

        conditions = metric["conditions"]
        assert type(conditions) is list, error_msg
        for cond in conditions:
            assert type(cond) is dict and len(cond) == 3, error_msg
            assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
            assert "operator" in cond and (
                    type(cond["operator"]) is str and cond["operator"] in Config.__allowed_operators), error_msg
            assert "value" in cond and (type(cond["value"]) is str or type(cond["value"]) is int)

    @staticmethod
    def _grouprule_check(metric, error_msg):
        """
        Check the definition of a groupRule metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: groupRule metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert "columns" in metric and "conditions" in metric, error_msg
        assert len(metric) == 3 or (len(metric) == 4 and "having"), error_msg

        # check columns for validity
        columns = metric["columns"]
        assert type(columns) is list or type(columns) is str, error_msg
        if type(columns) is list:
            for col in columns:
                assert type(col) is int or type(col) is str, error_msg

        # check 'having' conditions
        having = metric["having"]
        assert type(having) is list, error_msg
        for have in having:
            assert type(have) is dict and len(have) == 3, error_msg
            assert "column" in have and (type(have["column"]) is str or type(have["column"]) is int)
            assert "operator" in have and (
                    type(have["operator"]) is str and have["operator"] in Config.__allowed_operators), error_msg
            assert "value" in have and (type(have["value"]) is str or type(have["value"]) is int)

        # check conditions
        conditions = metric["conditions"]
        assert type(conditions) is list, error_msg
        for cond in conditions:
            assert type(cond) is dict and len(cond) == 3, error_msg
            assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
            assert "operator" in cond and (
                    type(cond["operator"]) is str and cond["operator"] in Config.__allowed_operators), error_msg
            assert "value" in cond and (type(cond["value"]) is str or type(cond["value"]) is int)

    def _process__args(self, config):
        """
        Given a dict containing the configuration to run with, this method is going to check the
        arguments and assign them to the class instance internal dict if they are valid, otherwise the program will
        stop running through an assertion error.
        Required arguments (table, inferSchema, output, metrics) have no default value, optional arguments
        (delimiter, header, threads) have default values (',', True, 1).

        :param config: Dict with config parameters of the program.
        """

        # check for unknown _args
        allowed_args = ["table", "inferSchema", "output", "metrics", "delimiter", "header", "threads", "verbose"]
        for key in config:
            assert key in allowed_args, "Argument '%s' is unknown." % key

        # check required arguments
        assert "table" in config, "Missing tablePath in _config file."
        assert type(config["table"]) is str, "table should be a string representing the path/url/source."
        self._args["table"] = config["table"]

        assert "inferSchema" in config, "Missing inferSchema in _config file."
        assert type(config["inferSchema"]) is bool, "inferSchema should be a boolean."
        self._args["inferSchema"] = config["inferSchema"]

        assert "output" in config, "Missing output in _config file."
        assert type(config["output"]) is str, "output should be a string representing the path where results " \
                                              "should be saved. "
        # check that output can be written
        assert os.access(os.path.dirname(config["output"]), os.W_OK), "Cannot write to output path."
        self._args["output"] = config["output"]

        assert "metrics" in config, "Missing metrics list in _config file"
        assert type(config["metrics"]) is list, "metrics should be mapped to a list of metrics"
        assert len(config["metrics"]), "metrics list is empty"

        # check optional arguments
        if "delimiter" in config:
            assert type(config["delimiter"]) is str
            self._args["delimiter"] = config["delimiter"]
        else:
            self._args["delimiter"] = ","

        if "header" in config:
            assert type(config["header"]) is bool
            self._args["header"] = config["header"]
        else:
            self._args["header"] = True

        if "threads" in config:
            assert type(config["threads"]) is int
            self._args["threads"] = config["threads"]
        else:
            self._args["threads"] = "1"

        if "verbose" in config:
            assert type(config["verbose"]) is bool
            self._args["verbose"] = config["verbose"]
        else:
            self._args["verbose"] = False

    def __getitem__(self, item):
        """
        Make _config act as a dictionary
        :param item: Key to retrieve item (i.e. the table path)
        :return: Item mapped to Key, an exception is returned if not present.
        """
        return self._args[item]
