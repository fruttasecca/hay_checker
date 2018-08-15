"""
Configuration file reader, the configuration file is a json file
which describes all parameters for the script, from specifying the input file
to specific data conditions to be checked for quality.
"""

import json
import os  # to check on paths
import copy  # to deepcopy returned items from __get_item__


class Config(object):
    _allowed_operators = ["eq", "gt", "lt"]

    def __init__(self, config_path):
        # import dict and process/check for correctness
        self._config = json.load(open(config_path, "r"))
        assert type(self._config) is dict, "Config file should contain a dictionary."
        self._process_args(self._config)
        self._process_metrics(self._config["metrics"])

        if self._config["verbose"]:
            self._announce_config()

    def _announce_config(self):
        print("Running with the following configuration:")
        for arg in sorted(self._config):
            print("%s: %s" % (arg, self._config[arg] if arg != "metrics" else len(self._config[arg])))

    @staticmethod
    def _process_metrics(metrics):
        """
        Given a list of metrics parameters, wich are in dicts,  check for their correctness, this will rise
        an assertion error if any incorrectness is met.
        :param metrics: List of metrics in dict form.
        """
        allowed_metrics = ["completeness", "freshness", "timeliness", "deduplication", "constraint", "rule",
                           "groupRule"]
        for i, metric in enumerate(metrics):
            # check that it has a metric name and that the name is allowed
            assert type(metric) is dict and "metric" in metric, "Metric %i has no 'metric' field" % i
            name = metric["metric"]
            assert name in allowed_metrics, "Metric %i '%s' is unknown." % (i + 1, name)

            error_msg = "Erroneous definition in metric %i" % (i + 1)
            if name == "completeness":
                Config._completeness_params_check(metric, error_msg)
            elif name == "freshness":
                Config._freshness_params_check(metric, error_msg)
            elif name == "timeliness":
                Config._timeliness_params_check(metric, error_msg)
            elif name == "deduplication":
                Config._deduplication_params_check(metric, error_msg)
            elif name == "constraint":
                Config._constraint_params_check(metric, error_msg)
            elif name == "rule":
                Config._rule_params_check(metric, error_msg)
            elif name == "groupRule":
                Config._grouprule_params_check(metric, error_msg)

    @staticmethod
    def _completeness_params_check(metric, error_msg):
        """
        Check the definition of a completeness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Completeness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert "completeness" in metric, error_msg
        assert len(metric) == 1 or (len(metric) == 2 and "columns" in metric), error_msg
        if len(metric) == 2:
            columns = metric["columns"]
            assert len(columns) > 0, "Columns list is empty"
            assert type(columns) is list, error_msg
            for col in columns:
                assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _freshness_params_check(metric, error_msg):
        """
        Check the definition of a freshness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Freshness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert "freshness" in metric, error_msg
        assert len(metric) == 3 and "columns" in metric and (
                "dateFormat" in metric or "timeFormat" in metric), error_msg

        if "dateFormat" in metric:
            assert type(metric["dateFormat"]) is str
        else:
            assert type(metric["timeFormat"]) is str

        # check columns for validity
        columns = metric["columns"]
        assert len(columns) > 0, "Columns list is empty"
        assert type(columns) is list, error_msg
        for col in columns:
            assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _timeliness_params_check(metric, error_msg):
        """
        Check the definition of a timeliness metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Timeliness metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert "timeliness" in metric, error_msg
        assert len(metric) == 4 and "columns" in metric and (
                "dateFormat" in metric or "timeFormat" in metric) and "value" in metric, error_msg

        if "dateFormat" in metric:
            assert type(metric["dateFormat"]) is str
        else:
            assert type(metric["timeFormat"]) is str

        assert type(metric["value"]) is str

        # check columns for validity
        columns = metric["columns"]
        assert len(columns) > 0, "Columns list is empty"
        assert type(columns) is list, error_msg
        for col in columns:
            assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _deduplication_params_check(metric, error_msg):
        """
        Check the definition of a deduplication metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Deduplication metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert "deduplication" in metric, error_msg
        assert len(metric) == 1 or (len(metric) == 2 and "columns" in metric), error_msg
        if len(metric) == 2:
            columns = metric["columns"]
            assert len(columns) > 0, "Columns list is empty"
            assert type(columns) is list, error_msg
            for col in columns:
                assert type(col) is int or type(col) is str, error_msg

    @staticmethod
    def _constraint_params_check(metric, error_msg):
        """
        Check the definition of a Constraint metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: Constraint metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert "constraint" in metric, error_msg
        assert "when" in metric and "then" in metric, error_msg
        assert len(metric) == 3 or (len(metric) == 4 and "conditions" in metric), error_msg

        # check on when field
        when = metric["when"]
        assert type(when) is list, error_msg
        assert len(when) > 0, error_msg
        for w in when:
            assert type(w) is int or type(w) is str, error_msg

        # check on then field
        then = metric["then"]
        assert type(then) is list, error_msg
        assert len(then) > 0, error_msg
        for t in then:
            assert type(t) is int or type(t) is str, error_msg

        # check on conditions if there are
        if "conditions" in metric:
            conditions = metric["conditions"]
            assert len(conditions) > 0, "Conditions list is empty"
            assert type(conditions) is list, error_msg
            for cond in conditions:
                assert type(cond) is dict and len(cond) == 3, error_msg
                assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
                assert "operator" in cond and (
                        type(cond["operator"]) is str and cond["operator"] in Config._allowed_operators), error_msg
                assert "value" in cond and (
                        type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"] is float))

    @staticmethod
    def _rule_params_check(metric, error_msg):
        """
        Check the definition of a rule metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: rule metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """
        assert "rule" in metric, error_msg
        assert len(metric) == 2 and "conditions" in metric, error_msg

        conditions = metric["conditions"]
        assert type(conditions) is list, error_msg
        assert len(conditions) > 0, "Conditions list is empty"
        for cond in conditions:
            assert type(cond) is dict and len(cond) == 3, error_msg
            assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
            assert "operator" in cond and (
                    type(cond["operator"]) is str and cond["operator"] in Config._allowed_operators), error_msg
            assert "value" in cond and (
                        type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"]) is float)

    @staticmethod
    def _grouprule_params_check(metric, error_msg):
        """
        Check the definition of a groupRule metric for consistency, this will
        rise an assertion error if any error is met.

        :param metric: groupRule metric in a dict form.
        :param error_msg: Error message to return in case of error.
        """

        assert "rule" in metric, error_msg
        assert "columns" in metric and "having" in metric, error_msg
        assert len(metric) == 3 or (len(metric) == 4 and "conditions" in metric), error_msg

        # check columns for validity
        columns = metric["columns"]
        assert type(columns) is list or type(columns) is str, error_msg
        assert len(columns) > 0, error_msg
        for col in columns:
            assert type(col) is int or type(col) is str, error_msg

        having = metric["having"]
        assert type(having) is list, error_msg
        assert len(having) > 0, error_msg
        for have in having:
            assert type(have) is dict and len(have) == 3, error_msg
            assert "column" in have and (type(have["column"]) is str or type(have["column"]) is int)
            assert "operator" in have and (
                    type(have["operator"]) is str and have["operator"] in Config._allowed_operators), error_msg
            assert "value" in have and (
                        type(have["value"]) is str or type(have["value"]) is int or type(have["value"]) is float)

        # check conditions
        if "conditions" in metric:
            conditions = metric["conditions"]
            assert type(conditions) is list, error_msg
            assert len(conditions) > 0, error_msg
            for cond in conditions:
                assert type(cond) is dict and len(cond) == 3, error_msg
                assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
                assert "operator" in cond and (
                        type(cond["operator"]) is str and cond["operator"] in Config._allowed_operators), error_msg
                assert "value" in cond and (
                            type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"]) is float)

    def _process_args(self):
        """
        Check the arguments in the self._config dict, stop running through an assertion error if an incorrectness
        is met.
        Required arguments (table, inferSchema, output, metrics) have no default value, optional arguments
        (delimiter, header, threads) have default values (',', True, 1).
        """

        # check for unknown _config
        allowed_args = ["table", "inferSchema", "output", "metrics", "delimiter", "header", "threads", "verbose"]
        for key in self._config:
            assert key in allowed_args, "Argument '%s' is unknown." % key

        # check required arguments
        assert "table" in self._config, "Missing tablePath in _config file."
        assert type(self._config["table"]) is str, "table should be a string representing the path/url/source."

        assert "inferSchema" in self._config, "Missing inferSchema in _config file."
        assert type(self._config["inferSchema"]) is bool, "inferSchema should be a boolean."

        assert "output" in self._config, "Missing output in _config file."
        assert type(self._config["output"]) is str, "output should be a string representing the path where results " \
                                                    "should be saved. "
        # check that output can be written
        assert os.access(os.path.dirname(self._config["output"]), os.W_OK), "Cannot write to output path."
        self._config["output"] = self._config["output"]

        assert "metrics" in self._config, "Missing metrics list in _config file"
        assert type(self._config["metrics"]) is list, "metrics should be mapped to a list of metrics"
        assert len(self._config["metrics"]), "metrics list is empty"

        # check optional arguments, assign default values if missing
        if "delimiter" in self._config:
            assert type(self._config["delimiter"]) is str
        else:
            self._config["delimiter"] = ","

        if "header" in self._config:
            assert type(self._config["header"]) is bool
        else:
            self._config["header"] = True

        if "threads" in self._config:
            assert type(self._config["threads"]) is int
        else:
            self._config["threads"] = "1"

        if "verbose" in self._config:
            assert type(self._config["verbose"]) is bool
        else:
            self._config["verbose"] = False

    def __getitem__(self, item):
        """
        To access config items as if this class was a dictionary, note
        that returned items are a copy, changing those will not affect the instance
        of the Config class.
        :param item: Key to retrieve item (i.e. the table path)
        :return: Item (copy) mapped to Key, an exception is returned if not present.
        """
        return self._config[copy.deepcopy(item)]
