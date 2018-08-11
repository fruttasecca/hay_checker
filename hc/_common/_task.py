"""
Class that contains different _metrics to compute, can be run multiple times on
multiple datasets, its meant to store set of _metrics that you wish to compute
on different dataframes.
"""
import copy

from hc._common.config_reader import Config


class _Task(object):
    def __init__(self, metrics_params=[]):
        """
        Init the class, adding metrics_params to the
        stuff to compute if any metric parameters dictionary is passed.

        :param metrics_params: List of metrics, each metric is a dictionary mapping params of
        the metric to a value, as in the json config file.
        """
        Config._process_metrics(metrics_params)
        self.__metrics = metrics_params

    def __run(self, metrics_list, df):
        """
        Method that must be implemented by classes extending _Task, given a list containing the metrics to run,
        returns the list of metrics with an added key, their result.
        Both input and output dict must be independent from the framework actually running the computation (spark, etc.).
        Eventual checks on the existence of columns with a certain name, format of values etc, are left to this method,
        same for checks on checks on the df, like the type, etc.
        :param metrics_list: List containing metrics to run, each metric is itself a dict where keys are parameters,
        mapped to their values, as in the jscon config file.
        :param df: Dataframe on which to run the task.
        :return: Dict containing results.
        TODO: Expand on the dict returned.
        """
        raise NotImplementedError

    def get_metrics(self):
        """
        Returns a list of metrics currently part of the task, changing the returned
        list or any returned metric (a dictionary) will have no effect on the task.
        :return: List of metrics, which are dict mapping params to values.
        """
        # copy everything so that changing those has no effect on the _Task instance
        result = []
        for metric in self.__metrics:
            result.append(copy.deepcopy(metric))
        return result

    def run(self, df):
        """
        Run the task on a given dataframe.
        :param df: Dataframe on which to run the task.
        :return: List of metrics with an hadded
        """
        filtered_metrics = self.__filter_metrics(self.__metrics)
        raw_results = self.__run(filtered_metrics, df)
        # assert type(raw_results) is dict, "__Run is not returning a dict as required."
        output = self._format_results(raw_results)
        return output

    def _format_results(self, raw_results):
        return raw_results

    def add(self, input):
        """
        Adds more metrics to the task, the input can be of:
        - a _Task class or any class extending it
        - a dict containing metric params, or a list of those
        :param input: a _Task class, a list of metric params or a single metric param
        :return: Returns a reference to this instance of _Task, for chaining.
        """
        if type(input) is list:
            Config._process_metrics(input)
            self.__metrics.extend(input)
        elif type(input) is dict:
            Config._process_metrics([input])
            self.__metrics.append(input)
        elif type(input) is _Task:
            # metrics should already have been processed, no need to check here
            self.__metrics.extend(input.__metrics)

    def __filter_metrics(self, metrics_list):
        """
        Filters metrics, checking for repetitions and removing them, etc.
        :param metrics_list: List of metrics.
        :return: A dict having as keys metrics types ('completeness', etc), mapped to lists of stuff to do.
        TODO: Expand on the dict returned.
        """
        return metrics_list


