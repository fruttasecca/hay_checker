"""
Class that contains different _metrics to compute, can be run multiple times on
multiple datasets, its meant to store a list of metrics that you wish to compute
on different dataframes or by trying to run those metrics on the same pass on data
to optimize.
"""
import copy

from haychecker._common.config import Config


class _Task(object):
    def __init__(self, metrics_params=[]):
        """
        Init the class, adding metrics_params to the
        stuff to compute if a list of metrics parameters (dictionaries) is passed.
        Metrics parameters are checked for correctness, an assertion error will interrupt
        the program if any incorrectness is met.

        :param metrics_params: List of metrics, each metric is a dictionary mapping params of
            the metric to a value, as in the json config file.
        :type metrics_params: list
        """
        Config._process_metrics(metrics_params)
        self._metrics = metrics_params

    def run(self, df):
        """
        Method that must be implemented by classes extending _Task, to run the metrics on the specified df.
        Eventual checks on the existence of columns with a certain name, format of values etc, are left to this method,
        same for checks on the df, like the type of columns, etc.

        :param df: Dataframe on which to run the task.
        :type df: DataFrame
        :return: A list of metrics that is the same as the metrics contained in the instance of this
            Task class, each metric will have new field named 'scores', containing results related to that metric.
        """
        raise NotImplementedError

    def get_metrics(self):
        """
        Returns a list of metrics currently part of the task, changing the returned
        list or any returned metric (a dictionary) will have no effect on the task, because the returned list
        is a deepcopy of the original.
        :return: List of metrics (a copy), which are dict mapping params to values.
        :rtype: list
        """
        # copy everything so that changing those has no effect on the _Task instance
        return copy.deepcopy(self._metrics)

    def add(self, input):
        """
        Adds more metrics to the task, the input can be of:
        - a _Task class or any class extending it
        - a dict containing metric params, or a list of those
        :param input: a _Task class or a class inheriting from it, a list of metrics params or a single metric param
        :type input dict/list/Task
        :return: Returns a reference to this instance of _Task, for chaining.
        """
        if type(input) is list:
            Config._process_metrics(input)
            self._metrics.extend(input)
        elif type(input) is dict:
            Config._process_metrics([input])
            self._metrics.append(input)
        elif isinstance(input, _Task):
            # metrics should already have been processed, no need to check here
            self._metrics.extend(input._metrics)
        else:
            "Input must be either a list of dicts, a dict or another Task instance"
            exit()
        return self
