"""
Class extending the _Task class from the common
scripts. It contains metrics to run that can be run
on different data.
"""

import copy

from pyspark.sql.functions import count

from hc._common._task import _Task as _Task
from hc.dhc import _util as util
from hc.dhc import metrics as m


class Task(_Task):
    def __init__(self, metrics_params=[]):
        """
        Init the class, adding _metrics to the
        stuff to compute if any metric parameters dictionary is passed.

        :param metrics_params: List of _metrics, each metric is a dictionary mapping params of
        the metric to a value, as in the json config file.
        """
        super().__init__(metrics_params)

    def __run(self, metrics, df):
        todo = []
        needs_count_all = False
        """
        For each metric __util params then add the computation to do
        to the list.
        """
        for metric in metrics:
            if metric["metric"] == "completeness":
                needs_count_all = True
                columns = metric.get("columns", None)
                util._completeness_check(columns, df)
                todo.extend(m._completeness_todo(columns, df))
            elif metric["metric"] == "deduplication":
                needs_count_all = True
                columns = metric.get("columns", None)
                util._deduplication_check(columns, df)
                todo.extend(m._deduplication_todo(columns, df))
            elif metric["metric"] == "timeliness":
                needs_count_all = True
                columns = metric.get("columns")
                value = metric.get("value")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util._timeliness_check(columns, value, df, dateFormat, timeFormat)
                todo.extend(m._timeliness_todo(columns, value, df, dateFormat, timeFormat))
            elif metric["metric"] == "freshness":
                columns = metric.get("columns")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util._freshness_check(columns, df, dateFormat, timeFormat)
                todo.extend(m._freshness_todo(columns, df, dateFormat, timeFormat))
            else:
                print("Metric %s not recognized" % metric["metric"])
                exit()
        if needs_count_all:
            todo.append(count("*"))
        collected = df.agg(*todo).collect()[0]
        results = self._map_results_to_metrics(metrics, collected, needs_count_all, df)
        return results

    def _map_results_to_metrics(self, metrics, collected, has_count_all, df):
        index = 0
        metrics = copy.deepcopy(metrics)
        total_rows = collected[-1] if has_count_all else None
        total_columns = len(df.columns)

        for metric in metrics:
            if metric["metric"] == "completeness":
                ncolumns = len(metric.get("columns", []))
                normalizer = total_rows if ncolumns > 0 else total_rows * total_columns
                if ncolumns == 0:
                    # aggregate over all columns of the table
                    scores = 0
                    for _ in range(total_columns):
                        scores += (collected[index] / normalizer)
                        index += 1
                    metric["scores"] = [scores * 100]
                else:
                    # aggregate over columns parameter
                    scores = []
                    for _ in range(ncolumns):
                        scores.append((collected[index] / normalizer) * 100)
                        index += 1
                    metric["scores"] = scores
            elif metric["metric"] == "deduplication":
                """
                Using ["placeholder"] because no columns means counting distinct over the tuples of the table, so 
                there is one column to collect, and not zero.
                """
                ncolumns = len(metric.get("columns", ["placeholder"]))
                scores = []
                for _ in range(ncolumns):
                    scores.append((collected[index] / total_rows) * 100)
                    index += 1
                metric["scores"] = scores

            elif metric["metric"] == "timeliness":
                ncolumns = len(metric.get("columns"))
                scores = []
                for _ in range(ncolumns):
                    scores.append((collected[index] / total_rows) * 100)
                    index += 1
                metric["scores"] = scores
            elif metric["metric"] == "freshness":
                ncolumns = len(metric.get("columns"))
                scores = []
                for _ in range(ncolumns):
                    scores.append((collected[index]))
                    index += 1
                metric["scores"] = scores
            else:
                print("Metric %s not recognized" % metric["metric"])
                exit()
        return metrics

