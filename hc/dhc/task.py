"""
Class extending the _Task class from the common
scripts. It contains metrics to run that can be run
on different data.
"""

import copy
from operator import itemgetter

from pyspark.sql.functions import count

from .._common._task import _Task
from . import _util as util
from . import metrics as m


class Task(_Task):
    def __init__(self, metrics_params=[]):
        """
        Init the class, adding _metrics to the
        stuff to compute if any metric parameters dictionary is passed.

        :param metrics_params: List of _metrics, each metric is a dictionary mapping params of
        the metric to a value, as in the json config file.
        """
        super().__init__(metrics_params)

    @staticmethod
    def _perform_run_checks(metrics, df):
        for metric in metrics:
            if metric["metric"] == "completeness":
                columns = metric.get("columns", None)
                util.completeness_run_check(columns, df)
            elif metric["metric"] == "deduplication":
                columns = metric.get("columns", None)
                util.deduplication_run_check(columns, df)
            elif metric["metric"] == "timeliness":
                columns = metric.get("columns")
                value = metric.get("value")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util.timeliness_run_check(columns, value, df, dateFormat, timeFormat)
            elif metric["metric"] == "freshness":
                columns = metric.get("columns")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util.freshness_run_check(columns, df, dateFormat, timeFormat)
            elif metric["metric"] == "rule":
                conditions = metric["conditions"]
                util.rule_run_check(conditions, df)
            elif metric["metric"] == "constraint":
                when = metric["when"]
                then = metric["then"]
                conditions = metric.get("conditions", None)
                util.constraint_run_check(when, then, conditions, df)
            elif metric["metric"] == "groupRule":
                columns = metric["columns"]
                conditions = metric.get("conditions", None)
                having = metric["having"]
                util.grouprule_run_check(columns, conditions, having, df)
            else:
                print("Metric %s not recognized" % metric["metric"])
                exit()

    def run(self, df):
        """
        For each metric check parameter for run time correctness (i.e. column with those name
         existing in the df, etc.), then perform the required computations.
        """
        metrics = copy.deepcopy(self._metrics)
        # run time checks on every metric before starting
        self._perform_run_checks(metrics, df)

        # get stuff to do for metrics that can be run together in a single pass
        todo = []
        needs_count_all = False  # if this metrics requires a count('*') to be performed
        simple_metrics = []  # these will be run in a single pass, together
        # these will be run one at a time
        grouprules = []
        constraints = []

        for i, metric in enumerate(metrics):
            if metric["metric"] == "completeness":
                metric["_task_id"] = i
                needs_count_all = True
                columns = metric.get("columns", None)
                todo.extend(m._completeness_todo(columns, df))
                simple_metrics.append(metric)
            elif metric["metric"] == "deduplication":
                metric["_task_id"] = i
                needs_count_all = True
                columns = metric.get("columns", None)
                todo.extend(m._deduplication_todo(columns, df))
                simple_metrics.append(metric)
            elif metric["metric"] == "timeliness":
                metric["_task_id"] = i
                needs_count_all = True
                columns = metric.get("columns")
                value = metric.get("value")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                todo.extend(m._timeliness_todo(columns, value, df, dateFormat, timeFormat))
                simple_metrics.append(metric)
            elif metric["metric"] == "freshness":
                metric["_task_id"] = i
                columns = metric.get("columns")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                todo.extend(m._freshness_todo(columns, df, dateFormat, timeFormat))
                simple_metrics.append(metric)
            elif metric["metric"] == "rule":
                metric["_task_id"] = i
                needs_count_all = True
                conditions = metric["conditions"]
                todo.extend(m._rule_todo(conditions))
                simple_metrics.append(metric)
            elif metric["metric"] == "constraint":
                metric["_task_id"] = i
                constraints.append(metric)
            elif metric["metric"] == "groupRule":
                metric["_task_id"] = i
                grouprules.append(metric)

        if needs_count_all:
            todo.append(count("*"))

        # run and add results to the simple metrics
        collected = df.agg(*todo).collect()[0]
        self._add_scores_to_metrics(simple_metrics, collected, needs_count_all, df)

        # run constraints, one at a time
        for constraint in constraints:
            when = constraint["when"]
            then = constraint["then"]
            conditions = constraint.get("conditions", None)
            todo = m._constraint_todo(when, then, conditions, df)
            # get first row, first element of that row, multiply by 100:w
            constraint["scores"] = [list(todo.collect()[0])[0] * 100]

        # run groupRule, one at a time
        for grouprule in grouprules:
            columns = grouprule["columns"]
            having = grouprule["having"]
            conditions = grouprule.get("conditions", None)
            todo = m._grouprule_todo(columns, conditions, having, df)
            grouprule["scores"] = [list(todo.collect()[0])[0] * 100]

        # sort metrics and return them after removing the id
        metrics = simple_metrics + constraints + grouprules
        metrics = sorted(metrics, key=itemgetter('_task_id'))
        for metric in metrics:
            del metric["_task_id"]
        return metrics

    @staticmethod
    def _add_scores_to_metrics(metrics, collected, has_count_all, df):
        index = 0
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
                if "dateFormat" in metric:
                    metric["scores"] = [int(score) for score in scores]
                elif "timeFormat" in metric:
                    metric["scores"] = [util._seconds_to_timeFormat(score) for score in scores]
            elif metric["metric"] == "rule":
                scores = [(collected[index] / total_rows) * 100]
                index += 1
                metric["scores"] = scores
        return metrics
