"""
Class extending the _Task class from the common
scripts. It contains metrics that can be run
on different data.
"""

import copy
from operator import itemgetter

import numpy as np
import pyspark
from pyspark.sql.functions import count
from pyspark.sql.functions import isnan, count, col

from haychecker._common._task import _Task
from haychecker.dhc import _runtime_checks as util
from haychecker.dhc import metrics as m


class Task(_Task):
    """
    Class to contain defined metrics to run them on different data and/or at different times.
    An instance of it can contain any number of metrics to be run, trying
    to run them together/on the same pass on the data instead of one at a time when possible.
    Other tasks can be added to a Task, or
    metrics as a dict describing the metric can be added to a task, or a list of those metrics.
    Once run on a df, it returns a list of results, which are the list of contained metrics (in the same order)
    with a score field added for each metric, containing results/scores of that metric.
    """

    def __init__(self, metrics_params=[], allow_casting=True):
        """
        Init the class, adding _metrics to the
        stuff to compute if any metric parameters dictionary is passed.

        :param metrics_params: List of _metrics, each metric is a dictionary mapping params of
            the metric to a value, as in the json config file.
        :type metrics_params: list
        :param allow_casting: If a column not of the type of what it is evaluated against (i.e. a condition checking
            for column 'x' being gt 3.0, with the type of 'x' being string) should be casted to the type of the value
            it is checked against. If casting is not allowed the previous example would provoke an error, (through an
            assertion); default is True.
        :type allow_casting: bool
        """
        super().__init__(metrics_params)
        self._allow_casting = allow_casting

    @staticmethod
    def _columns_index_to_name(columns, idxdict):
        """
        Substitutes column indexes with column names, return the passed list of columns
        as a list of names only.

        :param columns:
        :type columns: list
        :param idxdict:
        :type idxdict: dict
        :return:
        :rtype: list
        """
        res = []
        for col in columns:
            if type(col) is int:
                assert 0 <= col < len(idxdict), "Column index '%s' out of range" % col
                res.append(idxdict[col])
            else:
                res.append(col)
        return res

    @staticmethod
    def _conditions_column_index_to_name(conditions, idxdict, typesdict, allow_casting):
        """
        Changes the conditions contained in the list in the following way:
        column indexes will be substituted by names, while column names (strings) will
        be untouched. If casting is allowed conditions which requires casting will
        have a new field 'casted_to' added, mapped to 'numeric'.

        :param conditions:
        :type conditions: list
        :param idxdict: Dict mapping column indexes to names
        :type idxdict: dict
        :param typesdict: Dict mapping column names to a type
        :type typesdict: dict
        :param allow_casting: If casting a column to a type matching the 'value' field in condition is allowed
        :type allow_casting: bool
        """
        for cond in conditions:
            col = cond["column"]
            if type(col) is int:
                assert 0 <= col < len(idxdict), "column index '%s' out of range" % col
                cond["column"] = idxdict[col]
            if cond["column"] != "*" and typesdict[cond["column"]] == "string" and type(cond["value"]) != str:
                assert allow_casting, "Type of column '%s' is string, but 'value' %s is of numeric type, and casting " \
                                      "is not allowed" % (
                                          col, cond["value"])
                vtype = type(cond["value"])
                cond["casted_to"] = "double" if vtype is float else "bigint"

    @staticmethod
    def _perform_run_checks(metrics, df, allow_casting):
        """
        Perform run time parameter checking.

        :param metrics:
        :type metrics: list
        :param df:
        :type df: DataFrame
        :param allow_casting:
        :type allow_casting: bool
        """

        # maps an index to a column name
        idxdict = dict(enumerate(df.columns))
        typesdict = dict(df.dtypes)

        for metric in metrics:
            if metric["metric"] == "completeness":
                if "columns" in metric:
                    metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                columns = metric.get("columns", None)
                util.completeness_run_check(columns, df)
            elif metric["metric"] == "deduplication":
                if "columns" in metric:
                    metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                columns = metric.get("columns", None)
                util.deduplication_run_check(columns, df)
            elif metric["metric"] == "deduplication_approximated":
                if "columns" in metric:
                    metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                columns = metric.get("columns", None)
                util.deduplication_approximated_run_check(columns, df)
            elif metric["metric"] == "timeliness":
                metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                columns = metric.get("columns")
                value = metric.get("value")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util.timeliness_run_check(columns, value, df, dateFormat, timeFormat)
            elif metric["metric"] == "freshness":
                metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                columns = metric.get("columns")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                util.freshness_run_check(columns, df, dateFormat, timeFormat)
            elif metric["metric"] == "rule":
                Task._conditions_column_index_to_name(metric["conditions"], idxdict, typesdict, allow_casting)
                util.rule_run_check(metric["conditions"], df)
            elif metric["metric"] == "constraint":
                when = Task._columns_index_to_name(metric["when"], idxdict)
                then = Task._columns_index_to_name(metric["then"], idxdict)
                metric["when"] = when
                metric["then"] = then
                if "conditions" in metric:
                    Task._conditions_column_index_to_name(metric["conditions"], idxdict, typesdict, allow_casting)
                conditions = metric.get("conditions", None)
                util.constraint_run_check(when, then, conditions, df)
            elif metric["metric"] == "groupRule":
                metric["columns"] = Task._columns_index_to_name(metric["columns"], idxdict)
                if "conditions" in metric:
                    Task._conditions_column_index_to_name(metric["conditions"], idxdict, typesdict, allow_casting)
                Task._conditions_column_index_to_name(metric["having"], idxdict, typesdict, allow_casting)
                columns = metric["columns"]
                conditions = metric.get("conditions", None)
                having = metric["having"]
                util.grouprule_run_check(columns, conditions, having, df)
            elif metric["metric"] == "entropy":
                metric["column"] = Task._columns_index_to_name([metric["column"]], idxdict)[0]
                util.entropy_run_check(metric["column"], df)
            elif metric["metric"] == "mutual_info":
                when = Task._columns_index_to_name([metric["when"]], idxdict)[0]
                then = Task._columns_index_to_name([metric["then"]], idxdict)[0]
                metric["when"] = when
                metric["then"] = then
                util.mutual_info_run_check(when, then, df)
            else:
                print("Metric %s not recognized" % metric["metric"])
                exit()

    def run(self, df):
        """
        For each metric check its parameters for run time correctness (i.e. column with those name
        existing in the df, etc.), then perform the required computations, return results as a list of
        dictionaries identical to the metrics contained in the task, each with a field "scores" added, mapped
        to a list of scores related to the metric.

        :param df: DataFrame
        :type df: DataFrame
        :return: List of metrics with their scores added as a field.
        :rtype: list
        """
        metrics = copy.deepcopy(self._metrics)
        # run time checks on every metric before starting, substitute column indexes with names
        self._perform_run_checks(metrics, df, self._allow_casting)

        typesdict = dict(df.dtypes)
        # get stuff to do for metrics that can be run together in a single pass
        todo = []
        needs_count_all = False  # if this metrics requires a count('*') to be performed
        simple_metrics = []  # these will be run in a single pass, together
        # these will be run one at a time
        grouprules = []
        constraints = []
        entropies = []
        mutual_infos = []

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
            elif metric["metric"] == "deduplication_approximated":
                metric["_task_id"] = i
                needs_count_all = True
                columns = metric.get("columns", None)
                todo.extend(m._deduplication_approximated_todo(columns, df))
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
            elif metric["metric"] == "entropy":
                metric["_task_id"] = i
                entropies.append(metric)
            elif metric["metric"] == "mutual_info":
                metric["_task_id"] = i
                mutual_infos.append(metric)

        if needs_count_all:
            todo.append(count("*"))

        # replace nans with null so that checks like nan > 0 will go false
        numerics = ["float", "bigint", "double", "int", "long"]
        for dcol in df.columns:
            if typesdict[dcol] in numerics:
                df = df.withColumn(dcol, pyspark.sql.functions.when(isnan(col(dcol)), None).otherwise(col(dcol)))
        # run and add results to the simple metrics
        if len(simple_metrics) > 0:
            collected = df.agg(*todo).collect()[0]
            self._add_scores_to_metrics(simple_metrics, collected, needs_count_all, df)

        # run constraints, one at a time
        for constraint in constraints:
            when = constraint["when"]
            then = constraint["then"]
            conditions = constraint.get("conditions", None)
            todo = m._constraint_todo(when, then, conditions, df)
            # get first row, first element of that row, multiply by 100:w
            constraint["scores"] = [list(todo.collect()[0])[0]]
            if constraint["scores"][0] is None:
                constraint["scores"] = [100.]
            else:
                constraint["scores"][0] = constraint["scores"][0] * 100.

        # run groupRule, one at a time
        for grouprule in grouprules:
            columns = grouprule["columns"]
            having = grouprule["having"]
            conditions = grouprule.get("conditions", None)
            todo = m._grouprule_todo(columns, conditions, having, df)
            grouprule["scores"] = [list(todo.collect()[0])[0]]
            if grouprule["scores"][0] is None:
                grouprule["scores"] = [100.]
            else:
                grouprule["scores"][0] = grouprule["scores"][0] * 100.

        # run entropies, one at a time
        for entropy in entropies:
            column = entropy["column"]
            todo = m._entropy_todo(column, df)
            entropy["scores"] = [list(todo.collect()[0])[0]]
            if entropy["scores"][0] is None:
                entropy["scores"] = [0]

        # run mutual infos, one at a time
        for info in mutual_infos:
            when = info["when"]
            then = info["then"]
            todo = m._mutual_info_todo(when, then, df)
            info["scores"] = [list(todo.collect()[0])[0]]
            if info["scores"][0] is None:
                info["scores"] = [0]

        # sort metrics and return them after removing the id
        metrics = simple_metrics + constraints + grouprules + entropies + mutual_infos
        metrics = sorted(metrics, key=itemgetter('_task_id'))
        for metric in metrics:
            del metric["_task_id"]
        return metrics

    @staticmethod
    def _add_scores_to_metrics(metrics, collected, has_count_all, df):
        """
        Add a scores field to the passed metrics, getting them from the collected argument, a
        dataframe.

        :param metrics:
        :type metrics: list
        :param collected:
        :type collected: DataFrame
        :param df:
        :type df: DataFrame
        """
        index = 0
        collected = np.array(collected)
        total_rows = collected[-1] if has_count_all else None
        total_columns = len(df.columns)

        for metric in metrics:
            if metric["metric"] == "completeness":
                ncolumns = len(metric.get("columns", []))
                normalizer = total_rows if ncolumns > 0 else total_rows * total_columns

                if ncolumns == 0:
                    # aggregate over all columns of the table
                    scores = 0
                    if normalizer == 0:
                        scores = 1.
                        index += total_columns
                    else:
                        for _ in range(total_columns):
                            scores += (collected[index] / normalizer)
                            index += 1
                    metric["scores"] = [scores * 100]
                else:
                    # aggregate over columns parameter
                    scores = []
                    if normalizer == 0:
                        for _ in range(ncolumns):
                            scores.append(100.)
                        index += ncolumns
                    else:
                        for _ in range(ncolumns):
                            scores.append((collected[index] / normalizer) * 100)
                            index += 1
                    metric["scores"] = scores
            elif metric["metric"] == "deduplication" or metric["metric"] == "deduplication_approximated":
                """
                Using ["placeholder"] because no columns means counting distinct over the tuples of the table, so 
                there is one column to collect, and not zero.
                """
                ncolumns = len(metric.get("columns", ["placeholder"]))
                scores = []

                if total_rows == 0:
                    for _ in range(ncolumns):
                        scores.append(100.)
                    index += ncolumns
                else:
                    for _ in range(ncolumns):
                        scores.append((collected[index] / total_rows) * 100)
                        index += 1
                metric["scores"] = scores
            elif metric["metric"] == "timeliness":
                ncolumns = len(metric.get("columns"))
                scores = []
                if total_rows == 0:
                    for _ in range(ncolumns):
                        scores.append(100.)
                    index += ncolumns
                else:
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
                    metric["scores"] = [str(score) + " days" for score in scores]
                elif "timeFormat" in metric:
                    metric["scores"] = [str(score) + " seconds" for score in scores]
            elif metric["metric"] == "rule":
                if total_rows == 0:
                    index += 1
                    metric["scores"] = [100.]
                else:
                    scores = [(collected[index] / total_rows) * 100]
                    index += 1
                    metric["scores"] = scores
        return metrics
