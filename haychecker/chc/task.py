"""
Class extending the _Task class from the common
scripts. It contains metrics to run that can be run
on different data, after having defined the task once.
"""

import copy
from operator import itemgetter

from haychecker._common._task import _Task
from haychecker.chc import _runtime_checks as util
from haychecker.chc import metrics as m


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
            for column 'x' being gt 3.0, with the type of 'x' being string) should be casted to numeric.
            If casting is not allowed the previous example would provoke an error, (through an
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
            if cond["column"] != "*" and typesdict[cond["column"]] == str and type(cond["value"]) != str:
                assert allow_casting, "Type of column '%s' is string, but 'value' %s is of numeric type, and casting " \
                                      "is not allowed" % (
                                          col, cond["value"])
                cond["casted_to"] = "numeric"

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
        typesdict = dict()
        for col in df:
            typesdict[col] = type(df[col].iat[0]) if len(df.index) > 0 else str

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

    @staticmethod
    def _merge_todo(todo):
        """
        Merge different todo lists avoiding repeated aggregations or functions for
        the same column.

        :param todo: List of dictionaries, mapping a column to an aggregation or lambda to compute on that column.
        :type todo: list
        :return: Dictionary mapping a column to a list of aggregations and lambdas to compute on that, no repetitions.
        :rtype: dict
        """
        res = dict()
        # iterate over dicts, getting lists of stuff to do for each column
        for d in todo:
            for k, v in d.items():
                if k not in res:
                    res[k] = []
                res[k].extend(v)
        for col in res:
            names = set()
            res[col] = list(set(res[col]))
            tmp = []
            for item in res[col]:
                if hasattr(item, '__call__') and hasattr(item, "__name__"):
                    if item.__name__ not in names:
                        names.add(item.__name__)
                        tmp.append(item)

                else:
                    tmp.append(item)
            res[col] = tmp
        return res

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

        # get stuff to do for metrics that can be run together in a single pass
        todo = []
        simple_metrics = []  # these will be run together
        # these will be run one at a time
        rules = []
        grouprules = []
        constraints = []
        entropies = []
        mutual_infos = []

        typesdict = dict()
        for col in df:
            typesdict[col] = type(df[col].iat[0]) if len(df.index) > 0 else str

        for i, metric in enumerate(metrics):
            if metric["metric"] == "completeness":
                metric["_task_id"] = i
                columns = metric.get("columns", None)
                todo.append(m._completeness_todo(columns, df))
                simple_metrics.append(metric)
            elif metric["metric"] == "deduplication":
                metric["_task_id"] = i
                columns = metric.get("columns", None)
                simple_metrics.append(metric)
                if columns is not None:
                    todo.append(m._deduplication_todo(columns, df))
            elif metric["metric"] == "timeliness":
                metric["_task_id"] = i
                columns = metric.get("columns")
                value = metric.get("value")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                todo.append(m._timeliness_todo(columns, value, typesdict, dateFormat, timeFormat))
                simple_metrics.append(metric)
            elif metric["metric"] == "freshness":
                metric["_task_id"] = i
                columns = metric.get("columns")
                dateFormat = metric.get("dateFormat", None)
                timeFormat = metric.get("timeFormat", None)
                todo.append(m._freshness_todo(columns, typesdict, dateFormat, timeFormat))
                simple_metrics.append(metric)
            elif metric["metric"] == "rule":
                metric["_task_id"] = i
                rules.append(metric)
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

        todo = Task._merge_todo(todo)

        # run and add results to the simple metrics
        if len(simple_metrics) > 0:
            collected = df.agg(todo)
            self._add_scores_to_metrics(simple_metrics, collected, df)

        # run rules, one at a time
        for rule in rules:
            if len(df.index) == 0.:
                rule["scores"] = [100.]
            else:
                rule["scores"] = [m._rule_compute(rule["conditions"], df) * 100.]

        # run constraints, one at a time
        for constraint in constraints:
            if len(df.index) == 0.:
                constraint["scores"] = [100.]
            else:
                when = constraint["when"]
                then = constraint["then"]
                conditions = constraint.get("conditions", None)
                constraint["scores"] = [m._constraint_compute(when, then, conditions, df) * 100]

        # run groupRule, one at a time
        for grouprule in grouprules:
            if len(df.index) == 0.:
                grouprule["scores"] = [100.]
            else:
                columns = grouprule["columns"]
                having = grouprule["having"]
                conditions = grouprule.get("conditions", None)
                grouprule["scores"] = [m._grouprule_compute(columns, conditions, having, df) * 100.]

        # run entropies, one at a time
        for entropy in entropies:
            if len(df.index) == 0:
                entropy["scores"] = [0.]
            else:
                entropy["scores"] = [m._entropy_compute(entropy["column"], df)]

        # run mutual infos, one at a time
        for info in mutual_infos:
            if len(df.index) == 0:
                info["scores"] = [0.]
            else:
                when = info["when"]
                then = info["then"]
                info["scores"] = [m._mutual_info_compute(when, then, df)]

        # sort metrics and return them after removing the id
        metrics = simple_metrics + rules + constraints + grouprules + entropies + mutual_infos
        metrics = sorted(metrics, key=itemgetter('_task_id'))
        for metric in metrics:
            del metric["_task_id"]
        return metrics

    @staticmethod
    def _add_scores_to_metrics(metrics, collected, df):
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
        collected = collected
        total_rows = len(df.index)
        total_cells = df.size
        columns = list(df.columns)
        unique_rows = None  # to avoid computing unique rows multiple times

        for metric in metrics:
            if metric["metric"] == "completeness":
                ncolumns = len(metric.get("columns", []))

                if ncolumns == 0:
                    # aggregate over all columns of the table
                    scores = 0
                    if total_rows == 0:
                        scores = 1.
                    else:
                        for col in columns:
                            scores += (collected[col]["count"] / total_cells)
                    metric["scores"] = [scores * 100]
                else:
                    # aggregate over columns parameter
                    scores = []
                    if total_rows == 0:
                        for _ in range(ncolumns):
                            scores.append(100.)
                    else:
                        for col in metric["columns"]:
                            scores.append((collected[col]["count"] / total_rows) * 100)
                    metric["scores"] = scores
            elif metric["metric"] == "deduplication":
                ncolumns = len(metric.get("columns", []))
                scores = []

                if total_rows == 0:
                    if ncolumns > 0:
                        for _ in range(ncolumns):
                            scores.append(100.)
                    else:
                        scores.append(100.)
                else:
                    if ncolumns > 0:
                        for col in metric["columns"]:
                            scores.append((collected[col]["nunique"] / total_rows) * 100)
                    else:
                        import pandas as pd
                        d = pd.DataFrame()
                        unique_rows = unique_rows if unique_rows is not None else len(
                            df.dropna().drop_duplicates().index)
                        scores.append((unique_rows / total_rows) * 100)
                metric["scores"] = scores
            elif metric["metric"] == "timeliness":
                ncolumns = len(metric.get("columns"))
                scores = []
                if total_rows == 0:
                    for _ in range(ncolumns):
                        scores.append(100.)
                else:
                    format = metric["timeFormat"] if "timeFormat" in metric else metric["dateFormat"]
                    filler = "timeFormat" if "timeFormat" in metric else "dateFormat"
                    for col in metric["columns"]:
                        scores.append(
                            (collected[col][
                                 "_timeliness_agg_%s_%s_%s_%s" % (col, filler, format, metric["value"])] * 100))
                metric["scores"] = scores
            elif metric["metric"] == "freshness":
                ncolumns = len(metric.get("columns"))
                scores = []
                if total_rows == 0:
                    for _ in range(ncolumns):
                        scores.append("None days" if "dateFormat" in metric else "None seconds")
                    metric["scores"] = scores
                else:
                    format = metric["timeFormat"] if "timeFormat" in metric else metric["dateFormat"]
                    filler = "timeFormat" if "timeFormat" in metric else "dateFormat"
                    for col in metric["columns"]:
                        scores.append((collected[col]["_freshness_agg_%s_%s_%s" % (col, filler, format)]))
                    if "dateFormat" in metric:
                        metric["scores"] = [str(score) + " days" for score in scores]
                    elif "timeFormat" in metric:
                        metric["scores"] = [str(score) + " seconds" for score in scores]
