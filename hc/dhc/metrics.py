"""
Module containing metrics for the distributed version of hay_checker.
TODO: check if its possible to ignore the year, month, day of timestamps, to
make timeliness/freshness more handy while using timeFormat
TODO format results to hours, days, minutes whatever
"""

from datetime import datetime

import pyspark
from pyspark.sql.functions import isnan, when, count, col, sum, countDistinct, avg, to_date, lit, \
    abs, datediff, unix_timestamp, to_timestamp, current_timestamp, current_date, approx_count_distinct, log2

from . import task
from . import _runtime_checks as check
from .._common._util import seconds_to_time_format
from .._common.config import Config


def _completeness_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        columns = df.columns
    todo = [count(col(c)).alias(c) for c in columns]
    return todo


def completeness(columns=None, df=None):
    """
    If a df is passed, the completeness metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric, None to run the completeness
    metric on the whole table.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """

    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "completeness"}
    if not (columns is None):
        params["columns"] = columns

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        columns = check._columns_index_to_name(columns, df)
        Config._completeness_params_check(params, "Erroneous parameters")
        check.completeness_run_check(columns, df)

        todo = _completeness_todo(columns, df)
        todo.append(count("*"))  # get number of rows, used to normalize
        collected = list(df.agg(*todo).collect()[0])
        if columns is None:
            total_cells = len(df.columns * collected[-1])
            result = 0
            # divide one result at a time to avoid overflow
            for i in range(len(collected) - 1):
                result += (collected[i] / total_cells)
            return result * 100
        else:
            for i in range(len(collected) - 1):
                collected[i] = (collected[i] / collected[-1]) * 100
            return collected[:-1]


def _deduplication_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        # 1 count distinct, on all columns
        todo = [countDistinct(*[col(c) for c in df.columns])]
    else:
        # multiple count distinct, one column each
        todo = [countDistinct(col(c)).alias(c) for c in columns]
    return todo


def deduplication(columns=None, df=None):
    """
    If a df is passed, the deduplication metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric, None to run the deduplication
    metric on the whole table.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "deduplication"}
    if not (columns is None):
        params["columns"] = columns

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        columns = check._columns_index_to_name(columns, df)
        Config._deduplication_params_check(params, "Erroneous parameters")
        check.deduplication_run_check(columns, df)

        todo = _deduplication_todo(columns, df)
        todo.append([count('*')])  # count all rows

        # using [0] at the end because a single row is being returned
        collected = list(df.agg(*todo).collect()[0])
        # divide everything by the number of rows in the dataset
        for i in range(len(collected) - 1):
            collected[i] /= collected[-1]
            collected[i] *= 100
        return collected[:-1]


def _deduplication_approximated_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        # 1 count distinct, on all columns
        todo = [approx_count_distinct(*[col(c) for c in df.columns])]
    else:
        # multiple count distinct, one column each
        todo = [approx_count_distinct(col(c)).alias(c) for c in columns]
    return todo


def deduplication_approximated(columns=None, df=None):
    """
    If a df is passed, the deduplication_approximated metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric, None to run the deduplication_approximated
    metric on the whole table.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "deduplication_approximated"}
    if not (columns is None):
        params["columns"] = columns

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        columns = check._columns_index_to_name(columns, df)
        Config._deduplication_params_check(params, "Erroneous parameters")
        check.deduplication_run_check(columns, df)

        todo = _deduplication_approximated_todo(columns, df)
        todo.append([count('*')])  # count all rows

        # using [0] at the end because a single row is being returned
        collected = list(df.agg(*todo).collect()[0])
        # divide everything by the number of rows in the dataset
        for i in range(len(collected) - 1):
            collected[i] /= collected[-1]
            collected[i] *= 100
        return collected[:-1]


def _timeliness_todo(columns, value, df, dateFormat=None, timeFormat=None):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param value
    :param df:
    :param dateFormat:
    :param timeFormat:
    :return: Pyspark columns representing what to compute.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = []
    types = dict(df.dtypes)

    if dateFormat:
        value = to_date(lit(value), dateFormat)
        for c in columns:
            if types[c] == "timestamp" or types[c] == "date":
                todo.append(sum(when(datediff(value, c) > 0, 1).otherwise(0)).alias(c))
            elif types[c] == "string":
                todo.append(sum(when(datediff(value, to_date(c, dateFormat)) > 0, 1).otherwise(0)).alias(c))
            else:
                print(
                    "Type of a column on which the timeliness metric is run must be either timestamp, "
                    "date or string, if the metric is being run on dateFormat.")
                exit()
    elif timeFormat:
        value = to_timestamp(lit(value), timeFormat).cast("long")
        for c in columns:
            if types[c] == "timestamp":
                todo.append(sum(when(value - col(c).cast("long") > 0, 1).otherwise(0)).alias(c))
            elif types[c] == "string":
                todo.append(sum(when(value - to_timestamp(c, timeFormat).cast("long") > 0, 1).otherwise(0)).alias(c))
            else:
                print(
                    "Type of a column on which the timeliness metric is run must be either timestamp or string, if "
                    "the metric is being run on a timeFormat")
                exit()
    return todo


def timeliness(columns, value, df=None, dateFormat=None, timeFormat=None):
    """
    If a df is passed, the timeliness metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric
    :param value: Value used to run the metric, confronting values in the specified columns against it.
    :param dateFormat: Format in which the value (and values in columns, if they are of string type) are; used
    if the value and columns contain dates as strings, or are of date or timestamp type. Either dateFormat
    or timeFormat must be passed, but not both.
    :param timeFormat: Format in which the value (and values in columns, if they are of string type) are; used
    if the value and columns contain times as strings or are of timestamp type. Either dateFormat
    or timeFormat must be passed, but not both.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, not both."
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "timeliness", "columns": columns, "value": value}
    if dateFormat:
        params["dateFormat"] = dateFormat
    elif timeFormat:
        params["timeFormat"] = timeFormat

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        columns = check._columns_index_to_name(columns, df)
        Config._timeliness_params_check(params, "Erroneous parameters")
        check.timeliness_run_check(columns, value, df, dateFormat, timeFormat)

        todo = _timeliness_todo(columns, value, df, dateFormat, timeFormat)
        todo.append(count('*'))  # count rows

        collected = list(df.agg(*todo).collect()[0])
        # divide everything by the number of rows in the dataset
        for i in range(len(collected) - 1):
            collected[i] /= collected[-1]
            collected[i] *= 100
        return collected[:-1]


def _freshness_todo(columns, df, dateFormat=None, timeFormat=None):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param df:
    :param dateFormat:
    :param timeFormat:
    :return: Pyspark columns representing what to compute.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    types = dict(df.dtypes)
    todo = []

    if dateFormat:
        now = current_date()
        for c in columns:
            if types[c] == "timestamp" or types[c] == "date":
                todo.append(avg(abs(datediff(c, now))).alias(c))
            elif types[c] == "string":
                todo.append(avg(abs(datediff(to_date(c, dateFormat), now))).alias(c))
            else:
                print(
                    "Type of a column on which the freshness metric is run must be either timestamp, "
                    "date or string, if the metric is being run on dateFormat.")
                exit()
    elif timeFormat:
        now = to_timestamp(lit("1970-01-01 " + str(datetime.now())[11:19])).cast("long")
        for c in columns:
            if types[c] == "timestamp":
                todo.append(avg(abs(col(c).cast("long") - now)).alias(c))
            elif types[c] == "string":
                todo.append(avg(abs(to_timestamp(c, timeFormat).cast("long") - now)).alias(c))
            else:
                print(
                    "Type of a column on which the freshness metric is run must be either timestamp"
                    "or string, if the metric is being run on timeFormat.")
                exit()
    return todo


def freshness(columns, df=None, dateFormat=None, timeFormat=None):
    """
    If a df is passed, the freshness metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric
    :param dateFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
    be of type date or timestamp. Use this parameter if you are interested in a result in terms of days.
    Either dateFormat or timeFormat must be passed, but not both.
    :param timeFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
    be of type timestamp. Use this parameter if you are interested in a result totalling less than a day, shown
    in a format of HH:mm:ss. Either dateFormat or timeFormat must be passed, but not both.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "freshness", "columns": columns}
    if dateFormat:
        params["dateFormat"] = dateFormat
    elif timeFormat:
        params["timeFormat"] = timeFormat

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        columns = check._columns_index_to_name(columns, df)
        Config._freshness_params_check(params, "Erroneous parameters")
        check.freshness_run_check(columns, df, dateFormat, timeFormat)
        todo = _freshness_todo(columns, df, dateFormat, timeFormat)
        result = list(df.agg(*todo).collect()[0])
        if dateFormat:
            result = [int(res) for res in result]
        else:
            result = [seconds_to_time_format(res) for res in result]

        return result


def _and_conditions_as_columns(conditions):
    # add first condition
    cond = conditions[0]
    if cond["operator"] == "gt":
        result = col(cond["column"]) > cond["value"]
    elif cond["operator"] == "lt":
        result = col(cond["column"]) < cond["value"]
    elif cond["operator"] == "eq":
        result = col(cond["column"]) == cond["value"]

    # add the rest
    for cond in conditions[1:]:
        if cond["operator"] == "gt":
            result = result & (col(cond["column"]) > cond["value"])
        elif cond["operator"] == "lt":
            result = result & (col(cond["column"]) < cond["value"])
        elif cond["operator"] == "eq":
            result = result & (col(cond["column"]) == cond["value"])
    return result


def _constraint_todo(when, then, conditions, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param when:
    :param then:
    :param conditions:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    todo = df

    # filter if needed
    if conditions:
        filtering_conditions = _and_conditions_as_columns(conditions)
        todo = todo.filter(filtering_conditions)

    # groupby the when columns
    todo = todo.groupBy(*when)

    # for each group, count the total and the number of distinct 'thens' (should be 1 if the constraint is respected)
    todo = todo.agg(count("*").alias("metrics_check_count_1"), countDistinct(*then).alias("distinct_then"))

    # given the new 'table', aggregate over it, summing over all total rows to get the total number of filtered
    # rows, and summing the count only of groups that have one distinct then value
    todo = todo.agg(sum("metrics_check_count_1").alias("all_filtered"), sum(
        pyspark.sql.functions.when(col("distinct_then") == 1, col("metrics_check_count_1")).otherwise(0)).alias(
        "respecting"))

    # get the ratio between the tuples respecting the constraint and the total, where total is the number of
    # rows that have passed the filtering
    todo = todo.select(col("respecting") / col("all_filtered"))
    return todo


def constraint(when, then, conditions=None, df=None):
    """
    If a df is passed, the constraint metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param when: A list of columns in the df to use as the precondition of a functional constraint. No column
    should be in both when and then.
    :param then: A list of columns in the df to use as the postcondition of a functional constraint. No column
    should be in both when and then.
    :param conditions: Conditions on which to filter data before applying the metric.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "constraint", "when": when, "then": then}
    if conditions:
        params["conditions"] = conditions

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        when, then = check._columns_index_to_name([when, then], df)
        conditions = check._conditions_columns_index_to_name(conditions, df)
        Config._constraint_params_check(params, "Erroneous parameters")
        check.constraint_run_check(when, then, conditions, df)
        todo = _constraint_todo(when, then, conditions, df)
        res = list(todo.collect()[0])
        return [res[0] * 100]


def _rule_todo(conditions):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param conditions:
    :return: Pyspark columns representing what to compute.
    """
    filtering_conditions = _and_conditions_as_columns(conditions)
    todo = sum(when(filtering_conditions, 1.0).otherwise(0.))
    return [todo]


def rule(conditions, df=None):
    """
    If a df is passed, the rule metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param conditions: Conditions on which to run the metric.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "rule", "conditions": conditions}

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        conditions = check._conditions_columns_index_to_name(conditions, df)
        Config._rule_params_check(params, "Erroneous parameters")
        check.rule_run_check(conditions, df)
        todo = _rule_todo(conditions)
        todo.append(count('*'))  # count all rows
        collected = list(df.agg(*todo).collect()[0])
        return [(collected[0] / collected[1]) * 100]


def _having_aggregations_as_columns(condition):
    column = condition["column"]
    aggregator = condition["aggregator"] if "aggregator" in condition else None
    if aggregator == "count":
        return count(column)
    elif aggregator == "min":
        return pyspark.sql.functions.min(column)
    elif aggregator == "max":
        return pyspark.sql.functions.max(column)
    elif aggregator == "avg":
        return pyspark.sql.functions.avg(column)
    elif aggregator == "sum":
        return pyspark.sql.functions.sum(column)
    elif aggregator == "sqrt":
        return pyspark.sql.functions.sqrt(column)
    else:
        print("Aggregator %s not recognized" % aggregator)
        exit()


def _having_constraints_as_column(having):
    # add first condition
    index = 0
    cond = having[0]
    if cond["operator"] == "gt":
        result = col("_grouprule_h%i" % index) > cond["value"]
    elif cond["operator"] == "lt":
        result = col("_grouprule_h%i" % index) < cond["value"]
    elif cond["operator"] == "eq":
        result = col("_grouprule_h%i" % index) == cond["value"]
    index += 1

    # add the rest
    for cond in having[1:]:
        if cond["operator"] == "gt":
            result = result & col("_grouprule_h%i" % index) > cond["value"]
        elif cond["operator"] == "lt":
            result = result & col("_grouprule_h%i" % index) < cond["value"]
        elif cond["operator"] == "eq":
            result = result & col("_grouprule_h%i" % index) == cond["value"]
        index += 1
    return result


def _grouprule_todo(columns, conditions, having, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param columns:
    :param conditions:
    :param having:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    todo = df

    # filter if needed
    if conditions:
        filtering_conditions = _and_conditions_as_columns(conditions)
        todo = todo.filter(filtering_conditions)

    # get groups
    todo = todo.groupBy(*columns)

    # aggregate groups over 'having' conditions
    aggregations = [_having_aggregations_as_columns(cond).alias("_grouprule_h%i" % i) for i, cond in enumerate(having)]
    todo = todo.agg(*aggregations)

    # aggregate the tuples (each tuple representing a group
    # to 1) count them 2) count the ones passing the having conditions
    having_constraints = _having_constraints_as_column(having)
    todo = todo.agg(sum(when(having_constraints, 1).otherwise(0)).alias("_having_filtered"),
                    count("*").alias("_having_all"))

    # normalize (divide passing groups by total groups)
    todo = todo.select(col("_having_filtered") / col("_having_all"))
    return todo


def grouprule(columns, having, conditions=None, df=None):
    """
    If a df is passed, the rule metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param columns: Columns on which to run the metric, grouping data.
    :param conditions: Conditions on which to run the metric, filtering data before grouping, can be None.
    :param having: Conditions to apply to groups.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "groupRule", "columns": columns, "conditions": conditions}
    if having:
        params["having"] = having

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        columns = check._columns_index_to_name(columns, df)
        conditions = check._conditions_columns_index_to_name(conditions, df)
        if having:
            having = check._conditions_columns_index_to_name(having, df)
        Config._grouprule_params_check(params, "Erroneous params")
        check.grouprule_run_check(columns, conditions, having, df)
        todo = _grouprule_todo(columns, conditions, having, df)

        collected = list(todo.collect()[0])
        return [collected[0] * 100]


def _entropy_todo(column, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param column:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    # group on that column
    todo = df.groupBy(column)

    # count instances of each group
    todo = todo.agg(count("*").alias("_entropy_ci"))
    todo = todo.select(sum(col("_entropy_ci") * log2("_entropy_ci")).alias("_sumcilogci"),
                       sum("_entropy_ci").alias("_total"))
    todo = todo.select(log2(col("_total")) - col("_sumcilogci") / col("_total"))
    return todo


def entropy(column, df=None):
    """
    If a df is passed, the entropy metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param column: Column on which to run the metric.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    column = check._columns_index_to_name([column], df)[0]
    params = {"metric": "entropy", "column": column}

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        Config._entropy_params_check(params, "Erroneous parameters")
        check.entropy_run_check(column, df)

        todo = _entropy_todo(column, df)

        # using [0] at the end because a single row is being returned
        collected = list(df.agg(*todo).collect()[0])
        return [collected[0]]


def _mutual_info_todo(when, then, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.
    :param when:
    :param then:
    :param df:
    :return: Pyspark columns representing what to compute.
    """
    # group on the pair of columns, count occurrences
    pairs_table = df.groupBy([when, then]).agg(count("*").alias("_pairs_count"))
    pairs_table.cache()

    when_table = pairs_table.groupBy(when).agg(sum("_pairs_count").alias("_when_count"))
    then_table = pairs_table.groupBy(then).agg(sum("_pairs_count").alias("_then_count"))
    final_table = pairs_table.join(when_table, when).join(then_table, then)
    # prepare 4 subformulas of MI to later sum, plus the total
    todo = final_table.select(sum(col("_pairs_count") * log2(col("_pairs_count"))).alias("_s1"),  # c_xy * logc_xy
                              sum(col("_pairs_count")).alias("_s2"),  # c_xy
                              sum(col("_pairs_count") * log2(col("_when_count"))).alias("_s3"),  # c_xy * logc_x
                              sum(col("_pairs_count") * log2(col("_then_count"))).alias("_s4"),  # c_xy * logc_y
                              sum(col("_pairs_count")).alias("_total")  # total
                              )
    todo = todo.select((col("_s1") / col("_total")) + (log2(col("_total")) * (col("_s2") / col("_total"))) - (
            (col("_s3")) / col("_total")) - ((col("_s4")) / col("_total")).alias("mutual_info"))
    return todo


def mutual_info(when, then, df=None):
    """
    If a df is passed, the mutual_info metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    :param when: First column on which to compute MI.
    :param then: Second column on which to compute MI.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    # either needed to make the Task instance or to check params
    params = {"metric": "mutual_info", "when": when, "then": then}

    if df is None:
        # if no df specified create a task that contains this parameters
        return task.Task([params])
    else:
        # if df is specified run now
        when, then = check._columns_index_to_name([when, then], df)
        Config._mutual_info_params_check(params, "Erroneous parameters")
        check.mutual_info_run_check(when, then, df)

        todo = _mutual_info_todo(when, then, df)
        return [list(todo.collect()[0])]
