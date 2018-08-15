"""
Module containing metrics for the distributed
version of hay_checker,
TODO: check if its possible to ignore the year, month, day of timestamps, to
make timeliness/freshness more handy while using timeFormat
TODO format results to hours, days, minutes whatever
"""

from . import task
from . import _util as check

import pyspark
from pyspark.sql.functions import isnan, when, count, col, sum, countDistinct, avg, to_date, lit, \
    abs, datediff, unix_timestamp, to_timestamp, current_timestamp, current_date
from datetime import datetime
from pyspark.sql.types import DataType


def _completeness_todo(columns, df):
    if columns is None:
        columns = df.columns
    todo = [count(col(c)).alias(c) for c in columns]
    return todo


def completeness(columns=None, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "completeness"}
        if not (columns is None):
            params["columns"] = columns
        return task.Task([params])
    else:
        # if df is specified run now
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
    if columns is None:
        columns = df.columns
    if columns is None:
        # 1 count distinct, on all columns
        todo = [countDistinct(*[col(c) for c in df.columns])]
    else:
        # multiple count distinct, one column each
        todo = [countDistinct(col(c)).alias(c) for c in columns]
    return todo


def deduplication(columns=None, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "deduplication"}
        if not (columns is None):
            params["columns"] = columns
        return task.Task([params])
    else:
        # if df is specified run now
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


def _timeliness_todo(columns, value, df, dateFormat=None, timeFormat=None):
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
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "timeliness", "columns": columns, "value": value}
        if dateFormat:
            params["dateFormat"] = dateFormat
        elif timeFormat:
            params["timeFormat"] = timeFormat
        return task.Task([params])
    else:
        # if df is specified run now
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
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "freshness", "columns": columns}
        if dateFormat:
            params["dateFormat"] = dateFormat
        elif timeFormat:
            params["timeFormat"] = timeFormat
        return task.Task([params])
    else:
        # if df is specified run now
        check.freshness_run_check(columns, df, dateFormat, timeFormat)
        todo = _freshness_todo(columns, df, dateFormat, timeFormat)
        result = list(df.agg(*todo).collect()[0])
        if dateFormat:
            result = [int(res) for res in result]
        else:
            result = [check._seconds_to_timeFormat(res) for res in result]

        return result


_operators_map = {"gt": '>', "lt": "<", "eq": "="}


def _and_conditions_as_strings(conditions):
    result = []
    conditions = conditions if conditions else []  # empty instead of None
    for cond in conditions:
        if type(cond["value"]) == str:
            result.append("%s %s '%s'" % (cond["column"], _operators_map[cond["operator"]], cond["value"]))
        else:
            result.append("%s %s %s" % (cond["column"], _operators_map[cond["operator"]], cond["value"]))
    return " and ".join(result)


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
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "constraint", "when": when, "then": then}
        if conditions:
            params["conditions"] = conditions
        return task.Task([params])
    else:
        # if df is specified run now
        check.constraint_run_check(when, then, conditions, df)
        todo = _constraint_todo(when, then, conditions, df)
        res = list(todo.collect()[0])
        return [res[0] * 100]


def _rule_todo(conditions):
    filtering_conditions = _and_conditions_as_columns(conditions)
    todo = sum(when(filtering_conditions, 1.0).otherwise(0.))
    return [todo]


def rule(conditions, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "rule", "conditions": conditions}
        return task.Task([params])
    else:
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
    todo = todo.agg(sum(when(having_constraints, 1).otherwise(0)).alias("_having_filtered"), count("*").alias("_having_all"))

    # normalize (divide passing groups by total groups)
    todo = todo.select(col("_having_filtered") / col("_having_all"))
    return todo


def grouprule(columns, conditions, having=None, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "groupRule", "columns": columns, "conditions": conditions}
        if having:
            params["having"] = having
        return task.Task([params])
    else:
        check.grouprule_run_check(columns, conditions, having, df)
        todo = _grouprule_todo(columns, conditions, having, df)

        collected = list(todo.collect()[0])
        return [collected[0] * 100]
