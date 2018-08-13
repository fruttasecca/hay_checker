"""
Module containing metrics for the distributed
version of hay_checker,
TODO: check if its possible to ignore the year, month, day of timestamps, to
make timeliness/freshness more handy while using timeFormat
TODO format results to hours, days, minutes whatever
"""

from hc.dhc.task import Task
from hc.dhc import _util as check

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
        return Task([params])
    else:
        # if df is specified run now
        check._completeness_check(columns, df)
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
        return Task([params])
    else:
        # if df is specified run now
        check._deduplication_check(columns, df)
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
        return Task([params])
    else:
        # if df is specified run now
        check._timeliness_check(columns, value, df, dateFormat, timeFormat)
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
        return Task([params])
    else:
        # if df is specified run now
        check._freshness_check(columns, df, dateFormat, timeFormat)
        todo = _freshness_todo(columns, df, dateFormat, timeFormat)
        result = list(df.agg(*todo).collect()[0])
        if dateFormat:
            result = [int(res) for res in result]
        else:
            result = [check._seconds_to_timeFormat(res) for res in result]

        return result
