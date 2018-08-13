"""
Module containing metrics for the distributed
version of hay_checker,
"""

from hc.dhc.task import Task
from hc.dhc import _util as check

from pyspark.sql.functions import isnan, when, count, col, sum, countDistinct, avg, to_date, lit, \
    abs, datediff, unix_timestamp, to_timestamp
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


def _timeliness_todo(columns, value, format, df):
    """
    TODO: also __util for date type
    """
    todo = []
    types = dict(df.dtypes)

    if format == "dd/mm/yyyy":
        value = to_date(lit(value), format)

        for c in columns:
            if types[c] == "timestamp" or types[c] == "date":
                todo.append(sum(when(datediff(value, c) > 0, 1).otherwise(0)).alias(c))
            else:
                todo.append(sum(when(datediff(value, to_date(c, format)) > 0, 1).otherwise(0)).alias(c))
    elif format == "HH:mm:ss":
        value = to_timestamp(lit(value), format).cast("long")

        for c in columns:
            if types[c] == "timestamp":
                todo.append(sum(when(value - col(c).cast("long") > 0, 1).otherwise(0)).alias(c))
                df = df.withColumn("value", value)
            else:
                todo.append(sum(when(value - to_timestamp(c, format).cast("long") > 0, 1).otherwise(0)).alias(c))
    return todo


def timeliness(columns, value, format, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "timeliness", "columns": columns, "value": value}
        if format == "dd/mm/yyyy":
            params["dateFormat"] = format
        elif format == "HH:mm:ss":
            params["timeFormat"] = format
        return Task([params])
    else:
        # if df is specified run now
        check._timeliness_check(columns, value, format, df)
        todo = _timeliness_todo(columns, value, format, df)
        todo.append(count('*'))  # count rows

        collected = list(df.agg(*todo).collect()[0])
        # divide everything by the number of rows in the dataset
        for i in range(len(collected) - 1):
            collected[i] /= collected[-1]
            collected[i] *= 100
        return collected[:-1]


def _freshness_todo(columns, format, df):
    """
    TODO: also __util for date type
    """
    types = dict(df.dtypes)
    todo = []

    if format == "dd/mm/yyyy":
        now = str(datetime.now())[:10]
        now = to_timestamp(lit(now), "yyyy-mm-dd")

        for c in columns:
            if types[c] == "timestamp":
                todo.append(avg(abs(datediff(c, now))).alias(c))
            else:
                todo.append(avg(abs(datediff(to_timestamp(c, format), now))).alias(c))
    elif format == "HH:mm:ss":
        now = "1970-01-01 " + str(datetime.now())[11:19]
        now = unix_timestamp(lit(now), format="yyyy-MM-dd HH:mm:ss")

        for c in columns:
            if types[c] == "timestamp":
                todo.append(avg(abs(col(c).cast("long") - now)).alias(c))
            else:
                todo.append(avg(abs(unix_timestamp(c, format) - now)).alias(c))
    return todo


def freshness(columns, format, df=None):
    """
    TODO: format results to days, hours, minutes etc
    """
    # https: // docs.oracle.com / javase / 7 / docs / api / java / text / SimpleDateFormat.html
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "freshness", "columns": columns}
        if format == "dd/mm/yyyy":
            params["dateFormat"] = format
        elif format == "HH:mm:ss":
            params["timeFormat"] = format
        return Task([params])
    else:
        # if df is specified run now
        check._freshness_check(columns, format, df)
        todo = _freshness_todo(columns, format, df)
        result = df.agg(*todo).collect()[0]
        return list(result)
