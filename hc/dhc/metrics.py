"""
Module containing metrics for the distributed
version of hay_checker,
"""

# allowed_metrics = [ "constraint", "rule", "groupRule"]
from hc.dhc.task import Task
import hc.dhc.__util as check

from pyspark.sql.functions import isnan, when, count, col, sum, countDistinct, avg, to_date, lit, \
    abs, datediff, unix_timestamp, to_timestamp
from datetime import datetime
from pyspark.sql.types import DataType


def completeness(columns=None, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "completeness"}
        if not (columns is None):
            params["columns"] = columns
        return Task([params])
    else:
        # if df is specified run now
        check.__completeness_check(df, columns)
        if not (columns is None):
            # using [0] at the end because a single row is being returned
            result = list(df.agg(*([count(col(c)).alias(c) for c in columns] + [count('*')])).collect()[0])
            # divide everything by the number of rows in the dataset
            for i in range(len(result) - 1):
                result[i] /= result[-1]
                result[i] *= 100
            return result[:-1]
        else:
            # count not nulls on all columns then merge results
            tmp = list(df.agg(*([count(col(c)).alias(c) for c in df.columns] + [count('*')])).collect()[0])
            total_cells = len(df.columns * tmp[-1])

            # divide one result at a time to avoid overflow
            result = 0
            for i in range(len(tmp) - 1):
                result += (tmp[i] / total_cells)
            return result * 100


def deduplication(columns=None, df=None):
    if df is None:
        # if no df specified create a task that contains this parameters
        params = {"metric": "deduplication"}
        if not (columns is None):
            params["columns"] = columns
        return Task([params])
    else:
        # if df is specified run now
        check.__deduplication_check(df, columns)
        if columns is None:
            # 1 count distinct on all columns
            to_do = [countDistinct(*[col(c) for c in df.columns])]
        else:
            # multiple count distinct, one column each
            to_do = [countDistinct(col(c)).alias(c) for c in columns]
        to_do += [count('*')]  # count rows

        # using [0] at the end because a single row is being returned
        result = list(df.agg(*to_do).collect()[0])

        # divide everything by the number of rows in the dataset
        for i in range(len(result) - 1):
            result[i] /= result[-1]
            result[i] *= 100
        return result[:-1]


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
        check.__timeliness_check(df, columns, value, format)

        types = dict(df.dtypes)
        to_do = []

        if format == "dd/mm/yyyy":
            value = to_date(lit(value), format)

            for c in columns:
                if types[c] == "timestamp" or types[c] == "date":
                    to_do.append(sum(when(datediff(value, c) > 0, 1).otherwise(0)).alias(c))
                else:
                    to_do.append(sum(when(datediff(value, to_date(c, format)) > 0, 1).otherwise(0)).alias(c))
        elif format == "HH:mm:ss":
            value = to_timestamp(lit(value), format).cast("long")

            for c in columns:
                if types[c] == "timestamp":
                    to_do.append(sum(when(value - col(c).cast("long") > 0, 1).otherwise(0)).alias(c))
                    df = df.withColumn("value", value)
                else:
                    to_do.append(sum(when(value - to_timestamp(c, format).cast("long") > 0, 1).otherwise(0)).alias(c))
        to_do += [count('*')]  # count rows

        result = list(df.agg(*to_do).collect()[0])
        # divide everything by the number of rows in the dataset
        for i in range(len(result) - 1):
            result[i] /= result[-1]
            result[i] *= 100
        return result[:-1]


def freshness(columns, format, df=None):
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
        check.__freshness_check(df, columns, format)
        types = dict(df.dtypes)
        to_do = []
        if format == "dd/mm/yyyy":
            now = str(datetime.now())[:10]
            now = to_timestamp(lit(now), "yyyy-mm-dd")

            for c in columns:
                if types[c] == "timestamp":
                    to_do.append(avg(abs(datediff(c, now))).alias(c))
                else:
                    to_do.append(avg(abs(datediff(to_timestamp(c, format), now))).alias(c))
        elif format == "HH:mm:ss":
            now = "1970-01-01 " + str(datetime.now())[11:19]
            now = unix_timestamp(lit(now), format="yyyy-MM-dd HH:mm:ss")

            for c in columns:
                if types[c] == "timestamp":
                    to_do.append(avg(abs(col(c).cast("long") - now)).alias(c))
                else:
                    to_do.append(avg(abs(unix_timestamp(c, format) - now)).alias(c))

        result = df.agg(*to_do).collect()[0]
        return list(result)
