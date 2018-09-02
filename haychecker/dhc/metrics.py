"""
Module containing metrics for the distributed version of hay_checker.
"""

import pyspark
from pyspark.sql.functions import isnan, when, count, col, sum, countDistinct, avg, to_date, lit, \
    abs, datediff, to_timestamp, current_timestamp, current_date, approx_count_distinct, log2, log

from haychecker.dhc import task


def _completeness_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        columns = df.columns
    todo = [count(c).alias(c) for c in columns]
    return todo


def completeness(columns=None, df=None):
    """
    If a df is passed, the completeness metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param columns: Columns on which to run the metric, None to run the completeness
        metric on the whole table.
    :type columns: list
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """

    # make a dict representing the parameters
    params = {"metric": "completeness"}
    if not (columns is None):
        params["columns"] = columns
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _deduplication_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        # 1 count distinct, on all columns
        todo = [countDistinct(*[col(c) for c in df.columns])]
    else:
        # multiple count distinct, one column each
        todo = [countDistinct(col(c)) for c in columns]
    return todo


def deduplication(columns=None, df=None):
    """
    If a df is passed, the deduplication metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param columns: Columns on which to run the metric, None to run the deduplication
        metric on the whole table (deduplication on rows).
    :type columns: list
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "deduplication"}
    if not (columns is None):
        params["columns"] = columns
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _deduplication_approximated_todo(columns, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :return: Pyspark columns representing what to compute.
    """
    if columns is None:
        print("Approximated count distinct spanning over the whole row is currently not supported")
    else:
        # multiple count distinct, one column each
        todo = [approx_count_distinct(col(c)).alias(c) for c in columns]
    return todo


def deduplication_approximated(columns, df=None):
    """
    If a df is passed, the deduplication_approximated metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    Differently from deduplication, here columns must be specified (deduplication_approximated does not
    work on a whole row level).

    :param columns: Columns on which to run the metric.
    :type columns: list
        :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "deduplication_approximated"}
    if not (columns is None):
        params["columns"] = columns
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _contains_date(format):
    """
    Check if a format (string) contains a date.
    (It currently check if the string contains tokens from the simpledateformat
    https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    that represent, years, months, days).

    :param format: A string format representing a simple date format.
    :type format: str
    :return: True if values part of a date are contained in the format string.
    :rtype: bool
    """
    part_of_date_tokens = "GyYMwWdDFEu"
    for token in part_of_date_tokens:
        if token in format:
            return True
    return False


def _timeliness_todo(columns, value, df, dateFormat=None, timeFormat=None):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param value
    :type value: str
    :param df:
    :type df: DataFrame
    :param dateFormat:
    :type dateFormat: str
    :param timeFormat:
    :type timeFormat: str
    :return: Pyspark columns representing what to compute.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = []
    types = dict(df.dtypes)

    if dateFormat:
        value_date = to_date(lit(value), dateFormat)
        for c in columns:
            if types[c] == "timestamp" or types[c] == "date":
                todo.append(sum(when(datediff(value_date, c) > 0, 1).otherwise(0)).alias(c))
            elif types[c] == "string":
                todo.append(sum(when(datediff(value_date, to_date(c, dateFormat)) > 0, 1).otherwise(0)).alias(c))
            else:
                print(
                    "Type of a column on which the timeliness metric is run must be either timestamp, "
                    "date or string, if the metric is being run on dateFormat.")
                exit()
    elif timeFormat:
        value_long = to_timestamp(lit(value), timeFormat).cast("long")
        # check if value contains a date and not only hours, minutes, seconds
        has_date = _contains_date(timeFormat)
        if has_date:
            for c in columns:
                if types[c] == "timestamp":
                    todo.append(sum(when(value_long - col(c).cast("long") > 0, 1).otherwise(0)).alias(c))
                elif types[c] == "string":
                    todo.append(
                        sum(when(value_long - to_timestamp(col(c), timeFormat).cast("long") > 0, 1).otherwise(0)).alias(
                            c))
                else:
                    print(
                        "Type of a column on which the timeliness metric is run must be either timestamp or string, if "
                        "the metric is being run on a timeFormat")
                    exit()
        else:
            for c in columns:
                if types[c] == "timestamp":
                    """
                    If there is no years, months, days we must ignore the years, months, days in the timestamp.
                    """
                    value_long = to_timestamp(lit(value), timeFormat)
                    # remove years, months, days
                    value_long = value_long.cast("long") - value_long.cast("date").cast("timestamp").cast("long")

                    # check for difference, but only considering hours, minutes, seconds
                    todo.append(sum(
                        when(
                            value_long - (col(c).cast("long") - col(c).cast("date").cast("timestamp").cast("long")) > 0,
                            1).otherwise(0)).alias(c))
                elif types[c] == "string":
                    """
                    If there are no years, months, days and the column is in the same format, meaning that it also
                    has no years, months, days, this means that they will be both initialized to the same year, month, day;
                    so years, months, days will be basically ignored.
                    """
                    todo.append(
                        sum(when((value_long - to_timestamp(c, timeFormat).cast("long")) > 0, 1).otherwise(0)).alias(c))
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
    Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html directives to express formats.

    :param columns: Columns on which to run the metric, columns of type string will be casted to timestamp
        using the dateFormat or timeFormat argument.
    :type columns: list
    :param value: Value used to run the metric, confronting values in the specified columns against it.
    :type value: str
    :param dateFormat: Format in which the value (and values in columns, if they are of string type) are; used
        to cast columns if they contain dates as strings. Either dateFormat
        or timeFormat must be passed, but not both. Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
        directives to express formats.
    :type dateFormat: str
    :param timeFormat: Format in which the value (and values in columns, if they are of string type) are; used
        to cast columns if they contain dates as strings. Either dateFormat
        or timeFormat must be passed, but not both. Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
        directives to express formats.
    :type timeFormat: str
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, not both."
    # make a dict representing the parameters
    params = {"metric": "timeliness", "columns": columns, "value": value}
    if dateFormat:
        params["dateFormat"] = dateFormat
    elif timeFormat:
        params["timeFormat"] = timeFormat
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _freshness_todo(columns, df, dateFormat=None, timeFormat=None):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :param dateFormat:
    :type dateFormat: str
    :param timeFormat:
    :type timeFormat: str
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
        # check if value contains a date and not only hours, minutes, seconds
        has_date = _contains_date(timeFormat)

        current = current_timestamp()
        if has_date:
            """
            If the time format also contains a date it means the user is also interested in comparing years, months, days, 
            etc.
            """
            now = current.cast("long")
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
        else:
            """
            If the timestamp has no date the user is not interested in differences that consider years, months, days, but
            only hours, minutes, seconds.
            """
            now = current
            now = now.cast("long") - now.cast("date").cast("timestamp").cast("long")
            for c in columns:
                if types[c] == "timestamp":
                    todo.append(avg(
                        abs((col(c).cast("long") - col(c).cast("date").cast("timestamp").cast("long")) - now)).alias(c))
                elif types[c] == "string":
                    """
                    Need to remove seconds from years, months and days here as well because even if the format
                    does not specify anything for those values they are initialized to something by default.
                    """
                    todo.append(avg(abs((to_timestamp(c, timeFormat).cast("long") - to_timestamp(c, timeFormat).cast(
                        "date").cast("timestamp").cast("long")) - now)).alias(c))
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
    Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html directives to express formats.

    :param columns: Columns on which to run the metric, columns of type string will be casted to timestamp
        using the dateFormat or timeFormat argument.
    :type columns: list
    :param dateFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
        be of type date or timestamp. Use this parameter if you are interested in a result in terms of days.
        Either dateFormat or timeFormat must be passed, but not both.
        Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html directives to express formats.
    :type dateFormat: str
    :param timeFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
        be of type timestamp. Use this parameter if you are interested in results in terms of seconds.
        Either dateFormat or timeFormat must be passed, but not both.
        Use https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html directives to express formats.
    :type timeFormat: str
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "freshness", "columns": columns}
    if dateFormat:
        params["dateFormat"] = dateFormat
    elif timeFormat:
        params["timeFormat"] = timeFormat
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _and_conditions_as_columns(conditions):
    """
    Return conditions as an "and" concatenation of columns.

    :param conditions:
    :type conditions: list
    :return:
    """
    # add first condition
    cond = conditions[0]
    if "casted_to" in cond:
        casted_to = cond["casted_to"]
        if cond["operator"] == "gt":
            result = col(cond["column"]).cast(casted_to) > cond["value"]
        elif cond["operator"] == "lt":
            result = col(cond["column"]).cast(casted_to) < cond["value"]
        elif cond["operator"] == "eq":
            result = col(cond["column"]).cast(casted_to) == cond["value"]
    else:
        if cond["operator"] == "gt":
            result = col(cond["column"]) > cond["value"]
        elif cond["operator"] == "lt":
            result = col(cond["column"]) < cond["value"]
        elif cond["operator"] == "eq":
            result = col(cond["column"]) == cond["value"]

    # add the rest
    for cond in conditions[1:]:
        if "casted_to" in cond:
            casted_to = cond["casted_to"]
            if cond["operator"] == "gt":
                result = result & (col(cond["column"]).cast(casted_to) > cond["value"])
            elif cond["operator"] == "lt":
                result = result & (col(cond["column"]).cast(casted_to) < cond["value"])
            elif cond["operator"] == "eq":
                result = result & (col(cond["column"]).cast(casted_to) == cond["value"])
        else:
            if cond["operator"] == "gt":
                result = result & (col(cond["column"]) > cond["value"])
            elif cond["operator"] == "lt":
                result = result & (col(cond["column"]) < cond["value"])
            elif cond["operator"] == "eq":
                result = result & (col(cond["column"]) == cond["value"])
    return result


def _andcheckjoin(then):
    """
    Returns a column which is the and concatenation of different checks,
    1 for each column in 'then', checking for each distinct_then%s, where %s is a column
    in 'then', to be equal to 1.

    :param then: 'then' columns of the constraint metric
    :type then: list
    :return:
    """
    res = col("distinct_then%s" % then[0]) == 1
    for c in then[1:]:
        res = res & (col("distinct_then%s" % c) == 1)
    return res


def _constraint_todo(when, then, conditions, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param when:
    :type when: list
    :param then:
    :type then: list
    :param conditions:
    :type conditions: list
    :param df:
    :type df: DataFrame
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
    todo = todo.agg(count("*").alias("metrics_check_count_1"),
                    *[countDistinct(c).alias("distinct_then%s" % c) for c in then])

    # given the new 'table', aggregate over it, summing over all total rows to get the total number of filtered
    # rows, and summing the count only of groups that have one distinct then value
    todo = todo.agg(sum("metrics_check_count_1").alias("all_filtered"), sum(
        pyspark.sql.functions.when(_andcheckjoin(then), col("metrics_check_count_1")).otherwise(0)).alias(
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
    :type when: list
    :param then: A list of columns in the df to use as the postcondition of a functional constraint. No column
        should be in both when and then.
    :type then: list
    :param conditions: Conditions on which to filter data before applying the metric.
    :type conditions: list
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "constraint", "when": when, "then": then}
    if conditions:
        params["conditions"] = conditions
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _rule_todo(conditions):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param conditions:
    :type conditions: list
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
    :type conditions: list
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "rule", "conditions": conditions}
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _having_aggregations_as_columns(condition):
    """
    Return an "having" aggregation as a column.

    :param condition:
    :type condition: dict
    :return:
    """
    column = condition["column"]
    aggregator = condition["aggregator"] if "aggregator" in condition else None
    if "casted_to" in condition:
        casted_to = condition["casted_to"]
        if aggregator == "count":
            return count(column)
        elif aggregator == "min":
            return pyspark.sql.functions.min(col(column).cast(casted_to))
        elif aggregator == "max":
            return pyspark.sql.functions.max(col(column).cast(casted_to))
        elif aggregator == "avg":
            return pyspark.sql.functions.avg(col(column).cast(casted_to))
        elif aggregator == "sum":
            return pyspark.sql.functions.sum(col(column).cast(casted_to))
        else:
            print("Aggregator %s not recognized" % aggregator)
            exit()
    else:
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
        else:
            print("Aggregator %s not recognized" % aggregator)
            exit()


def _having_constraints_as_column(having):
    """
    Return "having" conditions as an "and" concatenation of columns.

    :param having:
    :type having: list
    :return:
    """
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
            result = result & (col("_grouprule_h%i" % index) > cond["value"])
        elif cond["operator"] == "lt":
            result = result & (col("_grouprule_h%i" % index) < cond["value"])
        elif cond["operator"] == "eq":
            result = result & (col("_grouprule_h%i" % index) == cond["value"])
        index += 1
    return result


def _grouprule_todo(columns, conditions, having, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param columns:
    :type columns: list
    :param conditions:
    :type conditions: list
    :param having:
    :type having: list
    :param df:
    :type df: DataFrame
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
    If a df is passed, the groupRule metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param columns: Columns on which to run the metric, grouping data.
    :param conditions: Conditions on which to run the metric, filtering data before grouping, can be None.
    :type conditions: list
    :param having: Conditions to apply to groups.
    :type having: list
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "groupRule", "columns": columns, "having": having}
    if conditions is not None:
        params["conditions"] = conditions
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _entropy_todo(column, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param column:
    :type column: str/int
    :param df:
    :type df: DataFrame
    :return: Pyspark columns representing what to compute.
    """
    # group on that column
    todo = df.groupBy(column)

    # count instances of each group
    todo = todo.agg(count("*").alias("_entropy_ci"))
    # ignore nans/null for computing entropy
    todo = todo.filter(~ col(column).isNull())
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
    :type column: str/int
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "entropy", "column": column}
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _mutual_info_todo(when, then, df):
    """
    Returns what (columns, as in spark columns) to compute to get the results requested by
    the parameters.

    :param when:
    :type when: str/int
    :param then:
    :type then: str/int
    :param df:
    :type df: DataFrame
    :return: Pyspark columns representing what to compute.
    """
    # group on the pair of columns, count occurrences
    pairs_table = df.groupBy([when, then]).agg(count("*").alias("_pairs_count"))
    # ignore nulls
    pairs_table = pairs_table.filter((~ col(when).isNull()) & (~ col(then).isNull()))
    pairs_table.cache()

    when_table = pairs_table.groupBy(col(when).alias("wt")).agg(sum("_pairs_count").alias("_when_count"))
    then_table = pairs_table.groupBy(col(then).alias("tt")).agg(sum("_pairs_count").alias("_then_count"))
    final_table = pairs_table.join(when_table, pairs_table[when].eqNullSafe(when_table["wt"]))
    final_table = final_table.join(then_table, final_table[then].eqNullSafe(then_table["tt"]))

    # prepare 4 subformulas of MI to later sum, plus the total
    todo = final_table.select(sum(col("_pairs_count") * log(col("_pairs_count"))).alias("_s1"),  # c_xy * logc_xy
                              sum(col("_pairs_count")).alias("_s2"),  # c_xy
                              sum(col("_pairs_count") * log(col("_when_count"))).alias("_s3"),  # c_xy * logc_x
                              sum(col("_pairs_count") * log(col("_then_count"))).alias("_s4"),  # c_xy * logc_y
                              sum(col("_pairs_count")).alias("_total")  # total
                              )
    todo = todo.select((col("_s1") / col("_total")) + (log(col("_total")) * (col("_s2") / col("_total"))) - (
            (col("_s3")) / col("_total")) - ((col("_s4")) / col("_total")).alias("mutual_info"))
    return todo


def mutual_info(when, then, df=None):
    """
    If a df is passed, the mutual_info metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param when: First column on which to compute MI.
    :type when: str/int
    :param then: Second column on which to compute MI.
    :type then: str/int
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
        this metric to be run later.
    :type df: DataFrame
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
        run later.
    :rtype: list/Task
    """
    # make a dict representing the parameters
    params = {"metric": "mutual_info", "when": when, "then": then}
    # create tak containing parameters
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]
