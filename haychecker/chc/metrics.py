"""
Module containing metrics for the centralized version of hay_checker.
Some functions parameters are unused, they have been kept like this to allow
easier code evolution.
"""
import numpy as np
import pandas as pd
from sklearn.metrics import mutual_info_score

from haychecker.chc import task


def _completeness_todo(columns, df):
    """
    Returns what to compute for each column in dict form, given the metric parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    :rtype: dict
    """
    todo = dict()
    if columns is None:
        columns = list(df.columns)
    for col in columns:
        todo[col] = ["count"]
    return todo


def completeness(columns=None, df=None):
    """
    If a df is passed, the completeness metric will be run and results returned
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
    Returns what to compute for each column in dict form, given the metric parameters.

    :param columns:
    :type columns: list
    :param df:
    :type df: DataFrame
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    :rtype: dict
    """
    todo = dict()
    for col in columns:
        todo[col] = ["nunique"]
    return todo


def deduplication(columns=None, df=None):
    """
    If a df is passed, the deduplication metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param columns: Columns on which to run the metric, None to run the deduplication
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
    params = {"metric": "deduplication"}
    if not (columns is None):
        params["columns"] = columns
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]


def _contains_date(format):
    """
    Check if a format contains tokens related to date.

    :param format:
    :type format: str
    :return: True if format contains tokens related to date, false otherwise.
    :rtype: boolean
    """
    part_of_date_tokens = "aAwdbBmyYjUW"
    for token in part_of_date_tokens:
        if token in format:
            return True
    return False


def _to_datetime_cached(s, format):
    """
    Transform a series of strings (dates) to datetimes, with a dict
    to cache results.

    :param s:
    :param format:
    :return:
    """
    dates = {date: pd.to_datetime(date, errors="coerce", format=format) for date in s.dropna().unique()}
    dates[np.NaN] = None
    return s.map(dates)


def _set_year_month_day(series, year=None, month=None, day=None):
    return pd.to_datetime(
        {'year': year,
         'month': month,
         'day': day,
         "hour": series.dt.hour,
         "minute": series.dt.minute,
         "second": series.dt.second
         }, errors="coerce")


def _set_hour_minute_second(series, hour=None, minute=None, second=None):
    return pd.to_datetime(
        {'year': series.dt.year,
         'month': series.dt.month,
         'day': series.dt.day,
         "hour": hour,
         "minute": minute,
         "second": second
         }, errors="coerce")


def _timeliness_todo(columns, value, types, dateFormat=None, timeFormat=None):
    """
    Returns what to compute for each column in dict form, given the metric parameters.

    :param columns:
    :type columns: list
    :param value
    :type value: str
    :param dateFormat:
    :type dateFormat: str
    :param timeFormat:
    :type timeFormat: str
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    :rtype: dict
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = dict()

    if dateFormat:
        cvalue = pd.to_datetime(value, format=dateFormat)
        for col in columns:
            if types[col] == str:
                def _timeliness_agg(x):
                    s = _to_datetime_cached(x, dateFormat)
                    return (s < cvalue).mean()

                _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "dateFormat", dateFormat, value))
                todo[col] = [_timeliness_agg]
            elif types[col] == pd._libs.tslib.Timestamp:
                def _timeliness_agg(x):
                    return (x < cvalue).mean()

                _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "dateFormat", dateFormat, value))
                todo[col] = [_timeliness_agg]
            else:
                print(
                    "Type of a column on which the timeliness metric is run must be either timestamp, "
                    "or string, if the metric is being run on dateFormat.")
                exit()
    elif timeFormat:
        cvalue = pd.to_datetime(value, format=timeFormat)

        # check if value contains a date and not only hours, minutes, seconds
        has_date = _contains_date(timeFormat)
        if has_date:
            for col in columns:
                if types[col] == str:
                    def _timeliness_agg(x):
                        s = _to_datetime_cached(x, timeFormat)
                        return (s < cvalue).mean()

                    _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "timeFormat", timeFormat, value))
                    todo[col] = [_timeliness_agg]
                elif types[col] == pd._libs.tslib.Timestamp:
                    def _timeliness_agg(x):
                        return (x < cvalue).mean()

                    _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "timeFormat", timeFormat, value))
                    todo[col] = [_timeliness_agg]
                else:
                    print(
                        "Type of a column on which the timeliness metric is run must be either timestamp or string, if "
                        "the metric is being run on a timeFormat")
                    exit()
        else:
            """
            Set year, month, day of the series equal to today, so that confrontation between the series
            and the 'value' argument will only be about hours, months, days
            """
            now = pd.to_datetime("now")
            year = now.year
            month = now.month
            day = now.day
            cvalue = pd.to_datetime(value, format=timeFormat)
            cvalue = pd.Timestamp(second=cvalue.second, hour=cvalue.hour, minute=cvalue.minute, day=day, month=month,
                                  year=year)

            for col in columns:
                if types[col] == str:
                    def _timeliness_agg(x):
                        s = _to_datetime_cached(x, timeFormat)
                        s = _set_year_month_day(s, year, month, day)
                        return (s < cvalue).mean()

                    _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "timeFormat", timeFormat, value))
                    todo[col] = [_timeliness_agg]
                elif types[col] == pd._libs.tslib.Timestamp:
                    def _timeliness_agg(x):
                        x = _set_year_month_day(x, year, month, day)
                        return (x < cvalue).mean()

                    _timeliness_agg.__name__ = ("_timeliness_agg_%s_%s_%s_%s" % (col, "timeFormat", timeFormat, value))
                    todo[col] = [_timeliness_agg]
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
    Use http://strftime.org/ directives to express formats.

    :param columns: Columns on which to run the metric, columns of type string will be casted to timestamp
        using the dateFormat or timeFormat argument.
    :type columns: list
    :param value: Value used to run the metric, confronting values in the specified columns against it.
    :type value: str
    :param dateFormat: Format in which the value (and values in columns, if they are of string type) are; used
        to cast columns if they contain dates as strings. Either dateFormat
        or timeFormat must be passed, but not both. Use http://strftime.org/ directives to express formats.
    :type dateFormat: str
    :param timeFormat: Format in which the value (and values in columns, if they are of string type) are; used
        to cast columns if they contain dates as strings. Either dateFormat
        or timeFormat must be passed, but not both. Use http://strftime.org/ directives to express formats.
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


def _freshness_todo(columns, types, dateFormat=None, timeFormat=None):
    """
    Returns what to compute for each column in dict form, given the metric parameters.

    :param columns:
    :type columns: list
    :param types: Dict mapping column names to type.
    :type types: dict
    :param dateFormat:
    :type dateFormat: str
    :param timeFormat:
    :type timeFormat: str
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    :rtype: dict
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = dict()

    if dateFormat:
        now = pd.Timestamp.today()
        for col in columns:
            if types[col] == str:
                def _freshness_agg(s):
                    s = _to_datetime_cached(s, dateFormat)
                    s = _set_hour_minute_second(s, 0, 0, 0)
                    return (now - s).astype("timedelta64[D]").abs().mean()

                _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "dateFormat", dateFormat))
                todo[col] = [_freshness_agg]
            elif types[col] == pd._libs.tslib.Timestamp:
                def _freshness_agg(s):
                    s = _set_hour_minute_second(s, 0, 0, 0)
                    return (now - s).astype("timedelta64[D]").abs().mean()

                _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "dateFormat", dateFormat))
                todo[col] = [_freshness_agg]
            else:
                print(
                    "Type of a column on which the freshness metric is run must be either timestamp "
                    "or string, if the metric is being run on dateFormat.")
                exit()
    elif timeFormat:
        now = pd.Timestamp.now()

        # check if value contains a date and not only hours, minutes, seconds
        has_date = _contains_date(timeFormat)

        if has_date:
            """
            If the time format also contains a date it means the user is also interested in comparing years, months, days, 
            etc.
            """
            for col in columns:
                if types[col] == str:
                    def _freshness_agg(s):
                        s = _to_datetime_cached(s, timeFormat)
                        return (now - s).astype("timedelta64[s]").abs().mean()

                    _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "timeFormat", timeFormat))
                    todo[col] = [_freshness_agg]
                elif types[col] == pd._libs.tslib.Timestamp:
                    def _freshness_agg(s):
                        return (now - s).astype("timedelta64[s]").abs().mean()

                    _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "timeFormat", timeFormat))
                    todo[col] = [_freshness_agg]
                else:
                    print(
                        "Type of a column on which the freshness metric is run must be either timestamp "
                        "or string, if the metric is being run on dateFormat.")
                    exit()
        else:
            """
            If the timestamp has no date the user is not interested in differences that consider years, months, days, but
            only hours, minutes, seconds.
            """
            year = now.year
            month = now.month
            day = now.day

            for col in columns:
                if types[col] == str:
                    def _freshness_agg(s):
                        s = _to_datetime_cached(s, timeFormat)
                        s = _set_year_month_day(s, year, month, day)
                        return (now - s).astype("timedelta64[s]").abs().mean()

                    _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "timeFormat", timeFormat))
                    todo[col] = [_freshness_agg]
                elif types[col] == pd._libs.tslib.Timestamp:
                    def _freshness_agg(s):
                        s = _set_year_month_day(s, year, month, day)
                        return (now - s).astype("timedelta64[s]").abs().mean()

                    _freshness_agg.__name__ = ("_freshness_agg_%s_%s_%s" % (col, "timeFormat", timeFormat))
                    todo[col] = [_freshness_agg]
                else:
                    print(
                        "Type of a column on which the freshness metric is run must be either timestamp "
                        "or string, if the metric is being run on dateFormat.")
                    exit()
    return todo


def freshness(columns, df=None, dateFormat=None, timeFormat=None):
    """
    If a df is passed, the freshness metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).
    Use http://strftime.org/ directives to express formats.

    :param columns: Columns on which to run the metric, columns of type string will be casted to timestamp
        using the dateFormat or timeFormat argument.
    :type columns: list
    :param dateFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
        be of type timestamp. Use this parameter if you are interested in a result in terms of days.
        Either dateFormat or timeFormat must be passed, but not both.
        Use http://strftime.org/ directives to express formats.
    :type dateFormat: str
    :param timeFormat: Format in which the values in columns are if those columns are of type string; otherwise they must
        be of type timestamp. Use this parameter if you are interested in results in terms of seconds.
        Either dateFormat or timeFormat must be passed, but not both.
        Use http://strftime.org/ directives to express formats.
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


def _and_conditions_as_columns(conditions, df):
    """
    Computes a boolean series given conditions of columns on the dataframes, representing
    what rows are passing the condition.

    :param conditions:
    :type conditions: list
    :param df:
    :type df: DataFrame
    :return: Boolean series representing what rows are passing the conditions, can be used as indices to
        get those rows from the df.
    :rtype: Series
    """
    # add first condition
    cond = conditions[0]
    if "casted_to" in cond:
        if cond["operator"] == "gt":
            result = pd.to_numeric(df[cond["column"]], errors="coerce") > cond["value"]
        elif cond["operator"] == "lt":
            result = pd.to_numeric(df[cond["column"]], errors="coerce") < cond["value"]
        elif cond["operator"] == "eq":
            result = pd.to_numeric(df[cond["column"]], errors="coerce") == cond["value"]
    else:
        if cond["operator"] == "gt":
            result = df[cond["column"]] > cond["value"]
        elif cond["operator"] == "lt":
            result = df[cond["column"]] < cond["value"]
        elif cond["operator"] == "eq":
            result = df[cond["column"]] == cond["value"]

    # add the rest
    for cond in conditions[1:]:
        if "casted_to" in cond:
            if cond["operator"] == "gt":
                result = result & (pd.to_numeric(df[cond["column"]], errors="coerce") > cond["value"])
            elif cond["operator"] == "lt":
                result = result & (pd.to_numeric(df[cond["column"]], errors="coerce") < cond["value"])
            elif cond["operator"] == "eq":
                result = result & (pd.to_numeric(df[cond["column"]], errors="coerce") == cond["value"])
        else:
            if cond["operator"] == "gt":
                result = result & (df[cond["column"]] > cond["value"])
            elif cond["operator"] == "lt":
                result = result & (df[cond["column"]] < cond["value"])
            elif cond["operator"] == "eq":
                result = result & (df[cond["column"]] == cond["value"])
    return result


def _constraint_compute(when, then, conditions, df):
    """
    Computes the metric result.

    :param when:
    :type when: list
    :param then:
    :type then: list
    :param conditions:
    :type conditions: list
    :param df:
    :type df: DataFrame
    :return: Result of the metric.
    :rtype: float
    """

    # filter if needed
    """
    Using _nan_constraint_filler is needed to make it so that pandas will groupby 
    also considering None values.
    """
    if conditions:
        conds = _and_conditions_as_columns(conditions, df)
        groups = df[conds].fillna("_none_constraint_filler")
    else:
        groups = df.fillna("_none_constraint_filler")

    # if no rows make it after the filtering return
    if len(groups.index) == 0:
        return 1

    # create named lambdas for each 'then' column
    aggs_lambdas = dict()
    for col in then:
        """
        Replacing _nan_constraint_filler back with None is needed to have the same semantics
        between sql/pyspark and pandas.
        """

        def _constraint_agg(s):
            return s.replace(to_replace="_none_constraint_filler", value=None).nunique()

        _constraint_agg.__name__ = "_constraint_agg_%s" % col
        aggs_lambdas[col] = [_constraint_agg]

    # group by "when" and get nuniques for each "then" column
    groups = groups.groupby(when)
    nuniques = groups.agg(aggs_lambdas)
    nuniques.columns = nuniques.columns.droplevel(0)
    nuniques.reset_index(level=when, inplace=True)

    # get counts for each group
    counts = groups.size().to_frame(name="count")
    counts.reset_index(level=when, inplace=True)
    del groups

    # join nuniques and counts to have counts for each groups
    result = pd.merge(nuniques, counts, how='inner', left_on=when, right_on=when)
    del nuniques
    del counts

    # query checking that every unique for every "then" is 1
    querystring = ["_constraint_agg_%s == 1" % col for col in then]
    querystring = " and ".join(querystring)

    passing_rows = result.query(querystring)["count"].sum()
    total_rows = result["count"].sum()
    return passing_rows / total_rows


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


def _rule_compute(conditions, df):
    """
    Computes the metric result.
    :param conditions:
    :type conditions: list
    :param df:
    :type df: DataFrame
    :return: Result of the metric.
    :rtype: float
    """
    filtering_conditions = _and_conditions_as_columns(conditions, df)
    res = filtering_conditions.mean()
    return res


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


def _remove_duplicate_groupby_aggs(aggs):
    """
    Removes duplicate aggregations from a dict mapping columns
    to named lambdas to compute, only one copy of lambdas named
    the same way will be kept.

    :param aggs:
    :type aggs: list
    :return:
    """
    # iterate over dicts, getting lists of stuff to do for each column
    for col in aggs:
        names = set()
        # remove duplicate names
        aggs[col] = list(set(aggs[col]))

        # remove duplicate custom lambdas
        tmp = []
        for item in aggs[col]:
            if hasattr(item, '__call__') and hasattr(item, "__name__"):
                if item.__name__ not in names:
                    names.add(item.__name__)
                    tmp.append(item)
            else:
                tmp.append(item)
        aggs[col] = tmp


def _grouby_aggregations(conditions, columns):
    """
    Returns functions to compute for each column, given the conditions.

    :param conditions: List of dictionaries which represent "having" conditions, i.e.
        having min(col1) > 5.
    :type conditions: list
    :param columns: Columns on which the group by is made.
    :type columns: list
    :return: A dict mapping each column to named lambdas to compute on that column, which results
        will be later required to check on the conditions in the "conditions" parameter.
    :rtype: dict
    """
    aggs = dict()
    for cond in conditions:
        column = columns[0] if (cond["aggregator"] == "count" and cond["column"] == "*") else cond["column"]
        aggregator = cond["aggregator"] if "aggregator" in cond else None
        if column not in aggs:
            aggs[column] = []

        if "casted_to" in cond:
            if cond["aggregator"] == "count" and cond["column"] == "*":
                def _groupby_agg(s):
                    return s.size

                aggs[column].append(_groupby_agg)
            elif aggregator == "count":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return pd.to_numeric(s, errors="coerce").count()

                aggs[column].append(_groupby_agg)
            elif aggregator == "min":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return pd.to_numeric(s, errors="coerce").min()

                aggs[column].append(_groupby_agg)
            elif aggregator == "max":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return pd.to_numeric(s, errors="coerce").max()

                aggs[column].append(_groupby_agg)
            elif aggregator == "avg":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return pd.to_numeric(s, errors="coerce").mean()

                aggs[column].append(_groupby_agg)
            elif aggregator == "sum":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return pd.to_numeric(s, errors="coerce").sum()

                aggs[column].append(_groupby_agg)
            else:
                print("Aggregator %s not recognized" % aggregator)
                exit()
        else:
            if cond["aggregator"] == "count" and cond["column"] == "*":
                def _groupby_agg(s):
                    return s.size

                aggs[column].append(_groupby_agg)
            elif aggregator == "count":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return s.count()

                aggs[column].append(_groupby_agg)
            elif aggregator == "min":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return s.min()

                aggs[column].append(_groupby_agg)
            elif aggregator == "max":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return s.max()

                aggs[column].append(_groupby_agg)
            elif aggregator == "avg":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return s.mean()

                aggs[column].append(_groupby_agg)
            elif aggregator == "sum":
                def _groupby_agg(s):
                    s.replace(to_replace="_none_grouprule_filler", value="None", inplace=True)
                    return s.sum()

                aggs[column].append(_groupby_agg)
            else:
                print("Aggregator %s not recognized" % aggregator)
                exit()
        if cond["aggregator"] == "count" and cond["column"] == "*":
            aggregator = "size"

        aggs[column][-1].__name__ = ("_groupby_agg_%s_%s" % (column, aggregator))
    _remove_duplicate_groupby_aggs(aggs)
    return aggs


def _grouprule_aggs_filter(having, columns):
    """
    Given (having) conditions, return what to filter on as a string, to be used
    after groupbys as grouped.query(string returned by this function).

    :param having:
    :type having: list
    :param columns: Columns on which the group by is made.
    :type columns: list
    :return: String to be used on a df.query to filter based on the "having" conditions.
    :rtype: str
    """
    # add first condition
    cond = having[0]
    operator_map = dict()
    operator_map["gt"] = ">"
    operator_map["lt"] = "<"
    operator_map["eq"] = "=="
    first_operator = cond["operator"]

    if cond["aggregator"] == "count" and cond["column"] == "*":
        result = "_groupby_agg_%s_%s %s %s" % (
            columns[0], "size", operator_map[first_operator], cond["value"])
    else:
        result = "_groupby_agg_%s_%s %s %s" % (
            cond["column"], cond["aggregator"], operator_map[first_operator], cond["value"])

    # add the rest
    for cond in having[1:]:
        operator = cond["operator"]
        if cond["aggregator"] == "count" and cond["column"] == "*":
            result = result + " and _groupby_agg_%s_%s %s %s" % (
                columns[0], "size", operator_map[operator], cond["value"])
        else:
            result = result + " and _groupby_agg_%s_%s %s %s" % (
                cond["column"], cond["aggregator"], operator_map[operator], cond["value"])
    return result


def _grouprule_compute(columns, conditions, having, df):
    """
    Computes the metric result.
    :param columns:
    :type columns: list
    :param conditions:
    :type conditions: list
    :param having:
    :type having: list
    :param df:
    :type df: DataFrame
    :return: Result of the metric.
    :rtype: float
    """
    # filter if needed
    """
    Replacing _nan_constraint_filler back with None is needed to have the same semantics
    between sql/pyspark and pandas.
    """
    if conditions:
        conds = _and_conditions_as_columns(conditions, df)
        groups = df[conds].fillna("_none_grouprule_filler")
    else:
        groups = df.fillna("_none_grouprule_filler")

    # if no rows make it after the filtering return
    if len(groups.index) == 0:
        return 1

    groups = groups.groupby(columns)
    tmp = groups.agg(_grouby_aggregations(having, columns))
    tmp.columns = tmp.columns.droplevel(0)
    groups = tmp
    total_groups = len(groups.index)
    passing_groups = len(groups.query(_grouprule_aggs_filter(having, columns)).index)
    return passing_groups / total_groups


def grouprule(columns, having, conditions=None, df=None):
    """
    If a df is passed, the groupRule metric will be run and result returned
    as a list of scores, otherwise an instance of the Task class containing this
    metric wil be returned, to be later run (possibly after adding to it other tasks/metrics).

    :param columns: Columns on which to run the metric, grouping data.
    :type columns: list
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


def _entropy_compute(column, df):
    """
    Computes the metric result.

    :param column:
    :type column: str/int
    :param df:
    :type df: DataFrame
    :return: Result of the metric.
    :rtype: float
    """
    value, counts = np.unique(df[column].dropna(), return_counts=True)
    norm_counts = counts / counts.sum()
    return -(norm_counts * np.log2(norm_counts)).sum()


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


def _mutual_info_compute(when, then, df):
    """
    Computes the metric results.

    :param when:
    :type when: str/int
    :param then:
    :type then: str/int
    :param df:
    :type df: DataFrame
    :return: Result of the metric.
    """
    index = (df[when].isna()) | (df[then].isna())
    index = ~index
    if sum(index) > 0:
        return mutual_info_score(df[when][index], df[then][index])
    else:
        return 0


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
