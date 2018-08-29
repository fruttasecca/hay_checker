"""
Module containing metrics for the centralized version of hay_checker.
"""
import pandas as pd
import numpy as np
from sklearn.metrics import mutual_info_score

from . import task


def _completeness_todo(columns, df):
    """
    Returns what to compute for each column in dict form, given the metric parameters.
    :param columns:
    :param df:
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    """
    todo = dict()
    if columns is None:
        columns = list(df.columns)
    for col in columns:
        todo[col] = ["count"]
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
    :param df:
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
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
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
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


def contains_date(format):
    """
    strtime tokens
    todo add more
    """
    part_of_date_tokens = "dmyY"
    for token in part_of_date_tokens:
        if token in format:
            return True
    return False


def to_datetime_cached(s, format):
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


def _convert_format(format):
    res = []
    seen = set()
    format = [x for x in format if not ((x.isalpha() and x in seen) or seen.add(x))]
    for char in format:
        if char.isalpha():
            res.append("%")
        res.append(char)
    return "".join(res)


def _timeliness_todo(columns, value, types, dateFormat=None, timeFormat=None):
    """
    Returns what to compute for each column in dict form, given the metric parameters.
    :param columns:
    :param value
    :param dateFormat:
    :param timeFormat:
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = dict()

    if dateFormat:
        cdateFormat = _convert_format(dateFormat)
        cvalue = pd.to_datetime(value, format=cdateFormat)
        for col in columns:
            if types[col] == str:
                def _timeliness_agg(x):
                    s = to_datetime_cached(x, cdateFormat)
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
        ctimeFormat = _convert_format(timeFormat)
        cvalue = pd.to_datetime(value, format=ctimeFormat)

        # check if value contains a date and not only hours, minutes, seconds
        has_date = contains_date(timeFormat)
        if has_date:
            for col in columns:
                if types[col] == str:
                    def _timeliness_agg(x):
                        s = to_datetime_cached(x, ctimeFormat)
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
            cvalue = pd.to_datetime(value, format=ctimeFormat)
            cvalue = pd.Timestamp(second=cvalue.second, hour=cvalue.hour, minute=cvalue.minute, day=day, month=month,
                                  year=year)

            for col in columns:
                if types[col] == str:
                    def _timeliness_agg(x):
                        s = to_datetime_cached(x, ctimeFormat)
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
    :param dateFormat:
    :param timeFormat:
    :return: Dict containing what to run (pandas functions names or named lambdas) for each column.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "
    todo = dict()

    if dateFormat:
        cdateFormat = _convert_format(dateFormat)
        now = pd.Timestamp.today()
        for col in columns:
            if types[col] == str:
                def _freshness_agg(s):
                    s = to_datetime_cached(s, cdateFormat)
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
        ctimeFormat = _convert_format(timeFormat)
        now = pd.Timestamp.now()

        # check if value contains a date and not only hours, minutes, seconds
        has_date = contains_date(timeFormat)

        if has_date:
            """
            If the time format also contains a date it means the user is also interested in comparing years, months, days, 
            etc.
            """
            for col in columns:
                if types[col] == str:
                    def _freshness_agg(s):
                        s = to_datetime_cached(s, ctimeFormat)
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
                        s = to_datetime_cached(s, ctimeFormat)
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
    :param df:
    :return: Boolean series representing what rows are passing the conditions, can be used as indices to
    get those rows from the df.
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
    :param then:
    :param conditions:
    :param df:
    :return: Result of the metric.
    """

    # filter if needed
    if conditions:
        conds = _and_conditions_as_columns(conditions, df)
        groups = df[conds]

    # create named lambdas for each 'then' column
    aggs_lambdas = dict()
    for col in then:
        def _constraint_agg(s):
            return s.nunique()

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
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
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
    :return: Result of the metric.
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
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    run later.
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


def _grouby_aggregations(conditions):
    """
    Returns functions to compute for each column, given the conditions.

    :param conditions: List of dictionaries which represent "having" conditions, i.e.
    having min(col1) > 5.
    :return: A dict mapping each column to named lambdas to compute on that column, which results
    will be later required to check on the conditions in the "conditions" parameter.
    """
    aggs = dict()
    for cond in conditions:
        # do not include count(*) in aggregations
        if cond["aggregator"] == "count" and cond["column"] == "*":
            continue

        column = cond["column"]
        aggregator = cond["aggregator"] if "aggregator" in cond else None
        if column not in aggs:
            aggs[column] = []

        if "casted_to" in cond:
            if aggregator == "count":
                def _groupby_agg(s):
                    return pd.to_numeric(s, errors="coerce").count()

                aggs[column].append(_groupby_agg)
            elif aggregator == "min":
                def _groupby_agg(s):
                    return pd.to_numeric(s, errors="coerce").min()

                aggs[column].append(_groupby_agg)
            elif aggregator == "max":
                def _groupby_agg(s):
                    return pd.to_numeric(s, errors="coerce").max()

                aggs[column].append(_groupby_agg)
            elif aggregator == "avg":
                def _groupby_agg(s):
                    return pd.to_numeric(s, errors="coerce").mean()

                aggs[column].append(_groupby_agg)
            elif aggregator == "sum":
                def _groupby_agg(s):
                    return pd.to_numeric(s, errors="coerce").sum()

                aggs[column].append(_groupby_agg)
            else:
                print("Aggregator %s not recognized" % aggregator)
                exit()
        else:
            if aggregator == "count":
                def _groupby_agg(s):
                    return s.count()

                aggs[column].append(_groupby_agg)
            elif aggregator == "min":
                def _groupby_agg(s):
                    return s.min()

                aggs[column].append(_groupby_agg)
            elif aggregator == "max":
                def _groupby_agg(s):
                    return s.max()

                aggs[column].append(_groupby_agg)
            elif aggregator == "avg":
                def _groupby_agg(s):
                    return s.mean()

                aggs[column].append(_groupby_agg)
            elif aggregator == "sum":
                def _groupby_agg(s):
                    return s.sum()

                aggs[column].append(_groupby_agg)
            else:
                print("Aggregator %s not recognized" % aggregator)
                exit()
        aggs[column][-1].__name__ = ("_groupby_agg_%s_%s" % (column, aggregator))
    _remove_duplicate_groupby_aggs(aggs)
    return aggs


def _grouprule_aggs_filter(having):
    """
    Given (having) conditions, return what to filter on as a string, to be used
    after groupbys as grouped.query(string returned by this function).
    :param having:
    :return: String to be used on a df.query to filter based on the "having" conditions.
    """
    # add first condition
    cond = having[0]
    operator_map = dict()
    operator_map["gt"] = ">"
    operator_map["lt"] = "<"
    operator_map["eq"] = "=="
    first_operator = cond["operator"]

    if cond["aggregator"] == "count" and cond["column"] == "*":
        result = "count %s %s" % (operator_map[first_operator], cond["value"])
    else:
        result = "_groupby_agg_%s_%s %s %s" % (
            cond["column"], cond["aggregator"], operator_map[first_operator], cond["value"])

    # add the rest
    for cond in having[1:]:
        operator = cond["operator"]
        if cond["aggregator"] == "count" and cond["column"] == "*":
            result = result + " and count %s %s" % (operator_map[operator], cond["value"])
        else:
            result = result + " and _groupby_agg_%s_%s %s %s" % (
                cond["column"], cond["aggregator"], operator_map[operator], cond["value"])
    return result


def _grouprule_compute(columns, conditions, having, df):
    """
    Computes the metric result.
    :param columns:
    :param conditions:
    :param having:
    :param df:
    :return: Result of the metric.
    """
    # filter if needed
    if conditions:
        conds = _and_conditions_as_columns(conditions, df)
        groups = df[conds]

    groups = groups.groupby(columns)
    tmp = groups.agg(_grouby_aggregations(having))
    tmp.columns = tmp.columns.droplevel(0)
    has_count_all = any([(h["column"] == "*" and h["aggregator"] == "count") for h in having])
    if has_count_all:
        count = groups.size().to_frame(name='count')
        groups = count.join(tmp)
        groups.reset_index()
    else:
        groups = tmp
    print(groups)
    total_groups = len(groups.index)
    passing_groups = len(groups.query(_grouprule_aggs_filter(having)).index)
    return passing_groups / total_groups


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
    :param df:
    :return: Result of the metric.
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
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
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
    :param then:
    :param df:
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
    :param then: Second column on which to compute MI.
    :param df: Dataframe on which to run the metric, None to have this function return a Task instance containing
    this metric to be run later.
    :return: Either a list of scores or a Task instance containing this metric (with these parameters) to be
    run later.
    """
    # make a dict representing the parameters
    params = {"metric": "mutual_info", "when": when, "then": then}
    # create tak containing parameters
    t = task.Task([params])
    if df is None:
        return t
    else:
        return t.run(df)[0]["scores"]
