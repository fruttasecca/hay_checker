"""
Checks like consistency between
the metric to compute and column types, etc, these
checks are considered run time checks (having a df to check against), and
to be run after running the parameters check.
"""


def completeness_run_check(columns, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param columns: Columns on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def deduplication_run_check(columns, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param columns: Columns on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def deduplication_approximated_run_check(columns, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param columns: Columns on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def timeliness_run_check(columns, value, df, dateFormat=None, timeFormat=None):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param columns: Columns on which to run the metric
    :param value: Value of the date/time, a string, in the dateFormat or timeFormat that is passed, currently
    not used.
    :param df: Dataframe on which to run the metric
    :param dateFormat: Dateformat for values that are dates.
    :param timeFormat: Timeformat for values that are times.
    """
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "

    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def freshness_run_check(columns, df, dateformat=None, timeFormat=None):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param dateformat: Currently not used.
    :param timeFormat: Currently not used.
    :param columns: Columns on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def constraint_run_check(when, then, conditions, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param when: A list of columns part of the df, used for functional dependencies.
    :param then: A list of columns part of the df, used for functional dependencies.
    :param conditions: Conditions on which to filter before running the metric, can be None.
    :param df: Dataframe on which to run the metric
    """
    allwhenthen = when + then
    for c in allwhenthen:
        assert not (c in when and c in then), "Column '%s' is in both when and then fields" % c
        assert c in df.columns, "Column '%s' is not in the dataframe" % c

    if conditions:
        assert type(conditions) is list, "Conditions should be a list of dictionaries."
        for cond in conditions:
            assert cond["column"] in df.columns, "Column %s in conditions %s is not part of the df" % (
                cond["column"], cond)


def rule_run_check(conditions, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param conditions: Conditions on which to filter before running the metric.
    :param df: Dataframe on which to run the metric
    """
    for cond in conditions:
        assert cond["column"] in df.columns, "Column %s in conditions %s is not part of the df" % (
            cond["column"], cond)


def grouprule_run_check(columns, conditions, having, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param columns: Columns on which to run the metric
    :param conditions: Conditions on which to filter before running the metric.
    :param having: Conditions on which to filter the groups after grouping.
    :param df: Dataframe on which to run the metric
    """
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col

    for cond in having:
        assert cond["column"] in df.columns or cond[
            "column"] == "*", "Column '%s' is not a column part of the dataframe" % cond["column"]

    if conditions is not None:
        for cond in conditions:
            assert cond["column"] in df.columns, "Column '%s' is not a column part of the dataframe" % cond["column"]


def entropy_run_check(column, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param column: Column on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    assert column in df.columns, "Column '%s' is not a column part of the dataframe" % column


def mutual_info_run_check(when, then, df):
    """
    Check for consistency between parameters and the dataframe, an assertion
    error will incur if the check is not passed.
    :param when: First column on which to run the metric
    :param then: Second column on which to run the metric
    :param df: Dataframe on which to run the metric
    """
    assert when in df.columns, "Column '%s' is not a column part of the dataframe" % when
    assert then in df.columns, "Column '%s' is not a column part of the dataframe" % then
    assert when != then, "When and then must differ"
