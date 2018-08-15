"""
Checks like consistency between
the metric to compute and column types, etc.
TODO: add all the possible digits or remove this check to allow the full usage of simple date time
"""

__possible_digits = ["D", "d", "M", "m", "Y", "y", "H", "h", "M", "m", "S", "s"]
__allowed_operators = ["eq", "gt", "lt"]


def completeness_run_check(columns, df):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def deduplication_run_check(columns, df):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def timeliness_run_check(columns, value, df, dateFormat=None, timeFormat=None):
    assert (dateFormat is None or timeFormat is None) and (
            not dateFormat is None or not timeFormat is None), "Pass either a dateFormat or a timeFormat, " \
                                                               "not both. "

    format = dateFormat if dateFormat else timeFormat
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col
    assert len(value) == len(format), "Value %s and format %s have different length" % (value, format)
    for i, (vchar, fchar) in enumerate(zip(value, format)):
        if fchar in __possible_digits:
            # then vchar must be a digit
            assert vchar.isdigit(), "Character at position %s in %s is not a digit but it should be, given the format %s" % (
                i, value, format)
        else:
            # else it's a separator like ':' for HH:MM:ss
            assert vchar == fchar, "Character at position %s in %s should be the same at position %s in %s" % (
                i, value, i, format)


def freshness_run_check(columns, df, dateformat=None, timeFormat=None):
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def constraint_run_check(when, then, conditions, df):
    allwhenthen = when + then
    for c in allwhenthen:
        assert not (c in when and c in then), "Column '%s' is in both when and then fields" % c

    if conditions:
        assert type(conditions) is list, "Conditions should be a list of dictionaries."
        for cond in conditions:
            assert type(cond) is dict and len(cond) == 3, "Every condition should be a dict of 3 elements. \n%s" % cond
            assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
            assert cond["column"] in df.columns, "Column %s in conditions %s is not part of the df" % (
                cond["column"], cond)
            assert "operator" in cond and (
                    type(cond["operator"]) is str and cond["operator"] in __allowed_operators), "Unknown operator"
            assert "value" in cond and (
                    type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"] is float))
            if "operator" == "gt" or "operator" == "lt":
                assert type(cond["value"]) is int or type(cond["value"]) is float, "Non numerical value for numerical " \
                                                                                   "operator. "


def rule_run_check(conditions, df):
    assert not (conditions is None), "No conditions have been passed."
    assert type(conditions) is list, "Conditions should be a list of dictionaries."
    assert len(conditions) > 0, "Conditions list is empty"
    for cond in conditions:
        assert type(cond) is dict and len(cond) == 3, "Every condition should be a dict of 3 elements. \n%s" % cond
        assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
        assert cond["column"] in df.columns, "Column %s in conditions %s is not part of the df" % (
            cond["column"], cond)
        assert "operator" in cond and (
                type(cond["operator"]) is str and cond["operator"] in __allowed_operators), "Unknown operator"
        assert "value" in cond and (
                type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"] is float))
        if "operator" == "gt" or "operator" == "lt":
            assert type(cond["value"]) is int or type(cond["value"]) is float, "Non numerical value for numerical " \
                                                                               "operator. "


def grouprule_run_check(columns, conditions, having, df):
    assert type(columns) is list, "Columns should be a list."
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col

    assert not (conditions is None), "No conditions have been passed."
    assert type(conditions) is list, "Conditions should be a list of dictionaries."
    assert len(conditions) > 0, "Conditions list is empty"
    for cond in conditions:
        assert type(cond) is dict and len(cond) == 3, "Every condition should be a dict of 3 elements. \n%s" % cond
        assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
        assert cond["column"] in df.columns or cond["column"] == "count(*)", "Column %s in conditions %s is not part " \
                                                                             "of the df, and is not equal to 'count(" \
                                                                             "*)' either." % (
                                                                                 cond["column"], cond)
        assert "operator" in cond and (
                type(cond["operator"]) is str and cond["operator"] in __allowed_operators), "Unknown operator"
        assert "value" in cond and (
                type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"] is float))
        if "operator" == "gt" or "operator" == "lt":
            assert type(cond["value"]) is int or type(cond["value"]) is float, "Non numerical value for numerical " \
                                                                               "operator. "
    if not (having is None):
        assert type(having) is list, "Having should be a list of conditions"
        assert len(having) > 0, "Having list is empty"
        for cond in having:
            assert type(cond) is dict and len(cond) == 4, "Every having condition should be a dict of 4 elements. \n%s" % cond
            assert "column" in cond and (type(cond["column"]) is str or type(cond["column"]) is int)
            """
            Currently not checking on column for 'having' conditions to allow for stuff like min(), max, et
            """
            # assert cond["column"] in df.columns or cond["column"] == "count(*)", "Column %s in conditions %s is not part " \
            #                                                                      "of the df, and is not equal to 'count(" \
            #                                                                      "*)' either." % (
            #                                                                          cond["column"], cond)
            assert "operator" in cond and (
                    type(cond["operator"]) is str and cond["operator"] in __allowed_operators), "Unknown operator"
            assert "value" in cond and (
                    type(cond["value"]) is str or type(cond["value"]) is int or type(cond["value"] is float))
            if "operator" == "gt" or "operator" == "lt":
                assert type(cond["value"]) is int or type(cond["value"]) is float, "Non numerical value for numerical " \
                                                                                   "operator. "


def _seconds_to_timeFormat(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)
