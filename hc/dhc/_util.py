"""
Checks like consistency between
the metric to compute and column types, etc.
TODO: add all the possible digits or remove this check to allow the full usage of simple date time
"""

__possible_digits = ["D", "d", "M", "m", "Y", "y", "H", "h", "M", "m", "S", "s"]


def _completeness_check(columns, df):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def _deduplication_check(columns, df):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def _timeliness_check(columns, value, df, dateFormat=None, timeFormat=None):
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


def _freshness_check(columns, df, dateformat=None, timeFormat=None):
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col
