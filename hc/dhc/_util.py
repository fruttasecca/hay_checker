"""
Checks like consistency between
the metric to compute and column types, etc.
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


def _timeliness_check(columns, value, format, df):
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


def _freshness_check(columns, format, df):
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col
