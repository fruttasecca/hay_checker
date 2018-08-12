"""
Checks like consistency between
the metric to compute and column types, etc.
"""

__possible_digits = ["D", "d", "M", "m", "Y", "y", "H", "h", "M", "m", "S", "s"]


def __completeness_check(df, columns=None):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def __deduplication_check(df, columns=None):
    if columns is not None:
        for col in columns:
            assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col


def __timeliness_check(df, columns, value, format):
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


def __freshness_check(df, columns, format):
    for col in columns:
        assert col in df.columns, "Column '%s' is not a column part of the dataframe" % col
