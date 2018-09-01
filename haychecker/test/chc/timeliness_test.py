import random
import unittest

import numpy as np
import pandas as pd

from haychecker.chc.metrics import timeliness


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


class TestTimeliness(unittest.TestCase):

    def test_empty(self):
        df = pd.DataFrame()

        df["c1"] = pd.Series(dtype=str)
        df["c2"] = pd.Series(dtype=str)

        r1, r2 = timeliness(["c1", "c2"], dateFormat="%d:%m:%Y", df=df, value="10:12:1980")
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)

    def test_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = ["np.NaN" for _ in range(100)]
        df["c1"] = df["c1"].astype(str)
        df["c2"] = df["c1"].astype(str)

        r1, r2 = timeliness(["c1", "c2"], dateFormat="%d:%m:%Y", df=df, value="10:12:1980")
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)

        r1, r2 = timeliness(["c1", "c2"], timeFormat="%S:%M:%H", df=df, value="10:12:19")
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)

    def test_dateformat(self):
        format = "%d/%m/%Y"

        # test wrong type of column
        df = pd.DataFrame()
        dates = [i for i in range(100)]
        df["c1"] = dates
        value = "21/05/2000"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], dateFormat=format, df=df, value=value)

        # test correct type
        df = pd.DataFrame()
        dates = ["10/05/2000" for _ in range(50)]
        dates.extend(["20/05/2000" for _ in range(50)])
        random.shuffle(dates)
        df["c1"] = dates
        df["c2"] = pd.to_datetime(df["c1"], errors="raise", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        value = "21/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "20/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "10/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "12/12/1999"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        df = pd.DataFrame()
        dates = ["10/05/2000" for _ in range(50)]
        dates.extend(["20/05/2000" for _ in range(50)])
        for i in range(20):
            dates[-(i + 1)] = None
        df["c1"] = dates
        df["c2"] = to_datetime_cached(df["c1"], format)
        df["c3"] = to_datetime_cached(df["c1"], format)

        value = "21/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 80.)
        self.assertEqual(r2, 80.)
        self.assertEqual(r3, 80.)

        value = "20/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "10/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "12/12/1999"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

    def test_timeformat_nodate(self):
        format = "%S:%H:%M"

        # test wrong type of column
        df = pd.DataFrame()
        times = [i for i in range(100)]
        df["c1"] = times
        value = "21:05:50"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], timeFormat=format, df=df, value=value)

        # test correct type
        df = pd.DataFrame()
        times = ["10:05:50" for _ in range(50)]
        times.extend(["01:18:01" for _ in range(50)])
        random.shuffle(times)
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        value = "01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "00:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "10:05:50"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        df = pd.DataFrame()
        times = ["10:05:50" for _ in range(50)]
        times.extend(["00:18:01" for _ in range(50)])
        for i in range(20):
            times[-(i + 1)] = np.NaN
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        value = "00:19:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 80.)
        self.assertEqual(r2, 80.)
        self.assertEqual(r3, 80.)

        value = "00:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)

        value = "10:05:50"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

    def test_timeformat_withdate(self):
        format = "%d/%m/%Y %S:%H:%M"

        # test wrong type of column
        df = pd.DataFrame()
        times = [i for i in range(100)]
        df["c1"] = times
        value = "01/10/1900 21:05:50"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], timeFormat=format, df=df, value=value)

        # test correct type
        df = pd.DataFrame()
        times = ["01/10/1900 21:05:50" for _ in range(50)]
        times.extend(["01/10/1900 01:18:01" for _ in range(50)])
        times.extend(["21/10/1900 01:18:01" for _ in range(100)])
        random.shuffle(times)
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        value = "21/10/1900 01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "21/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "01/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 25.)
        self.assertEqual(r2, 25.)
        self.assertEqual(r3, 25.)

        value = "01/10/1900 21:05:10"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "01/01/1801 01:01:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        df = pd.DataFrame()
        times = ["01/10/1900 21:05:50" for _ in range(50)]
        times.extend(["01/10/1900 01:18:01" for _ in range(50)])
        times.extend(["21/10/1900 01:18:01" for _ in range(100)])
        for i in range(1, 11):
            times[-i] = np.NaN
            times[-(i + 10)] = None
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        value = "21/10/1900 01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 90.)
        self.assertEqual(r2, 90.)
        self.assertEqual(r3, 90.)

        value = "21/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "01/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 25.)
        self.assertEqual(r2, 25.)
        self.assertEqual(r3, 25.)

        value = "01/10/1900 21:05:10"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "01/01/1801 00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)
