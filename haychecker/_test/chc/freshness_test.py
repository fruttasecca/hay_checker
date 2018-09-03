import datetime
import time as timelib
import unittest

import numpy as np
import pandas as pd

from haychecker.chc.metrics import freshness


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


class TestFreshness(unittest.TestCase):

    def test_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        r1, r2 = freshness(["c1", "c2"], dateFormat="%d:%M:%y", df=df)
        self.assertEqual("None days", r1)
        self.assertEqual("None days", r2)

        r1, r2 = freshness(["c1", "c2"], timeFormat="%d:%M:%y", df=df)
        self.assertEqual("None seconds", r1)
        self.assertEqual("None seconds", r2)

    def test_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = [np.NaN for _ in range(100)]
        df["c1"] = df["c1"].astype(str)
        df["c2"] = df["c2"].astype(str)

        r1, r2 = freshness(["c1", "c2"], dateFormat="%d:%M:%y", df=df)
        self.assertEqual(r1, "nan days")
        self.assertEqual(r2, "nan days")

        r1, r2 = freshness(["c1", "c2"], timeFormat="%d:%M:%y", df=df)
        self.assertEqual(r1, "nan seconds")
        self.assertEqual(r2, "nan seconds")

    def test_dateformat(self):
        format = "%Y-%m-%d %H:%M:%S"
        now = str(datetime.datetime.now())[:19]

        # test wrong type of column
        df = pd.DataFrame()
        dates = [i for i in range(100)]
        df["c1"] = dates

        # test correct type
        df = pd.DataFrame()
        dates = [now for _ in range(100)]
        df["c1"] = dates
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], dateFormat=format, df=df)
        self.assertEqual(r1, "0.0 days")
        self.assertEqual(r2, "0.0 days")
        self.assertEqual(r3, "0.0 days")

        df = pd.DataFrame()
        dates = [now for _ in range(100)]
        for i in range(20):
            dates[-(i + 1)] = None
        df["c1"] = dates
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], dateFormat=format, df=df)
        self.assertEqual(r1, "0.0 days")
        self.assertEqual(r2, "0.0 days")
        self.assertEqual(r3, "0.0 days")

    def test_timeformat_nodate(self):
        format = "%H:%M:%S"
        now = str(datetime.datetime.now())[11:19]

        # test wrong type of column
        df = pd.DataFrame()
        times = [i for i in range(100)]
        df["c1"] = times
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        df = pd.DataFrame()
        times = [now for _ in range(100)]
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

        df = pd.DataFrame()
        times = [now for _ in range(100)]
        for i in range(20):
            times[-(i + 1)] = None
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

    def test_timeformat_nodate_dateincolumns(self):
        format = "%H:%M:%S"
        now = str(datetime.datetime.now())[11:19]

        # test wrong type of column
        df = pd.DataFrame()
        times = [i for i in range(100)]
        df["c1"] = times
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        df = pd.DataFrame()
        times = [now for _ in range(100)]
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

        df = pd.DataFrame()
        times = [now for _ in range(100)]
        for i in range(20):
            times[-(i + 1)] = ""
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

    def test_timeformat_withdate(self):
        format = "%Y-%m-%d %H:%M:%S"
        time = str(datetime.datetime.now())[11:19]
        time = "1970-01-01 " + time

        # test wrong type of column
        df = pd.DataFrame()
        times = [i for i in range(100)]
        df["c1"] = times
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        df = pd.DataFrame()
        times = [time for _ in range(100)]
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        # seconds from 1970 plus 10 seconds for computation time
        seconds = int(timelib.time()) + 10

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, seconds)
        self.assertLessEqual(r2, seconds)
        self.assertLessEqual(r3, seconds)

        df = pd.DataFrame()
        times = [time for _ in range(100)]
        for i in range(20):
            times[-(i + 40)] = np.NaN
        df["c1"] = times
        df["c2"] = pd.to_datetime(df["c1"], errors="coerce", format=format)
        df["c3"] = pd.to_datetime(df["c1"], errors="coerce", format=format)

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, seconds)
        self.assertLessEqual(r2, seconds)
        self.assertLessEqual(r3, seconds)
