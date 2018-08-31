import random
import unittest

import numpy as np
import pandas as pd

from haychecker.chc.metrics import deduplication


class TestDeduplication(unittest.TestCase):
    def test_singlecolumns_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        r1, r2 = deduplication(["c1", "c2"], df)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)

    def test_wholetable_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        r = deduplication(df=df)[0]
        self.assertEqual(r, 100.)

    def test_singlecolumns_allsame(self):
        df = pd.DataFrame()
        df["c1"] = [chr(0) for _ in range(100)]
        df["c2"] = [10 for _ in range(100)]
        df["c3"] = [20 / 0.7 for _ in range(100)]

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 1.0)
        self.assertEqual(r2, 1.0)
        self.assertEqual(r3, 1.0)

    def test_wholetable_allsame(self):
        df = pd.DataFrame()
        df["c1"] = [chr(0) for _ in range(100)]
        df["c2"] = [10 for _ in range(100)]
        df["c3"] = [20 / 0.7 for _ in range(100)]

        r = deduplication(df=df)[0]
        self.assertEqual(r, 1.0)

    def test_singlecolumns_alldifferent(self):
        df = pd.DataFrame()
        df["c1"] = [chr(i) for i in range(100)]
        df["c2"] = [i for i in range(100)]
        df["c3"] = [i / 0.7 for i in range(100)]

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 100.0)
        self.assertEqual(r2, 100.0)
        self.assertEqual(r3, 100.0)

    def test_wholetable_alldifferent(self):
        df = pd.DataFrame()
        df["c1"] = [chr(i) for i in range(100)]
        df["c2"] = [i for i in range(100)]
        df["c3"] = [i / 0.7 for i in range(100)]

        r = deduplication(df=df)[0]
        self.assertEqual(r, 100.0)

    def test_singlecolumns_partial(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 81.0)
        self.assertEqual(r2, 81.0)
        self.assertEqual(r3, 81.0)

    def test_wholetable_partial(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r = deduplication(df=df)[0]
        self.assertEqual(r, 81.0)

    def test_singlecolumns_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = [np.NaN for _ in range(100)]
        df["c3"] = [np.NaN for _ in range(100)]

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 0.0)
        self.assertEqual(r2, 0.0)
        self.assertEqual(r3, 0.0)

    def test_wholetable_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = [np.NaN for _ in range(100)]
        df["c3"] = [np.NaN for _ in range(100)]

        r = deduplication(df=df)[0]
        self.assertEqual(r, 0.0)

    def test_singlecolumns_partialnulls(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = None
            l2[i] = np.NaN
            l3[i] = np.NaN
        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 80.0)
        self.assertEqual(r2, 80.0)
        self.assertEqual(r3, 80.0)

    def test_wholetable_partialnulls(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = None
            l2[i] = np.NaN
            l3[i] = np.NaN
        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r = deduplication(df=df)[0]
        self.assertEqual(r, 80.0)

    def test_singlecolumns_partialnullspartialdistinct(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = None
            l2[i] = np.NaN
            l3[i] = np.NaN
        for i in range(20, 40):
            l1[i] = "zzzzz"
            l2[i] = 500
            l3[i] = 402.2

        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 61.0)
        self.assertEqual(r2, 61.0)
        self.assertEqual(r3, 61.0)

    def test_wholetable_partialnullspartialdistinct(self):
        df = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = None
        for i in range(20, 40):
            l2[i] = np.NaN
        for i in range(40, 60):
            l3[i] = np.NaN
        for i in range(60, 80):
            l1[i] = "zzz"
            l2[i] = 133
            l3[i] = 231.22

        df["c1"] = l1
        df["c2"] = l2
        df["c3"] = l3

        r = deduplication(df=df)[0]
        self.assertEqual(r, 21.0)
