import random
import unittest

import numpy as np
import pandas as pd
from sklearn.metrics import mutual_info_score

from haychecker.chc.metrics import mutual_info


class TestMutualInfo(unittest.TestCase):
    def mi(self, df, x, y):
        index = (df[x].isna()) | (df[y].isna())
        index = ~index
        if sum(index) > 0:
            return mutual_info_score(df[x][index], df[y][index])
        else:
            return 0

    def test_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        r1 = mutual_info(0, 1, df)[0]
        self.assertEqual(r1, 0.)

    def test_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for i in range(100)]
        df["c2"] = [np.NaN for i in range(100)]

        pmi = self.mi(df, "c1", "c2")
        r = mutual_info(0, 1, df)[0]
        self.assertEqual(r, pmi)
        r = mutual_info(1, 0, df)[0]
        self.assertEqual(r, pmi)

    def test_allequal(self):
        df = pd.DataFrame()
        df["c1"] = [chr(0) for _ in range(100)]
        df["c2"] = [1 for _ in range(100)]

        pmi = self.mi(df, "c1", "c2")
        r = mutual_info(0, 1, df)[0]
        self.assertEqual(r, pmi)
        r = mutual_info(1, 0, df)[0]
        self.assertEqual(r, pmi)

    def test_halfnull_halfequal(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c1.extend([None for _ in range(50)])
        c2.extend([np.NaN for _ in range(50)])
        df["c1"] = c1
        df["c2"] = c2

        pmi = self.mi(df, "c1", "c2")
        r = mutual_info(0, 1, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)
        r = mutual_info(1, 0, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)

    def test_halfhalf(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [0.7 for _ in range(50)]
        c1.extend(["zz" for _ in range(50)])
        c2.extend([100 for _ in range(50)])
        c3.extend([32. for _ in range(50)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        pmi = self.mi(df, "c1", "c2")
        r = mutual_info(0, 1, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)
        r = mutual_info(1, 0, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)

        pmi = self.mi(df, "c1", "c3")
        r = mutual_info(0, 2, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)
        r = mutual_info(2, 0, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)

        pmi = self.mi(df, "c2", "c3")
        r = mutual_info(1, 2, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)
        r = mutual_info(2, 1, df)[0]
        self.assertAlmostEqual(r, pmi, delta=0.000001)

    def test_halfhalf_shuffled(self):
        for _ in range(2):
            df = pd.DataFrame()
            c1 = [chr(1) for _ in range(50)]
            c2 = [2 for _ in range(50)]
            c3 = [0.7 for _ in range(50)]
            c1.extend(["zz" for _ in range(50)])
            c2.extend([100 for _ in range(50)])
            c3.extend([32. for _ in range(50)])
            random.shuffle(c1)
            random.shuffle(c2)
            random.shuffle(c3)
            df["c1"] = c1
            df["c2"] = c2
            df["c3"] = c3

            pmi = self.mi(df, "c1", "c2")
            r = mutual_info(0, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(1, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c1", "c3")
            r = mutual_info(0, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c2", "c3")
            r = mutual_info(1, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

    def test_halfhalf_shuffled_withnull(self):
        for _ in range(2):
            df = pd.DataFrame()
            c1 = [chr(1) for _ in range(50)]
            c2 = [2 for _ in range(50)]
            c3 = [0.7 for _ in range(50)]
            c1.extend([None for _ in range(50)])
            c2.extend([np.NaN for _ in range(50)])
            c3.extend([None for _ in range(50)])
            random.shuffle(c1)
            random.shuffle(c2)
            random.shuffle(c3)
            df["c1"] = c1
            df["c2"] = c2
            df["c3"] = c3

            pmi = self.mi(df, "c1", "c2")
            r = mutual_info(0, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(1, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c1", "c3")
            r = mutual_info(0, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c2", "c3")
            r = mutual_info(1, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

    def test_mixed_shuffled_with_null(self):
        for _ in range(2):
            df = pd.DataFrame()
            c1 = [chr(i) for i in range(50)]
            c2 = [i for i in range(1, 51)]
            c3 = [i / 0.7 for i in range(1, 51)]
            c1.extend([None for _ in range(50)])
            c2.extend([np.NaN for _ in range(50)])
            c3.extend([None for _ in range(50)])
            random.shuffle(c1)
            random.shuffle(c2)
            random.shuffle(c3)
            df["c1"] = c1
            df["c2"] = c2
            df["c3"] = c3

            pmi = self.mi(df, "c1", "c2")
            r = mutual_info(0, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(1, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c1", "c3")
            r = mutual_info(0, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 0, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)

            pmi = self.mi(df, "c2", "c3")
            r = mutual_info(1, 2, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
            r = mutual_info(2, 1, df)[0]
            self.assertAlmostEqual(r, pmi, delta=0.000001)
