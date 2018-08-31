import unittest

import numpy as np
import pandas as pd

from haychecker.chc.metrics import entropy


class TestEntropy(unittest.TestCase):
    def test_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        r1 = entropy(0, df)[0]
        self.assertEqual(r1, 0.)

    def test_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for i in range(100)]
        df["c2"] = [np.NaN for i in range(100)]
        df["c3"] = [np.NaN / 0.7 for i in range(100)]

        r = entropy(0, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(1, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(2, df)[0]
        self.assertEqual(r, 0.)

    def test_allequal(self):
        df = pd.DataFrame()
        df["c1"] = [chr(0) for _ in range(100)]
        df["c2"] = [1 for _ in range(100)]
        df["c3"] = [0.7 for _ in range(100)]

        r = entropy(0, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(1, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(2, df)[0]
        self.assertEqual(r, 0.)

    def test_halfnull_halfequal(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [0.7 for _ in range(50)]
        c1.extend([None for _ in range(50)])
        c2.extend([np.NaN for _ in range(50)])
        c3.extend([None for _ in range(50)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)

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

        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)

    def test_alldifferent(self):
        df = pd.DataFrame()
        c1 = [chr(i) for i in range(100)]
        c2 = [i for i in range(100)]
        c3 = [i / 0.7 for i in range(100)]
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        res = 6.6438561897747395
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

        for i in range(10):
            c1[i] = None
            c2[i] = None
            c3[i] = np.NaN
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        res = 6.491853096329675
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

    def test_mixed(self):
        df = pd.DataFrame()
        c1 = [chr(i) for i in range(10)]
        c2 = [i for i in range(1, 11)]
        c3 = [i / 0.7 for i in range(1, 11)]
        c1.extend(["swwewww" for _ in range(20)])
        c2.extend([5000 for _ in range(20)])
        c3.extend([231321.23131 for _ in range(20)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        res = 2.025605199016944
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

        c1.extend([None for _ in range(5)])
        c2.extend([np.NaN for _ in range(5)])
        c3.extend([np.NaN for _ in range(5)])
        df = pd.DataFrame()
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        res = 2.025605199016944
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
