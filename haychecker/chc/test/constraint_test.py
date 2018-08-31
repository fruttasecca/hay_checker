import unittest

import numpy as np
import pandas as pd

from haychecker.chc.metrics import constraint


class TestConstraint(unittest.TestCase):
    def test_empty(self):
        df = pd.DataFrame()
        df["c1"] = []
        df["c2"] = []

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0], [1], conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_allnull(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = [np.NaN for _ in range(100)]
        df["c3"] = [None for _ in range(100)]

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 100.0)

    def test_allnull_with_conditions(self):
        df = pd.DataFrame()
        df["c1"] = [None for _ in range(100)]
        df["c2"] = [None for _ in range(100)]
        df["c3"] = [np.NaN for _ in range(100)]

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.0)

    def test_halfnull_halfequal_respected(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(50)]
        c1.extend([None for _ in range(50)])
        c2.extend([np.NaN for _ in range(50)])
        c3.extend([None for _ in range(50)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 100.0)

        condition1 = {"column": "c2", "operator": "lt", "value": 2}
        condition2 = {"column": "c2", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.0)
        r = constraint([0, 2], [1], conditions, df)[0]
        self.assertEqual(r, 100.0)

    def test_halfnull_halfequal_notrespected1(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(50)]
        c1.extend([None for _ in range(50)])
        c2.extend([None for _ in range(50)])
        c3.extend([np.NaN for _ in range(40)])
        c3.extend([10. for _ in range(10)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 50.0)

        condition1 = {"column": "c2", "operator": "lt", "value": 3}
        condition2 = {"column": "c2", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.0)
        r = constraint([0, 2], [1], conditions, df)[0]
        self.assertEqual(r, 100.0)

    def test_halfnull_halfequal_notrespected2(self):
        df = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(40)]
        c3.extend(6. for _ in range(10))
        c1.extend([None for _ in range(50)])
        c2.extend([None for _ in range(50)])
        c3.extend([None for _ in range(50)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 50.0)

        condition1 = {"column": "c2", "operator": "lt", "value": 3}
        condition2 = {"column": "c2", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([2, 1], [0], conditions, df)[0]
        self.assertEqual(r, 100.0)
        r = constraint([2], [0], conditions, df)[0]
        self.assertEqual(r, 100.0)
        r = constraint([1], [0], conditions, df)[0]
        self.assertEqual(r, 100.0)

    def test_halfnull_halfequal_notrespected3(self):
        df = pd.DataFrame()
        c1 = [None, "2", "2", "3", "3", "3", None]
        c2 = [None, 2, 2, 3, 3, None, 3]
        c3 = [24., 4, 4, 4, 2, 24, 2]
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, (5 / 7.) * 100)

        condition1 = {"column": "c2", "operator": "lt", "value": 3}
        conditions = [condition1]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.)

        condition1 = {"column": "c2", "operator": "eq", "value": 3}
        conditions = [condition1]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, (1 / 3.) * 100)

        condition1 = {"column": "c2", "operator": "eq", "value": 3}
        condition2 = {"column": "c1", "operator": "eq", "value": "3"}
        conditions = [condition1, condition2]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c3", "operator": "gt", "value": 20}
        condition2 = {"column": "c2", "operator": "lt", "value": 3}
        condition3 = {"column": "c1", "operator": "eq", "value": "3"}
        conditions = [condition1, condition2, condition3]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.)

        r = constraint([0], [1, 2], df=df)[0]
        self.assertEqual(r, (2 / 7.) * 100)
