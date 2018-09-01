import unittest

import pandas as pd

from haychecker.chc.metrics import completeness, deduplication, timeliness, rule, grouprule
from haychecker.chc.task import Task


class TestTask(unittest.TestCase):
    def test_groping_multile_columns(self):
        df = pd.DataFrame()
        c1 = [0, 0, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5]
        c2 = ["a", "a", "b", "b", "c", "c", "d", "d", "d", "d", "a", "a", "a"]
        c3 = [0.0, 0.0, 0.1, 0.1, 2.2, 2.2, 2.2, 3.1, 3.2, 3.3, 40, 40, 50]
        c4 = [10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0]
        c5 = ["09:10:10" for _ in range(10)]
        c5.extend(["00:11:10" for _ in range(3)])
        df["c1"] = c1
        df["c2"] = c2
        df["c3"] = c3
        df["c4"] = c4
        df["c5"] = c5
        task = Task()

        task.add(completeness())
        task.add(completeness([0, 1, 2]))
        task.add(deduplication([0, 1]))
        task.add(deduplication())
        task.add(timeliness(["c5"], value="10:10:10", timeFormat="%S:%M:%H"))
        task.add(completeness())
        condition1 = {"column": "c3", "operator": "lt", "value": 50}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        task.add(rule(conditions))
        condition1 = {"column": "c5", "operator": "eq", "value": "00:11:10"}
        conditions = [condition1]
        task.add(rule(conditions))

        condition1 = {"column": "c3", "operator": "lt", "value": 50}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
        having2 = {"column": "c4", "operator": "eq", "value": 50 / 3, "aggregator": "avg"}
        havings = [having1, having2]
        task.add(grouprule([0, "c2"], havings, conditions))

        result = task.run(df)

        # c1
        r = result[0]["scores"][0]
        self.assertEqual(r, 100.)

        # c2
        r1, r2, r3 = result[1]["scores"]
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        # d1
        r1, r2 = result[2]["scores"]
        self.assertEqual(r1, (6 / 13) * 100)
        self.assertEqual(r2, (4 / 13) * 100)

        # d2
        r = result[3]["scores"][0]
        self.assertEqual(r, 100.)

        # t
        r = result[4]["scores"][0]
        self.assertEqual(r, (10 / 13) * 100)

        # c3
        r = result[5]["scores"][0]
        self.assertEqual(r, 100.)

        # r1
        r = result[6]["scores"][0]
        self.assertEqual(r, (8 / 13) * 100)

        # r2
        r = result[7]["scores"][0]
        self.assertEqual(r, (3 / 13) * 100)

        # gr1
        r = result[8]["scores"][0]
        self.assertEqual(r, 25.0)
