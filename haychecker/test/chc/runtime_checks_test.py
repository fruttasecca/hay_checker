import unittest

import pandas as pd

from haychecker.chc._runtime_checks import completeness_run_check, deduplication_run_check, timeliness_run_check, \
    freshness_run_check, rule_run_check, grouprule_run_check, constraint_run_check, deduplication_approximated_run_check


class TestChecks(unittest.TestCase):
    def test_completeness_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        with self.assertRaises(AssertionError) as cm:
            completeness_run_check(["c0"], df)

        # this should work
        completeness_run_check(["c1"], df)
        completeness_run_check(None, df)

    def test_deduplication_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        with self.assertRaises(AssertionError) as cm:
            deduplication_run_check(["c1", "c0"], df)

        # this should work
        deduplication_run_check(["c1"], df)
        deduplication_run_check(None, df)

    def test_deduplication_approximated_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        with self.assertRaises(AssertionError) as cm:
            deduplication_approximated_run_check(["c1", "c0"], df)

        # this should work
        deduplication_approximated_run_check(["c1"], df)
        deduplication_approximated_run_check(None, df)

    def timeliness_run_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1", "c0"], "20:10:25", df, dateFormat=None, timeFormat="HH:mm:ss")

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1"], "20:10:25", df, dateFormat="yyyy", timeFormat="HH:mm:ss")

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1"], "20:10:25", df, dateFormat=None, timeFormat=None)

        with self.assertRaises(TypeError) as cm:
            timeliness_run_check("20:10:00", df, dateFormat="xx:ww:kk")

        # these should work
        timeliness_run_check(["c1", "c2"], "20:10:00", df, dateFormat="%x:%w:%k")
        timeliness_run_check(["c1", "c2"], "20:10:00", df, timeFormat="%x:%w:%k")

    def test_freshness_run_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        with self.assertRaises(AssertionError) as cm:
            freshness_run_check(["c1", "c0"], df)

        # this should work
        freshness_run_check(["c1"], timeFormat="s", df=df)

    def test_rule_run_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}

        with self.assertRaises(AssertionError) as cm:
            rule_run_check([cond2, cond1, cond2], df)

        # this should work
        rule_run_check([cond2, cond2, cond2], df)

    def test_grouprule_run_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]
        df["c3"] = [1000, 20, 30]
        df["c4"] = ["cia", "zzz", "ayy"]

        have1 = {"column": "c4", "operator": "gt", "value": 4, "aggregator": "min"}
        have2 = {"column": "c4", "operator": "gt", "value": 4, "aggregator": "max"}
        have3 = {"column": "c0", "operator": "eq", "value": "jhon"}

        with self.assertRaises(AssertionError) as cm:
            grouprule_run_check(["cfirst"], having=[have2], conditions=None, df=df)

        with self.assertRaises(TypeError) as cm:
            grouprule_run_check(None, having=[have2], conditions=None, df=df)

        with self.assertRaises(AssertionError) as cm:
            grouprule_run_check(["c4", "c1"], having=[have1, have2, have3], conditions=None, df=df)

        with self.assertRaises(TypeError) as cm:
            grouprule_run_check(["c1"], having=None, conditions=None, df=df)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}
        with self.assertRaises(AssertionError) as cm:
            grouprule_run_check(["c1"], having=[have1], conditions=[cond1, cond2], df=df)

        # should pass
        grouprule_run_check(["c1"], having=[have1], conditions=[cond2], df=df)

    def test_constraint_check(self):
        df = pd.DataFrame()
        df["c1"] = [10, 20, 30]
        df["c2"] = [1000, 20, 30]
        df["c3"] = [1000, 20, 30]
        df["c4"] = ["cia", "zzz", "ayy"]

        with self.assertRaises(AssertionError) as cm:
            constraint_run_check(["c0"], ["c1", "c2"], None, df)

        with self.assertRaises(AssertionError) as cm:
            constraint_run_check(["c1"], ["c2", "c0"], None, df)

        with self.assertRaises(AssertionError) as cm:
            constraint_run_check(["c1"], ["c2", "c3", "c4", "c1"], None, df)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}

        with self.assertRaises(AssertionError) as cm:
            constraint_run_check(["c1"], ["c2", "c3", "c4"], [cond2, cond1, cond1], df)

        # should pass
        constraint_run_check(["c1"], ["c2", "c3", "c4"], [cond2, cond2], df)
