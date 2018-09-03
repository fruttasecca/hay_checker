import unittest

import pandas as pd
from pyspark.sql import SparkSession

from haychecker.dhc._runtime_checks import completeness_run_check, deduplication_run_check, timeliness_run_check, \
    freshness_run_check, rule_run_check, grouprule_run_check, constraint_run_check, deduplication_approximated_run_check


class TestChecks(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestChecks, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("runtime_checks_tests").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_completeness_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        with self.assertRaises(AssertionError) as cm:
            completeness_run_check(["c0"], df)

        # this should work
        completeness_run_check(["c1"], df)
        completeness_run_check(None, df)

    def test_deduplication_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        with self.assertRaises(AssertionError) as cm:
            deduplication_run_check(["c1", "c0"], df)

        # this should work
        deduplication_run_check(["c1"], df)
        deduplication_run_check(None, df)

    def test_deduplication_approximated_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        with self.assertRaises(AssertionError) as cm:
            deduplication_approximated_run_check(["c1", "c0"], df)

        # this should work
        deduplication_approximated_run_check(["c1"], df)
        deduplication_approximated_run_check(None, df)

    def timeliness_run_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1", "c0"], "20:10:25", df, dateFormat=None, timeFormat="HH:mm:ss")

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1"], "20:10:25", df, dateFormat="yyyy", timeFormat="HH:mm:ss")

        with self.assertRaises(AssertionError) as cm:
            timeliness_run_check(["c1"], "20:10:25", df, dateFormat=None, timeFormat=None)

        with self.assertRaises(TypeError) as cm:
            timeliness_run_check("20:10:00", d, dateFormat="xx:ww:kk")

        # these should work
        timeliness_run_check(["c1", "c2"], "20:10:00", d, dateFormat="xx:ww:kk")
        timeliness_run_check(["c1", "c2"], "20:10:00", d, timeFormat="xx:ww:kk")

    def test_freshness_run_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        with self.assertRaises(AssertionError) as cm:
            freshness_run_check(["c1", "c0"], df)

        # this should work
        freshness_run_check(["c1"], timeFormat="s", df=df)

    def test_rule_run_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        df = self.spark.createDataFrame(data)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}

        with self.assertRaises(AssertionError) as cm:
            rule_run_check([cond2, cond1, cond2], df)

        # this should work
        rule_run_check([cond2, cond2, cond2], df)

    def test_grouprule_run_check(self):
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        data["c3"] = [1000, 20, 30]
        data["c4"] = ["cia", "zzz", "ayy"]
        df = self.spark.createDataFrame(data)

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
        data = pd.DataFrame()
        data["c1"] = [10, 20, 30]
        data["c2"] = [1000, 20, 30]
        data["c3"] = [1000, 20, 30]
        data["c4"] = ["cia", "zzz", "ayy"]
        df = self.spark.createDataFrame(data)

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
