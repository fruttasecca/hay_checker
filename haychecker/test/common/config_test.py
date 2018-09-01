#!/usr/bin/python3
import unittest
from os.path import expanduser

from haychecker._common.config import Config

home = expanduser("~")
"""
Required arguments (table, inferSchema, output, metrics) have no default value, optional arguments
(delimiter, header, verbose) have default values (',', True, False).
"""


class TestConfig(unittest.TestCase):

    def test_input_type(self):
        with self.assertRaises(SystemExit) as cm:
            Config(4)

    def test_required_arguments(self):
        # missing table
        j1 = {
            "inferSchema": True,
            "delimiter": "|",
            "header": True,
            "output": home + "/output.json",
            "verbose": True,
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }

        # missing inferschema
        j2 = {
            "table": "tablePath",
            "delimiter": "|",
            "header": True,
            "output": home + "/output.json",
            "verbose": True,
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }

        # missing output
        j3 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "|",
            "header": True,
            "verbose": True,
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }

        # missing metrics
        j4 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "|",
            "header": True,
            "output": home + "/output.json",
            "verbose": True,
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j1)

        with self.assertRaises(AssertionError) as cm:
            j1["table"] = 10
            Config(j1)

        with self.assertRaises(AssertionError) as cm:
            Config(j2)

        with self.assertRaises(AssertionError) as cm:
            j2["inferSchema"] = "yes"
            Config(j2)

        with self.assertRaises(AssertionError) as cm:
            Config(j3)

        with self.assertRaises(AssertionError) as cm:
            j3["output"] = True
            Config(j3)

        with self.assertRaises(AssertionError) as cm:
            Config(j4)

        with self.assertRaises(AssertionError) as cm:
            j4["metrics"] = "ss"
            Config(j4)

        with self.assertRaises(AssertionError) as cm:
            j4["metrics"] = []
            Config(j4)

    def test_optional_arguments1(self):
        # no optional arguments
        j5 = {
            "table": "tablePath",
            "inferSchema": True,
            "output": home + "/output.json",
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }
        # test default args are set as defaults
        c = Config(j5)
        self.assertEqual(c["delimiter"], ",")
        self.assertEqual(c["header"], True)
        self.assertEqual(c["verbose"], False)

    def test_optional_arguments2(self):
        # set optional arguments
        j6 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": True,
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }
        # test default args are set as we wanted
        c = Config(j6)
        self.assertEqual(c["delimiter"], "#")
        self.assertEqual(c["header"], False)
        self.assertEqual(c["verbose"], True)

        with self.assertRaises(AssertionError) as cm:
            j6["delimiter"] = True
            Config(j6)

        with self.assertRaises(AssertionError) as cm:
            j6["delimiter"] = "#"
            j6["header"] = "header"
            Config(j6)

        with self.assertRaises(AssertionError) as cm:
            j6["header"] = False
            j6["threads"] = "shouldnt be here"
            Config(j6)

        with self.assertRaises(AssertionError) as cm:
            j6["verbose"] = 1
            Config(j6)

    def test_getter(self):
        j7 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": True,
            "metrics": [
                {
                    "metric": "completeness"
                },
            ]
        }
        c = Config(j7)

        # test no assignment
        with self.assertRaises(TypeError) as cm:
            c["table"] = "new"

        # test is copy
        metrics = c["metrics"]
        metrics[0]["metric"] = "ayy"
        self.assertEqual(c["metrics"][0]["metric"], "completeness")

    def test_completeness_check(self):
        j8 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "completeness",
                    "columns": []
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j8)

        j8["metrics"][0]["metric"] = 10
        del j8["metrics"][0]["columns"]
        with self.assertRaises(AssertionError) as cm:
            Config(j8)

        j8["metrics"][0]["metric"] = "completeness"
        j8["metrics"][0]["useless param"] = 1010
        with self.assertRaises(AssertionError) as cm:
            Config(j8)

        j8["metrics"][0]["columns"] = ["c0"]
        # note that at this point "useless param" is still in there
        with self.assertRaises(AssertionError) as cm:
            Config(j8)

        del j8["metrics"][0]["useless param"]
        # should run
        Config(j8)

    def test_deduplication_check(self):
        j9 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "deduplication",
                    "columns": []
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j9)

        j9["metrics"][0]["metric"] = 10
        del j9["metrics"][0]["columns"]
        with self.assertRaises(AssertionError) as cm:
            Config(j9)

        j9["metrics"][0]["metric"] = "deduplication"
        j9["metrics"][0]["useless param"] = 1010
        with self.assertRaises(AssertionError) as cm:
            Config(j9)

        j9["metrics"][0]["columns"] = ["c0"]
        # note that at this point "useless param" is still in there
        with self.assertRaises(AssertionError) as cm:
            Config(j9)

        del j9["metrics"][0]["useless param"]
        # should run
        Config(j9)

    def test_freshness_check(self):
        j10 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "freshness",
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        j10["metrics"][0]["metric"] = 10
        j10["metrics"][0]["columns"] = ["1"]
        j10["metrics"][0]["timeFormat"] = "ss"
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = [list("true")]
        j10["metrics"][0]["timeFormat"] = "ss"
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        del j10["metrics"][0]["timeFormat"]
        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = ["c2"]
        j10["metrics"][0]["we"] = "ss"
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        del j10["metrics"][0]["we"]
        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = ["c2"]
        j10["metrics"][0]["timeFormat"] = True
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        del j10["metrics"][0]["timeFormat"]
        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = ["c2"]
        j10["metrics"][0]["dateFormat"] = list()
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        j10["metrics"][0]["metric"] = "freshness"
        j10["metrics"][0]["columns"] = [4.2]
        j10["metrics"][0]["dateFormat"] = "ss:hh:mm"
        with self.assertRaises(AssertionError) as cm:
            Config(j10)

        # should run
        j10["metrics"][0]["columns"] = ["s"]
        Config(j10)

    def test_timeliness_check(self):
        j11 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "timeliness",
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["metric"] = 10
        j11["metrics"][0]["columns"] = ["1"]
        j11["metrics"][0]["timeFormat"] = "ss"
        j11["metrics"][0]["value"] = "44"
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["metric"] = "timeliness"
        j11["metrics"][0]["columns"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["columns"] = ["c4", "c2"]
        j11["metrics"][0]["timeFormat"] = list()
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        del j11["metrics"][0]["timeFormat"]
        j11["metrics"][0]["dateFormat"] = list()
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["z<<"] = ["ss"]
        del j11["metrics"][0]["dateFormat"]
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        del j11["metrics"][0]["z<<"]
        j11["metrics"][0]["value"] = 44
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["timeFormat"] = "ss:hh:mm"
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["timeFormat"] = ""
        j11["metrics"][0]["value"] = ""
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        j11["metrics"][0]["timeFormat"] = ":::"
        j11["metrics"][0]["value"] = ":::"
        with self.assertRaises(AssertionError) as cm:
            Config(j11)

        # should run
        j11["metrics"][0]["timeFormat"] = "hhh"
        j11["metrics"][0]["value"] = "555"
        Config(j11)

    def test_rule_check(self):
        j12 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "rule",
                },
            ]
        }
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        j12["metrics"][0]["metric"] = 10
        j12["metrics"][0]["conditions"] = dict()
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        j12["metrics"][0]["metric"] = "rule"
        j12["metrics"][0]["conditions"] = "aaa"
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        j12["metrics"][0]["conditions"] = list()
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = dict()
        cond2 = dict()
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"s": 1, "z": 2, "x": 4}
        cond2 = {"s": 1, "z": 2, "x": 4}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"column": None, "operator": "gt", "value": 4}
        cond2 = {"column": 1, "operator": "gt", "value": 4}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "vgt", "value": 4}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"column": "we", "operator": "gt", "value": "s"}
        cond2 = {"column": "c1", "operator": "gt", "value": 4}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": True}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j12)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}
        j12["metrics"][0]["conditions"] = [cond1, cond2]
        # should run
        Config(j12)

    def test_group_rule_check(self):
        j13 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "groupRule",
                },
            ]
        }
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        j13["metrics"][0]["metric"] = "groupRule"
        j13["metrics"][0]["having"] = dict()
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        del j13["metrics"][0]["having"]
        j13["metrics"][0]["columns"] = ["s"]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        j13["metrics"][0]["metric"] = "groupRule"
        j13["metrics"][0]["columns"] = ["s"]
        j13["metrics"][0]["having"] = ["s"]
        j13["metrics"][0]["z"] = dict()
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        del j13["metrics"][0]["z"]
        j13["metrics"][0]["conditions"] = dict()
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}

        having = {"operator": "zz", "value": 5, "aggregator": "min", "column": "c2"}
        j13["metrics"][0]["having"] = [having]
        j13["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        having = {"operator": "gt", "value": 2, "aggregator": "dmin", "column": "c2"}
        j13["metrics"][0]["having"] = [having]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        having = {"operator": "gt", "value": 2, "aggregator": "dmin", "column": "c2"}
        j13["metrics"][0]["having"] = [having]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        having = {"operator": "gt", "value": 2, "aggregator": "max", "column": "c2"}
        j13["metrics"][0]["having"] = [having]
        cond2 = {"column": ["c11"], "operator": "eq", "value": "jhon"}
        j13["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        cond2 = {"column": "c11", "operator": "==", "value": "jhon"}
        j13["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        cond2 = {"column": "c11", "operator": "lt", "value": "0.01"}
        j13["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j13)

        cond2 = {"column": "c11", "operator": "eq", "value": 10}
        j13["metrics"][0]["conditions"] = [cond1, cond2]
        # should run
        Config(j13)

    def test_constraint_check(self):
        j14 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "constraint",
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c2"]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        del j14["metrics"][0]["when"]
        j14["metrics"][0]["then"] = ["c2"]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c2"]
        j14["metrics"][0]["then"] = ["c2"]
        j14["metrics"][0]["ssss"] = ["c2"]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = "s2"
        j14["metrics"][0]["then"] = ["c2"]
        j14["metrics"][0]["ssss"] = ["c2"]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        del j14["metrics"][0]["ssss"]
        j14["metrics"][0]["when"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1"]
        j14["metrics"][0]["then"] = True
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1"]
        j14["metrics"][0]["then"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1", None]
        j14["metrics"][0]["then"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1", None]
        j14["metrics"][0]["then"] = []
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1"]
        j14["metrics"][0]["then"] = ["c2", "c1"]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        j14["metrics"][0]["when"] = ["c1"]
        j14["metrics"][0]["then"] = ["c2", "c4"]
        cond1 = {"s": 1, "z": 2, "x": 4}
        cond2 = {"s": 1, "z": 2, "x": 4}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        cond1 = {"column": None, "operator": "gt", "value": 4}
        cond2 = {"column": 1, "operator": "gt", "value": 4}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "vgt", "value": 4}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        cond1 = {"column": "we", "operator": "gt", "value": "s"}
        cond2 = {"column": "c1", "operator": "gt", "value": 4}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        print("###########")
        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": True}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        with self.assertRaises(AssertionError) as cm:
            Config(j14)

        cond1 = {"column": "we", "operator": "gt", "value": 4}
        cond2 = {"column": "c1", "operator": "eq", "value": "jhon"}
        j14["metrics"][0]["conditions"] = [cond1, cond2]
        # should run
        Config(j14)

    def test_deduplication_aproximated_check(self):
        j15 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "deduplication_approximated",
                    "columns": []
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j15)

        j15["metrics"][0]["metric"] = 10
        del j15["metrics"][0]["columns"]
        with self.assertRaises(AssertionError) as cm:
            Config(j15)

        j15["metrics"][0]["metric"] = "deduplication_approximated"
        j15["metrics"][0]["useless param"] = 1010
        with self.assertRaises(AssertionError) as cm:
            Config(j15)

        j15["metrics"][0]["columns"] = ["c0"]
        # note that at this point "useless param" is still in there
        with self.assertRaises(AssertionError) as cm:
            Config(j15)

        del j15["metrics"][0]["useless param"]
        # should run
        Config(j15)

    def test_entropy_check(self):
        j16 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "entropy",
                    "column": []
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j16)

        j16["metrics"][0]["metric"] = 10
        j16["metrics"][0]["column"] = "c1"
        with self.assertRaises(AssertionError) as cm:
            Config(j16)

        j16["metrics"][0]["metric"] = "entropy"
        j16["metrics"][0]["useless param"] = 1010
        with self.assertRaises(AssertionError) as cm:
            Config(j16)

        del j16["metrics"][0]["column"]
        with self.assertRaises(AssertionError) as cm:
            Config(j16)

        del j16["metrics"][0]["useless param"]
        with self.assertRaises(AssertionError) as cm:
            Config(j16)

        j16["metrics"][0]["column"] = "c1"
        # should run
        Config(j16)

    def test_mutual_info_check(self):
        j17 = {
            "table": "tablePath",
            "inferSchema": True,
            "delimiter": "#",
            "header": False,
            "output": home + "/output.json",
            "verbose": False,
            "metrics": [
                {
                    "metric": "mutual",
                    "when": "c1",
                    "then": 2
                },
            ]
        }

        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["metric"] = "mutual_info"
        j17["metrics"][0]["when"] = ["c1"]
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["when"] = 1
        j17["metrics"][0]["then"] = [0]
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["then"] = 0
        j17["metrics"][0]["useless param"] = 1010
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["then"] = 1
        del j17["metrics"][0]["useless param"]
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        del j17["metrics"][0]["then"]
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        del j17["metrics"][0]["when"]
        j17["metrics"][0]["then"] = 1
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["then"] = 1
        with self.assertRaises(AssertionError) as cm:
            Config(j17)

        j17["metrics"][0]["when"] = 2
        Config(j17)
