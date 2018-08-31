import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import grouprule

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestGroupRule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGroupRule, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("grouprule_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        having1 = {"column": "*", "operator": "gt", "value": 0, "aggregator": "count"}
        conditions = [condition1, condition2]
        havings = [having1]
        r = grouprule([0, 1], havings, conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [" " for _ in range(100)]
        data["c2"] = [1 for _ in range(100)]
        data["c3"] = [1 for _ in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))

        having1 = {"column": "*", "operator": "gt", "value": 0, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0, 1], havings, df=df)[0]
        self.assertEqual(r, 100.)

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 0, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0, 1], havings, conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_groping_single_column(self):
        data = pd.DataFrame()
        c1 = ["", "2", "2", "3", "3", "3", ""]
        c2 = [0, 2, 2, 3, 3, 0, 3]
        c3 = [24., 4, 4, 4, 2, 24, 2]
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c2", "operator": "lt", "value": 1000}
        condition2 = {"column": "c2", "operator": "gt", "value": -1}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 2, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0], havings, conditions, df)[0]
        self.assertEqual(r, (1 / 3.) * 100)

        condition1 = {"column": "c2", "operator": "lt", "value": 1000}
        condition2 = {"column": "c2", "operator": "gt", "value": -1}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 3, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0], havings, conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c1", "operator": "eq", "value": "3"}
        condition2 = {"column": "c1", "operator": "eq", "value": "3"}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 2, "aggregator": "count"}
        having2 = {"column": "c2", "operator": "eq", "value": 0, "aggregator": "min"}
        having3 = {"column": "c3", "operator": "gt", "value": 20, "aggregator": "max"}
        havings = [having1, having2, having3]
        r = grouprule([0], havings, conditions, df)[0]
        self.assertEqual(r, 100.0)

        having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
        having2 = {"column": "c2", "operator": "lt", "value": 3, "aggregator": "min"}
        having3 = {"column": "c3", "operator": "gt", "value": 20, "aggregator": "max"}
        havings = [having1, having2, having3]
        r = grouprule([0], havings, df=df)[0]
        self.assertEqual(r, (2 / 3) * 100)

        condition1 = {"column": "c3", "operator": "gt", "value": 2}
        condition2 = {"column": "c2", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0], havings, conditions, df)[0]
        self.assertEqual(r, 50.)

    def test_groping_multile_columns(self):
        data = pd.DataFrame()
        c1 = [0, 0, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5]
        c2 = ["a", "a", "b", "b", "c", "c", "d", "d", "d", "d", "a", "a", "a"]
        c3 = [0.0, 0.0, 0.1, 0.1, 2.2, 2.2, 2.2, 3.1, 3.2, 3.3, 40, 40, 50]
        c4 = [10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0, 20.0, 10.0]
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        data["c4"] = c4
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c3", "operator": "lt", "value": 50}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
        having2 = {"column": "c4", "operator": "eq", "value": 50 / 3, "aggregator": "avg"}
        havings = [having1, having2]
        r = grouprule([0, "c2"], havings, conditions, df)[0]
        self.assertEqual(r, 25.)

        condition1 = {"column": "c3", "operator": "lt", "value": 50}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        having1 = {"column": "c4", "operator": "gt", "value": 25.0, "aggregator": "sum"}
        havings = [having1]
        r = grouprule([0, "c2"], havings, conditions, df)[0]
        self.assertEqual(r, 75.0)

        condition1 = {"column": "c3", "operator": "lt", "value": 50}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        having1 = {"column": "c4", "operator": "eq", "value": 10.0, "aggregator": "sum"}
        havings = [having1]
        r = grouprule([0, "c2"], havings, conditions, df)[0]
        self.assertEqual(r, 25.0)

        condition1 = {"column": "c2", "operator": "eq", "value": "d"}
        condition2 = {"column": "c3", "operator": "gt", "value": 1.0}
        conditions = [condition1, condition2]
        having1 = {"column": "c3", "operator": "gt", "value": 3.0, "aggregator": "avg"}
        havings = [having1]
        r = grouprule([0, "c2"], havings, conditions, df)[0]
        self.assertEqual(r, 50.0)

        condition1 = {"column": "c4", "operator": "eq", "value": 10.0}
        condition2 = {"column": "c3", "operator": "lt", "value": 3.0}
        conditions = [condition1, condition2]
        having1 = {"column": "c1", "operator": "gt", "value": 1.0, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0, "c2", 3], havings, conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c4", "operator": "eq", "value": 10.0}
        condition2 = {"column": "c3", "operator": "lt", "value": 3.0}
        conditions = [condition1, condition2]
        having1 = {"column": "c1", "operator": "gt", "value": 1.0, "aggregator": "count"}
        havings = [having1]
        r = grouprule([0, 3], havings, conditions, df)[0]
        self.assertEqual(r, (1 / 3) * 100)
