import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import constraint

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestConstraint(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestConstraint, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("constraint_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0], [1], conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [" " for _ in range(100)]
        data["c2"] = [1 for _ in range(100)]
        data["c3"] = [1 for _ in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 100.0)

    def test_allnull_with_conditions(self):
        data = pd.DataFrame()
        data["c1"] = [" " for _ in range(100)]
        data["c2"] = [1 for _ in range(100)]
        data["c3"] = [1 for _ in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))

        condition1 = {"column": "c1", "operator": "lt", "value": 1000}
        condition2 = {"column": "c1", "operator": "gt", "value": 0}
        conditions = [condition1, condition2]
        r = constraint([0, 1], [2], conditions, df)[0]
        self.assertEqual(r, 100.0)

    def test_halfnull_halfequal_respected(self):
        data = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(50)]
        c1.extend(["" for _ in range(50)])
        c2.extend([0 for _ in range(50)])
        c3.extend([0. for _ in range(50)])
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))

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
        data = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(50)]
        c1.extend(["" for _ in range(50)])
        c2.extend([0 for _ in range(50)])
        c3.extend([0. for _ in range(40)])
        c3.extend([10. for _ in range(10)])
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))

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
        data = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [2 / 0.6 for _ in range(40)]
        c3.extend([9 for _ in range(10)])

        c1.extend(["" for _ in range(50)])
        c2.extend([0 for _ in range(50)])
        c3.extend([0. for _ in range(50)])
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        r = constraint([0, 1], [2], df=df)[0]
        self.assertEqual(r, 0.0)

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
        data = pd.DataFrame()
        c1 = ["", "2", "2", "3", "3", "3", ""]
        c2 = [0, 2, 2, 3, 3, 0, 3]
        c3 = [24., 4, 4, 4, 2, 24, 2]
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

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
