import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import rule

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestRule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestRule, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("rule_test").getOrCreate()
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
        r = rule(conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_operator_eq(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [float(i) for i in range(100)]
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(50)}
        condition2 = {"column": "c1", "operator": "eq", "value": chr(50)}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(50)}
        condition2 = {"column": "c2", "operator": "eq", "value": 50}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(50)}
        condition2 = {"column": "c2", "operator": "eq", "value": 50}
        condition3 = {"column": "c3", "operator": "eq", "value": 50.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(50)}
        condition2 = {"column": "c2", "operator": "eq", "value": 50}
        condition3 = {"column": "c3", "operator": "eq", "value": 51.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

    def test_operator_gt(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [float(i) for i in range(100)]
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c2", "operator": "gt", "value": 0}
        conditions = [condition1]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 99.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": 49}
        condition3 = {"column": "c3", "operator": "gt", "value": 0.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 50.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": 49}
        condition3 = {"column": "c3", "operator": "gt", "value": 89.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 10.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": 49}
        condition3 = {"column": "c3", "operator": "gt", "value": 99.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": -1}
        condition3 = {"column": "c3", "operator": "gt", "value": -1.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_operator_lt(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [float(i) for i in range(100)]
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c2", "operator": "lt", "value": 0}
        conditions = [condition1]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c2", "operator": "lt", "value": 50}
        condition2 = {"column": "c2", "operator": "lt", "value": 49}
        condition3 = {"column": "c3", "operator": "lt", "value": 100.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 49.)

        condition1 = {"column": "c2", "operator": "lt", "value": 500}
        condition2 = {"column": "c2", "operator": "lt", "value": 10}
        condition3 = {"column": "c3", "operator": "lt", "value": 10.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 10.)

        condition1 = {"column": "c2", "operator": "lt", "value": 21}
        condition2 = {"column": "c2", "operator": "lt", "value": 22}
        condition3 = {"column": "c3", "operator": "lt", "value": 0.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c2", "operator": "lt", "value": 100}
        condition3 = {"column": "c3", "operator": "lt", "value": 100.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 100.)

    def test_operators_mixed(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [float(i) for i in range(100)]
        df = self.spark.createDataFrame(data)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(50)}
        condition2 = {"column": "c2", "operator": "lt", "value": 100.}
        condition3 = {"column": "c3", "operator": "lt", "value": 100.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c1", "operator": "eq", "value": chr(100)}
        condition2 = {"column": "c2", "operator": "lt", "value": 10}
        condition3 = {"column": "c3", "operator": "lt", "value": 10.}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

        condition2 = {"column": "c2", "operator": "gt", "value": 22}
        condition3 = {"column": "c3", "operator": "lt", "value": 31.}
        conditions = [condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 8.)

        condition1 = {"column": "c2", "operator": "gt", "value": 10}
        condition2 = {"column": "c2", "operator": "lt", "value": 100}
        condition3 = {"column": "c2", "operator": "gt", "value": 0}
        condition4 = {"column": "c2", "operator": "lt", "value": 50}
        condition5 = {"column": "c2", "operator": "lt", "value": 40}
        condition6 = {"column": "c3", "operator": "gt", "value": 20.}
        condition7 = {"column": "c3", "operator": "lt", "value": 100}
        condition8 = {"column": "c3", "operator": "gt", "value": 0}
        condition9 = {"column": "c3", "operator": "lt", "value": 25}
        condition10 = {"column": "c3", "operator": "lt", "value": 23}
        conditions = [condition1, condition2, condition3, condition4, condition5, condition6, condition7, condition8,
                      condition9, condition10]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 2.)

        condition1 = {"column": "c2", "operator": "gt", "value": 10}
        condition2 = {"column": "c2", "operator": "lt", "value": 100}
        condition3 = {"column": "c2", "operator": "gt", "value": 0}
        condition4 = {"column": "c2", "operator": "lt", "value": 50}
        condition5 = {"column": "c2", "operator": "lt", "value": 40}
        condition6 = {"column": "c3", "operator": "gt", "value": 20.}
        condition7 = {"column": "c3", "operator": "lt", "value": 100}
        condition8 = {"column": "c3", "operator": "eq", "value": -1}
        condition9 = {"column": "c3", "operator": "lt", "value": 25}
        condition10 = {"column": "c3", "operator": "lt", "value": 23}
        conditions = [condition1, condition2, condition3, condition4, condition5, condition6, condition7, condition8,
                      condition9, condition10]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

    def test_operators_mixed_and_nulls_notinconditions(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        c3 = [float(i) for i in range(100)]
        for i in range(10):
            c3[i] = 0
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c3", replace_0_with_null(df["c3"]))

        condition1 = {"column": "c1", "operator": "eq", "value": chr(0)}
        conditions = [condition1]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c2", "operator": "eq", "value": 0}
        conditions = [condition1]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": -1}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 100.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": 50}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 49.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c2", "operator": "gt", "value": 50}
        condition3 = {"column": "c2", "operator": "gt", "value": 20}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 49.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c2", "operator": "gt", "value": 50}
        condition3 = {"column": "c2", "operator": "lt", "value": 20}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)

    def test_operators_mixed_and_nulls_inconditions(self):
        data = pd.DataFrame()
        c1 = [chr(i) for i in range(100)]
        c2 = [i for i in range(100)]
        c3 = [float(i) for i in range(100)]
        for i in range(10):
            c1[i] = ""
            c2[i] = 0
            c3[i] = 0
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        condition1 = {"column": "c3", "operator": "eq", "value": 10.0}
        conditions = [condition1]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 1.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": -1}
        condition3 = {"column": "c3", "operator": "gt", "value": -1}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 90.)

        condition1 = {"column": "c2", "operator": "gt", "value": -1}
        condition2 = {"column": "c2", "operator": "gt", "value": 50}
        condition3 = {"column": "c3", "operator": "gt", "value": 50}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 49.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c2", "operator": "gt", "value": 50}
        condition3 = {"column": "c3", "operator": "gt", "value": 20}
        conditions = [condition1, condition2, condition3]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 49.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c3", "operator": "lt", "value": 20}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 10.)

        condition1 = {"column": "c2", "operator": "lt", "value": 100}
        condition2 = {"column": "c3", "operator": "lt", "value": 10}
        conditions = [condition1, condition2]
        r = rule(conditions, df)[0]
        self.assertEqual(r, 0.)
