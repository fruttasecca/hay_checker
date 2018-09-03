import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import entropy

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestEntropy(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestEntropy, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("entropy_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        r1 = entropy(0, df)[0]
        self.assertEqual(r1, 0.)

    def test_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [i / 0.7 for i in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))
        df = df.withColumn("c3", replace_every_float_with_null(df["c3"]))

        r = entropy(0, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(1, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(2, df)[0]
        self.assertEqual(r, 0.)

    def test_allequal(self):
        data = pd.DataFrame()
        data["c1"] = [chr(0) for _ in range(100)]
        data["c2"] = [1 for _ in range(100)]
        data["c3"] = [0.7 for _ in range(100)]
        df = self.spark.createDataFrame(data)

        r = entropy(0, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(1, df)[0]
        self.assertEqual(r, 0.)
        r = entropy(2, df)[0]
        self.assertEqual(r, 0.)

    def test_halfnull_halfequal(self):
        data = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [0.7 for _ in range(50)]
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

        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, 0., delta=0.000001)

    def test_halfhalf(self):
        data = pd.DataFrame()
        c1 = [chr(1) for _ in range(50)]
        c2 = [2 for _ in range(50)]
        c3 = [0.7 for _ in range(50)]
        c1.extend(["zz" for _ in range(50)])
        c2.extend([100 for _ in range(50)])
        c3.extend([32. for _ in range(50)])
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)

        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, 1., delta=0.000001)

    def test_alldifferent(self):
        data = pd.DataFrame()
        c1 = [chr(i) for i in range(100)]
        c2 = [i for i in range(100)]
        c3 = [i / 0.7 for i in range(100)]
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)

        res = 6.6438561897747395
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

        for i in range(10):
            c1[i] = ""
            c2[i] = 0
            c3[i] = 0.
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        res = 6.491853096329675
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

    def test_mixed(self):
        data = pd.DataFrame()
        c1 = [chr(i) for i in range(10)]
        c2 = [i for i in range(1, 11)]
        c3 = [i / 0.7 for i in range(1, 11)]
        c1.extend(["swwewww" for _ in range(20)])
        c2.extend([5000 for _ in range(20)])
        c3.extend([231321.23131 for _ in range(20)])
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)

        res = 2.025605199016944
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)

        c1.extend(["" for _ in range(5)])
        c2.extend([0 for _ in range(5)])
        c3.extend([0. for _ in range(5)])
        data = pd.DataFrame()
        data["c1"] = c1
        data["c2"] = c2
        data["c3"] = c3
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        res = 2.025605199016944
        r = entropy(0, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(1, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
        r = entropy(2, df)[0]
        self.assertAlmostEqual(r, res, delta=0.000001)
