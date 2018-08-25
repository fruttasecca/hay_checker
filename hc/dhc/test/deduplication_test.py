import random
import unittest
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType
import pandas as pd

from ..metrics import deduplication

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestDeduplication(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDeduplication, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("deduplication_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_singlecolumns_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        r1, r2 = deduplication(["c1", "c2"], df)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)

    def test_wholetable_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        r = deduplication(df=df)[0]
        self.assertEqual(r, 100.)

    def test_singlecolumns_allsame(self):
        data = pd.DataFrame()
        data["c1"] = [chr(0) for _ in range(100)]
        data["c2"] = [10 for _ in range(100)]
        data["c3"] = [20 / 0.7 for _ in range(100)]
        df = self.spark.createDataFrame(data)

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 1.0)
        self.assertEqual(r2, 1.0)
        self.assertEqual(r3, 1.0)

    def test_wholetable_allsame(self):
        data = pd.DataFrame()
        data["c1"] = [chr(0) for _ in range(100)]
        data["c2"] = [10 for _ in range(100)]
        data["c3"] = [20 / 0.7 for _ in range(100)]
        df = self.spark.createDataFrame(data)

        r = deduplication(df=df)[0]
        self.assertEqual(r, 1.0)

    def test_singlecolumns_alldifferent(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [i / 0.7 for i in range(100)]
        df = self.spark.createDataFrame(data)

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 100.0)
        self.assertEqual(r2, 100.0)
        self.assertEqual(r3, 100.0)

    def test_wholetable_alldifferent(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [i / 0.7 for i in range(100)]
        df = self.spark.createDataFrame(data)

        r = deduplication(df=df)[0]
        self.assertEqual(r, 100.0)

    def test_singlecolumns_partial(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)
        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 81.0)
        self.assertEqual(r2, 81.0)
        self.assertEqual(r3, 81.0)

    def test_wholetable_partial(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)

        r = deduplication(df=df)[0]
        self.assertEqual(r, 81.0)

    def test_singlecolumns_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [i / 0.7 for i in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))
        df = df.withColumn("c3", replace_every_float_with_null(df["c3"]))

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 0.0)
        self.assertEqual(r2, 0.0)
        self.assertEqual(r3, 0.0)

    def test_wholetable_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [i for i in range(100)]
        data["c3"] = [i / 0.7 for i in range(100)]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_int_with_null(df["c2"]))
        df = df.withColumn("c3", replace_every_float_with_null(df["c3"]))

        r = deduplication(df=df)[0]
        self.assertEqual(r, 0.0)

    def test_singlecolumns_partialnulls(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 80.0)
        self.assertEqual(r2, 80.0)
        self.assertEqual(r3, 80.0)

    def test_wholetable_partialnulls(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [10 for _ in range(100)]
        l3 = [0.7 for _ in range(100)]
        for i in range(20):
            l1[i] = ""
        random.shuffle(l1)
        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))

        r = deduplication(df=df)[0]
        self.assertEqual(r, 80.0)

    def test_singlecolumns_partialnullspartialdistinct(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
            l2[i] = 0
            l3[i] = 0.
        for i in range(20, 40):
            l1[i] = "zzzzz"
            l2[i] = 500
            l3[i] = 402.2

        random.shuffle(l1)
        random.shuffle(l2)
        random.shuffle(l3)
        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        r1, r2, r3 = deduplication(["c1", "c2", "c3"], df)
        self.assertEqual(r1, 61.0)
        self.assertEqual(r2, 61.0)
        self.assertEqual(r3, 61.0)

    def test_wholetable_partialnullspartialdistinct(self):
        data = pd.DataFrame()
        # create and assign columns to df
        l1 = [chr(i) for i in range(100)]
        l2 = [i for i in range(100)]
        l3 = [i / 0.7 for i in range(100)]
        for i in range(20):
            l1[i] = ""
        for i in range(20, 40):
            l2[i] = 0
        for i in range(40, 60):
            l3[i] = 0.
        for i in range(60, 80):
            l1[i] = "zzz"
            l2[i] = 133
            l3[i] = 231.22

        data["c1"] = l1
        data["c2"] = l2
        data["c3"] = l3

        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", replace_0_with_null(df["c2"]))
        df = df.withColumn("c3", replace_0dot_with_null(df["c3"]))

        r = deduplication(df=df)[0]
        self.assertEqual(r, 21.0)
