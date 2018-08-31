import datetime
import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, to_timestamp, to_date, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import freshness

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestFreshness(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFreshness, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("freshness_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", StringType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        r1, r2 = freshness(["c1", "c2"], dateFormat="dd:MM:yyyy", df=df)
        self.assertEqual("None days", r1)
        self.assertEqual("None days", r2)

        r1, r2 = freshness(["c1", "c2"], timeFormat="dd:MM:yyyy", df=df)
        self.assertEqual("None seconds", r1)
        self.assertEqual("None seconds", r2)

    def test_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [chr(i + 1) for i in range(100)]
        schema = [StructField("c1", StringType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_string_with_null(df["c2"]))

        r1, r2 = freshness(["c1", "c2"], dateFormat="dd:MM:yyyy", df=df)
        self.assertEqual("None days", r1)
        self.assertEqual("None days", r2)

        r1, r2 = freshness(["c1", "c2"], timeFormat="ss:mm:HH", df=df)
        self.assertEqual("None seconds", r1)
        self.assertEqual("None seconds", r2)

    def test_dateformat(self):
        format = "yyyy-MM-dd HH:mm:ss"
        now = str(datetime.datetime.now())[:19]

        # test wrong type of column
        data = pd.DataFrame()
        dates = [i for i in range(100)]
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], dateFormat=format, df=df)

        # test correct type
        data = pd.DataFrame()
        dates = [now for _ in range(100)]
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_date(df["c1"], format))

        r1, r2, r3 = freshness(["c1", "c2", "c3"], dateFormat=format, df=df)
        self.assertEqual(r1, "0.0 days")
        self.assertEqual(r2, "0.0 days")
        self.assertEqual(r3, "0.0 days")

        data = pd.DataFrame()
        dates = [now for _ in range(100)]
        for i in range(20):
            dates[i] = ""
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_date(df["c1"], format))

        r1, r2, r3 = freshness(["c1", "c2", "c3"], dateFormat=format, df=df)
        self.assertEqual(r1, "0.0 days")
        self.assertEqual(r2, "0.0 days")
        self.assertEqual(r3, "0.0 days")

    def test_timeformat_nodate(self):
        format = "HH:mm:ss"
        now = str(datetime.datetime.now())[11:19]

        # test wrong type of column
        data = pd.DataFrame()
        times = [i for i in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        data = pd.DataFrame()
        times = [now for _ in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

        data = pd.DataFrame()
        times = [now for _ in range(100)]
        for i in range(20):
            times[i] = ""
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

    def test_timeformat_nodate_dateincolumns(self):
        format = "HH:mm:ss"
        now = str(datetime.datetime.now())[11:19]

        # test wrong type of column
        data = pd.DataFrame()
        times = [i for i in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        data = pd.DataFrame()
        times = [now for _ in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", current_timestamp())
        df = df.withColumn("c3", current_timestamp())

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

        data = pd.DataFrame()
        times = [now for _ in range(100)]
        for i in range(20):
            times[i] = ""
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", current_timestamp())
        df = df.withColumn("c3", current_timestamp())

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, 10.0)
        self.assertLessEqual(r2, 10.0)
        self.assertLessEqual(r3, 10.0)

    def test_timeformat_withdate(self):
        format = "yyyy-MM-dd HH:mm:ss"
        time = str(datetime.datetime.now())[11:19]
        time = "1970-01-01 " + time

        # test wrong type of column
        data = pd.DataFrame()
        times = [i for i in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        with self.assertRaises(SystemExit) as cm:
            r1 = freshness(["c1"], timeFormat=format, df=df)

        # test correct type
        data = pd.DataFrame()
        times = [time for _ in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))
        df = df.withColumn("c4", current_timestamp().cast("long") - to_timestamp(lit(time), format).cast("long"))
        # seconds from 1970 plus 10 seconds for computation time
        seconds = df.collect()[0][3] + 10

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, seconds)
        self.assertLessEqual(r2, seconds)
        self.assertLessEqual(r3, seconds)

        data = pd.DataFrame()
        times = [time for _ in range(100)]
        for i in range(20):
            times[i] = ""
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        r1, r2, r3 = freshness(["c1", "c2", "c3"], timeFormat=format, df=df)
        r1 = float(r1.split(" ")[0])
        r2 = float(r2.split(" ")[0])
        r3 = float(r3.split(" ")[0])
        self.assertLessEqual(r1, seconds)
        self.assertLessEqual(r2, seconds)
        self.assertLessEqual(r3, seconds)
