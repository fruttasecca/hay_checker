import random
import unittest

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, to_timestamp, to_date
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType

from haychecker.dhc.metrics import timeliness

replace_empty_with_null = udf(lambda x: None if x == "" else x, StringType())
replace_0_with_null = udf(lambda x: None if x == 0 else x, IntegerType())
replace_0dot_with_null = udf(lambda x: None if x == 0. else x, FloatType())
replace_every_string_with_null = udf(lambda x: None, StringType())
replace_every_int_with_null = udf(lambda x: None, IntegerType())
replace_every_float_with_null = udf(lambda x: None, FloatType())


class TestTimeliness(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestTimeliness, self).__init__(*args, **kwargs)

        self.spark = SparkSession.builder.master("local[2]").appName("timeliness_test").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def test_empty(self):
        data = pd.DataFrame()
        data["c1"] = []
        data["c2"] = []
        schema = [StructField("c1", StringType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))

        r1, r2 = timeliness(["c1", "c2"], dateFormat="dd:MM:yyyy", df=df, value="10:22:1980")
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)

    def test_allnull(self):
        data = pd.DataFrame()
        data["c1"] = [chr(i) for i in range(100)]
        data["c2"] = [chr(i + 1) for i in range(100)]
        schema = [StructField("c1", StringType(), True), StructField("c2", StringType(), True)]
        df = self.spark.createDataFrame(data, StructType(schema))
        df = df.withColumn("c1", replace_every_string_with_null(df["c1"]))
        df = df.withColumn("c2", replace_every_string_with_null(df["c2"]))

        r1, r2 = timeliness(["c1", "c2"], dateFormat="dd:MM:yyyy", df=df, value="10:22:1980")
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)

        r1, r2 = timeliness(["c1", "c2"], timeFormat="ss:mm:HH", df=df, value="10:22:19")
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)

    def test_dateformat(self):
        format = "dd/MM/yyyy"

        # test wrong type of column
        data = pd.DataFrame()
        dates = [i for i in range(100)]
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        value = "21/05/2000"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], dateFormat=format, df=df, value=value)

        # test correct type
        data = pd.DataFrame()
        dates = ["10/05/2000" for _ in range(50)]
        dates.extend(["20/05/2000" for _ in range(50)])
        random.shuffle(dates)
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_date(df["c1"], format))

        value = "21/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "20/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "10/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "12/12/1999"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        data = pd.DataFrame()
        dates = ["10/05/2000" for _ in range(50)]
        dates.extend(["20/05/2000" for _ in range(50)])
        for i in range(10):
            dates[i] = ""
            dates[-(i + 1)] = ""
        random.shuffle(dates)
        data["c1"] = dates
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_date(df["c1"], format))

        value = "21/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 80.)
        self.assertEqual(r2, 80.)
        self.assertEqual(r3, 80.)

        value = "20/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 40.)
        self.assertEqual(r2, 40.)
        self.assertEqual(r3, 40.)

        value = "10/05/2000"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "12/12/1999"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], dateFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

    def test_timeformat_nodate(self):
        format = "ss:HH:mm"

        # test wrong type of column
        data = pd.DataFrame()
        times = [i for i in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        value = "21:05:50"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], timeFormat=format, df=df, value=value)

        # test correct type
        data = pd.DataFrame()
        times = ["10:05:50" for _ in range(50)]
        times.extend(["01:18:01" for _ in range(50)])
        random.shuffle(times)
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        value = "01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "00:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "10:05:50"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        data = pd.DataFrame()
        times = ["10:05:50" for _ in range(50)]
        times.extend(["00:18:01" for _ in range(50)])
        for i in range(10):
            times[i] = ""
            times[-(i + 1)] = ""
        random.shuffle(times)
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        value = "00:19:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 80.)
        self.assertEqual(r2, 80.)
        self.assertEqual(r3, 80.)

        value = "00:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 40.)
        self.assertEqual(r2, 40.)
        self.assertEqual(r3, 40.)

        value = "10:05:50"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

    def test_timeformat_withdate(self):
        format = "dd/MM/yyyy ss:HH:mm"

        # test wrong type of column
        data = pd.DataFrame()
        times = [i for i in range(100)]
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        value = "01/10/1900 21:05:50"
        with self.assertRaises(SystemExit) as cm:
            r1 = timeliness(["c1"], timeFormat=format, df=df, value=value)

        # test correct type
        data = pd.DataFrame()
        times = ["01/10/1900 21:05:50" for _ in range(50)]
        times.extend(["01/10/1900 01:18:01" for _ in range(50)])
        times.extend(["21/10/1900 01:18:01" for _ in range(100)])
        random.shuffle(times)
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        value = "21/10/1900 01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 100.)
        self.assertEqual(r2, 100.)
        self.assertEqual(r3, 100.)

        value = "21/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 50.)
        self.assertEqual(r2, 50.)
        self.assertEqual(r3, 50.)

        value = "01/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 25.)
        self.assertEqual(r2, 25.)
        self.assertEqual(r3, 25.)

        value = "01/10/1900 21:05:10"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00/00/0000 00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        data = pd.DataFrame()
        times = ["01/10/1900 21:05:50" for _ in range(50)]
        times.extend(["01/10/1900 01:18:01" for _ in range(50)])
        times.extend(["21/10/1900 01:18:01" for _ in range(100)])
        for i in range(10):
            times[i] = ""
            times[-(i + 1)] = ""
        random.shuffle(times)
        data["c1"] = times
        df = self.spark.createDataFrame(data)
        df = df.withColumn("c1", replace_empty_with_null(df["c1"]))
        df = df.withColumn("c2", to_timestamp(df["c1"], format))
        df = df.withColumn("c3", to_timestamp(df["c1"], format))

        value = "21/10/1900 01:19:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 90.)
        self.assertEqual(r2, 90.)
        self.assertEqual(r3, 90.)

        value = "21/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 45.)
        self.assertEqual(r2, 45.)
        self.assertEqual(r3, 45.)

        value = "01/10/1900 01:18:01"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 20.)
        self.assertEqual(r2, 20.)
        self.assertEqual(r3, 20.)

        value = "01/10/1900 21:05:10"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)

        value = "00/00/0000 00:00:00"
        r1, r2, r3 = timeliness(["c1", "c2", "c3"], timeFormat=format, df=df, value=value)
        self.assertEqual(r1, 0.)
        self.assertEqual(r2, 0.)
        self.assertEqual(r3, 0.)
