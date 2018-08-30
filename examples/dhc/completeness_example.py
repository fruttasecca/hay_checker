#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType
import pandas as pd

#TODO: change import file
from hc.dhc.metrics import completeness

spark = SparkSession.builder.master("local[2]").appName("completeness_tests").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = pd.DataFrame()
data["c1"] = []
data["c2"] = []
schema = [StructField("c1", IntegerType(), True), StructField("c2", StringType(), True)]
df = spark.createDataFrame(data, StructType(schema))

r1, r2 = completeness(["c1", "c2"], df)

print("Completeness c1: {}, completeness c2: {}".format(r1, r2))

