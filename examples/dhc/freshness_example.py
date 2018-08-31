#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import freshness

spark = SparkSession.builder.appName("freshness_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = freshness(["birthDate"], dateFormat="yyyy/MM/dd", df=df)[0]

print("Freshness birthDate: {}".format(r1))