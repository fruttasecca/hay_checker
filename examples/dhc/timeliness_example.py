#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import timeliness

spark = SparkSession.builder.appName("timeliness_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = timeliness(["birthDate"], dateFormat="yyyy/MM/dd", df=df, value="1960/10/22")[0]

print("Timeliness birthDate with respect to date 1960/10/22: {}".format(r1))