#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.metrics import freshness

spark = SparkSession.builder.master("local[2]").appName("freshness_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = freshness(["birthDate"], dateFormat="yyyy/MM/dd", df=df)[0]

print("Freshness birthDate: {}".format(r1))