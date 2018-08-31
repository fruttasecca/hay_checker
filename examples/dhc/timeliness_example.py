#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.metrics import timeliness

spark = SparkSession.builder.master("local[2]").appName("timeliness_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = timeliness(["birthDate"], dateFormat="yyyy/MM/dd", df=df, value="1960/10/22")[0]

print("Timeliness birthDate with respect to date 1960/10/22: {}".format(r1))