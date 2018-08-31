#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.metrics import completeness

spark = SparkSession.builder.master("local[2]").appName("completeness_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1, r2 = completeness(["region", "reportsTo"], df)

print("Completeness region: {}, completeness reportsTo: {}".format(r1, r2))

task1 = completeness(["region", "reportsTo"])
task2 = completeness(["city"])
task3 = task1.add(task2)

result = task3.run(df)

r1, r2 = result[0]["scores"]
r3 = result[1]["scores"][0]

print("Completeness region: {}, completeness reportsTo: {}, completeness city: {}".format(r1, r2, r3))