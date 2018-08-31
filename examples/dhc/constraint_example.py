#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import constraint

spark = SparkSession.builder.appName("constraint_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = constraint(["title"], ["salary"], df=df)[0]

print("Constraint title/salary: {}".format(r1))

task1 = constraint(["city"], ["region"])
task2 = constraint(["region"], ["city"])
task3 = task1.add(task2)

result = task3.run(df)

r1 = result[0]["scores"][0]
r2 = result[1]["scores"][0]

print("Constraint city/region: {}, сonstraint region/city: {}".format(r1, r2))