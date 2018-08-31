#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import entropy

spark = SparkSession.builder.appName("entropy_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = entropy("firstName", df)[0]

print("Entropy firstName: {}".format(r1))

task1 = entropy("firstName")
task2 = entropy("salary")
task3 = task1.add(task2)

result = task3.run(df)

r1 = result[0]["scores"][0]
r2 = result[1]["scores"][0]

print("Entropy firstName: {}, entropy salary: {}".format(r1, r2))