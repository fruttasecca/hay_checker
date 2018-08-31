#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import mutual_info

spark = SparkSession.builder.appName("mutual_information_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1 = mutual_info("title", "salary", df)[0]

print("Mutual information title/salary: {}".format(r1))

task1 = mutual_info("city", "salary")
task2 = mutual_info("title", "salary")
task3 = task1.add(task2)

result = task3.run(df)

r1 = result[0]["scores"][0]
r2 = result[1]["scores"][0]

print("Mutual information city/salary: {}, Mutual information title/salary: {}".format(r1, r2))