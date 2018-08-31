#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.metrics import deduplication_approximated

spark = SparkSession.builder.master("local[2]").appName("Deduplication_approximated_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1, r2 = deduplication_approximated(["title", "city"], df)

print("Deduplication_approximated title: {}, deduplication_approximated city: {}".format(r1, r2))

task1 = deduplication_approximated(["title", "city"])
task2 = deduplication_approximated(["lastName"])
task3 = task1.add(task2)

result = task3.run(df)

r1, r2 = result[0]["scores"]
r3 = result[1]["scores"][0]

print("Deduplication_approximated title: {}, deduplication_approximated city: {}, "
      "deduplication_approximated lastName: {}".format(r1, r2, r3))