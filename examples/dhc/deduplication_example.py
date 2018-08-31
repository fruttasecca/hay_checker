#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.metrics import deduplication

spark = SparkSession.builder.master("local[2]").appName("deduplication_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

r1, r2 = deduplication(["title", "city"], df)

print("Deduplication title: {}, deduplication city: {}".format(r1, r2))

task1 = deduplication(["title", "city"])
task2 = deduplication(["lastName"])
task3 = task1.add(task2)

result = task3.run(df)

r1, r2 = result[0]["scores"]
r3 = result[1]["scores"][0]

print("Deduplication title: {}, deduplication city: {}, deduplication lastName: {}".format(r1, r2, r3))