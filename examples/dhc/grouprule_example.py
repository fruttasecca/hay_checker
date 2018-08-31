#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import grouprule

spark = SparkSession.builder.appName("grouprule_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

condition1 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1]
having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
havings = [having1]
r1 = grouprule(["title"], havings, conditions, df)[0]

print("Grouprule groupby \'title\' where city=\'London\' having count * > 1: {}".format(r1))

condition1 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1]
having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
havings = [having1]
task1 = grouprule(["title"], havings, conditions)

condition1 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1]
having1 = {"column": "*", "operator": "gt", "value": 0, "aggregator": "count"}
havings = [having1]
task2 = grouprule(["firstName"], havings, conditions)

task3 = task1.add(task2)

result = task3.run(df)

r1 = result[0]["scores"][0]
r2 = result[1]["scores"][0]

print("Grouprule groupby \'title\' where city=\'London\' having count * > 1: {},"
      " grouprule groupby \'firstName\' where city=\'London\' having count * > 0: {}".format(r1, r2))