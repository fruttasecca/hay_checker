#!/usr/bin/python3
from pyspark.sql import SparkSession

from haychecker.dhc.metrics import rule

spark = SparkSession.builder.appName("rule_example").getOrCreate()

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

condition1 = {"column": "salary", "operator": "gt", "value": 2100}
conditions = [condition1]
r1 = rule(conditions, df)[0]

print("Rule salary>2100: {}".format(r1))

condition1 = {"column": "salary", "operator": "lt", "value": 2100}
condition2 = {"column": "title", "operator": "eq", "value": "Sales Representative"}
conditions = [condition1, condition2]
task1 = rule(conditions)

condition1 = {"column": "salary", "operator": "lt", "value": 2100}
condition2 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1, condition2]
task2 = rule(conditions)
task3 = task1.add(task2)

result = task3.run(df)

r1 = result[0]["scores"][0]
r2 = result[1]["scores"][0]

print("Rule salary<2100 and title=\"Sales Representative\": {},"
      " rule salary<2100 and city=\"London\": {}".format(r1, r2))