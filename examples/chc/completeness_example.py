#!/usr/bin/python3
import pandas as pd

from haychecker.chc.metrics import completeness

df = pd.read_csv("examples/resources/employees.csv")

pd.set_option('max_columns', 10)
print(df)

r1, r2 = completeness(["region", "reportsTo"], df)

print("Completeness region: {}, completeness reportsTo: {}".format(r1, r2))

task1 = completeness(["region", "reportsTo"])
task2 = completeness(["city"])
task3 = task1.add(task2)

result = task3.run(df)

r1, r2 = result[0]["scores"]
r3 = result[1]["scores"][0]

print("Completeness region: {}, completeness reportsTo: {}, completeness city: {}".format(r1, r2, r3))