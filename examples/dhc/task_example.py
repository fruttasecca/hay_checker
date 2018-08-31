#!/usr/bin/python3
from pyspark.sql import SparkSession

#TODO: change import file
from hc.dhc.task import Task
from hc.dhc.metrics import *

spark = SparkSession.builder.master("local[2]").appName("task_example").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")

df.show()

# create an empty Task
task = Task()

# constraint returns a Task instance without running it untill dataframe is passed
task1 = completeness(["region", "reportsTo"])
task2 = completeness(["city"])

task.add(task1)
task.add(task2)

task1 = deduplication(["title", "city"])
task2 = deduplication(["lastName"])

task.add(task1)
task.add(task2)

task1 = deduplication_approximated(["title", "city"])
task2 = deduplication_approximated(["lastName"])

task.add(task1)
task.add(task2)

task1 = timeliness(["birthDate"], dateFormat="yyyy/MM/dd", value="1960/10/22")

task.add(task1)

task1 = freshness(["birthDate"], dateFormat="yyyy/MM/dd")

task.add(task1)

task1 = entropy("firstName")
task2 = entropy("salary")

task.add(task1)
task.add(task2)

task1 = mutual_info("city", "salary")
task2 = mutual_info("title", "salary")

task.add(task1)
task.add(task2)

task1 = constraint(["city"], ["region"])
task2 = constraint(["region"], ["city"])

task.add(task1)
task.add(task2)

condition1 = {"column": "salary", "operator": "lt", "value": 2100}
condition2 = {"column": "title", "operator": "eq", "value": "Sales Representative"}
conditions = [condition1, condition2]
task1 = rule(conditions)

condition1 = {"column": "salary", "operator": "lt", "value": 2100}
condition2 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1, condition2]
task2 = rule(conditions)

task.add(task1)
task.add(task2)

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

task.add(task1)
task.add(task2)

# run all tasks contained in the task
# in this way the optimizer does all checks to save running time
result = task.run(df)

# read the results, a list of dictionaries is returned, a dictionary corresponds to a task,
# each dictionary containing score or scores for each task
r1, r2 = result[0]["scores"]
r3 = result[1]["scores"][0]

print("Completeness region: {}, completeness reportsTo: {}, completeness city: {}".format(r1, r2, r3))

r1, r2 = result[2]["scores"]
r3 = result[3]["scores"][0]

print("Deduplication title: {}, deduplication city: {}, deduplication lastName: {}".format(r1, r2, r3))

r1, r2 = result[4]["scores"]
r3 = result[5]["scores"][0]

print("Deduplication_approximated title: {}, deduplication_approximated city: {}, "
      "deduplication_approximated lastName: {}".format(r1, r2, r3))

r1 = result[6]["scores"][0]

print("Timeliness birthDate with respect to date 1960/10/22: {}".format(r1))

r1 = result[7]["scores"][0]

print("Freshness birthDate: {}".format(r1))

r1 = result[8]["scores"][0]
r2 = result[9]["scores"][0]

print("Entropy firstName: {}, entropy salary: {}".format(r1, r2))

r1 = result[10]["scores"][0]
r2 = result[11]["scores"][0]

print("Mutual information city/salary: {}, Mutual information title/salary: {}".format(r1, r2))

r1 = result[12]["scores"][0]
r2 = result[13]["scores"][0]

print("Constraint city/region: {}, сonstraint region/city: {}".format(r1, r2))

r1 = result[14]["scores"][0]
r2 = result[15]["scores"][0]

print("Rule salary<2100 and title=\"Sales Representative\": {},"
      " rule salary<2100 and city=\"London\": {}".format(r1, r2))

r1 = result[16]["scores"][0]
r2 = result[17]["scores"][0]

print("Grouprule groupby \'title\' where city=\'London\' having count * > 1: {},"
      " grouprule groupby \'firstName\' where city=\'London\' having count * > 0: {}".format(r1, r2))