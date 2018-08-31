# hay checker

A data quality tool

## Getting Started

These instructions will provide you a simple guide of how to use the tool and 
the prerequisites needed for both distributed and centralized versions of
 the *hay checker*

### Prerequisites

What things you need to use the *hay checker* script or library

Distributed version

```
hadoop
spark
pyspark
```

Centralized version

```
pandas
```

## API Reference & examples

Simple examples of metrics usage. More examples and source code can be 
found in [examples](./examples) folder

**Distributed version**

Create a spark session
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Deduplication_approximated_example").getOrCreate()
```

Import a dataframe

```python
df = spark.read.format("csv").option("header", "true").load("examples/resources/employees.csv")
df.show()
```
Output
```text
+---------+---------+--------------------+----------+----------+--------+------+---------+------+
| lastName|firstName|               title| birthDate|  hireDate|    city|region|reportsTo|salary|
+---------+---------+--------------------+----------+----------+--------+------+---------+------+
|  Davolio|    Nancy|Sales Representative|1948/12/08|1992/05/01| Seattle|    WA|        2|  2000|
|   Fuller|   Andrew|Vice President, S...|1952/02/19|1992/08/14|  Tacoma|    WA|     null|  3000|
|Leverling|    Janet|Sales Representative|1963/08/30|1992/04/01|Kirkland|    WA|        2|  2000|
|  Peacock| Margaret|Sales Representative|1937/09/19|1993/05/03| Redmond|    WA|        2|  2000|
| Buchanan|   Steven|       Sales Manager|1955/03/04|1993/10/17|  London|  null|        2|  2500|
|   Suyama|  Michael|Sales Representative|1963/07/02|1993/10/17|  London|  null|        5|  2000|
|     King|   Robert|Sales Representative|1960/05/29|1994/01/02|  London|  null|        5|  2000|
+---------+---------+--------------------+----------+----------+--------+------+---------+------+

```

In the following examples all imports are of the form *from haychecker.**dhc**.metrics import \<metric\>*
For the centralized version all metric calls are identical, while import has the form 
*from haychecker.**chc**.metrics import \<metric\>*

### Completeness
Measures how complete is the dataset by counting which entities are not 
missing in a column or in the whole table.


- column:

![alt text](figures/completeness/column.gif)
   
- table: 

![alt text](figures/completeness/table.gif)

***method* metrics.completeness(columns=None, df=None)**

Arguments

|         |                                                                                                                                |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the completenessmetric on the whole table.                                     |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later. |

Example

```python
from haychecker.dhc.metrics import completeness
r1, r2 = completeness(["region", "reportsTo"], df)
print("Completeness region: {}, completeness reportsTo: {}".format(r1, r2))
```
Output
```text
Completeness region: 57.14285714285714, completeness reportsTo: 85.71428571428571
```

---
### Deduplication
Measures how many values are duplicated within a column/dataset.

- column:

![alt text](figures/deduplication/column.gif)
       
- table: 

![alt text](figures/deduplication/table.gif)


***method* metrics.deduplication(columns=None, df=None)**

Arguments

|         |                                                                                                                                |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the deduplicationmetric on the whole table.                                    |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later  |

Example

```python
from haychecker.dhc.metrics import deduplication
r1, r2 = deduplication(["title", "city"], df)
print("Deduplication title: {}, deduplication city: {}".format(r1, r2))
```
Output
```text
Deduplication title: 42.857142857142854, deduplication city: 71.42857142857143
```

---
### Deduplication_approximated
Deduplication metric implementation using approximated count of distinct values,
 much faster then the standard deduplication implementation.

***method* metrics.deduplication_approximated(columns=None, df=None)**

Arguments

|         |                                                                                                                               |
|---------|-------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the deduplication_approximatedmetric on the whole table                       |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python
from haychecker.dhc.metrics import deduplication_approximated
r1, r2 = deduplication_approximated(["title", "city"], df)
print("Deduplication_approximated title: {}, deduplication_approximated city: {}".format(r1, r2))
```
Output
```text
Deduplication_approximated title: 42.857142857142854, deduplication_approximated city: 71.42857142857143
```
---
### Timeliness
Reflects how up-to-date the dataset is with respect to the given date/time.

- column

![alt text](figures/timeliness/column.gif)

!
In the centralized version of the library the [strftime](http://strftime.org/) directive 
is used as date/time format argument, while in the distributed version a [simple date format](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) is enough (e.g. "yyyy/MM/dd")

***method* metrics.timeliness(columns, value, df=None, dateFormat=None, timeFormat=None)**

Arguments

|            |                                                                                                                                                                                                                                         |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| columns    | Columns on which to run the metric                                                                                                                                                                                                      |
| value      | Value used to run the metric, confronting values in the specified columns against it                                                                                                                                                    |
| dateFormat | Format in which the value (and values in columns, if they are of string type) are; usedif the value and columns contain dates as strings, or are of date or timestamp type. Either dateFormator timeFormat must be passed, but not both |
| timeFormat | Format in which the value (and values in columns, if they are of string type) are; usedif the value and columns contain times as strings or are of timestamp type. Either dateFormator timeFormat must be passed, but not both          |
| df         | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later                                                                                                           |

Example

```python
from haychecker.dhc.metrics import timeliness
r1 = timeliness(["birthDate"], dateFormat="yyyy/MM/dd", df=df, value="1960/10/22")[0]
print("Timeliness birthDate with respect to date 1960/10/22: {}".format(r1))
```
Output
```text
Timeliness birthDate with respect to date 1960/10/22: 71.42857142857143
```
---
### Freshness
Measures how fresh is the dataset by taking the average distance of
 the tuples from the current date/time.
    
- column

![alt text](figures/freshness/column.gif)

!
In the centralized version of the library the [strftime](http://strftime.org/) directive 
is used as date/time format argument, while in the distributed version a [simple date format](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) is enough (e.g. "yyyy/MM/dd")

    
***method* metrics.freshness(columns, df=None, dateFormat=None, timeFormat=None)**

Arguments

|            |                                                                                                                                                                                                                                                                    |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| columns    | Columns on which to run the metric                                                                                                                                                                                                                                 |
| dateFormat | Format in which the value (and values in columns, if they are of string type) are; used if the value and columns contain dates as strings, or are of date or timestamp type. Either dateFormat or timeFormat must be passed, but not both                            |
| timeFormat | Format in which the values in columns are if those columns are of type string; otherwise they must be of type date or timestamp. Use this parameter if you are interested in a result in terms of days.Either dateFormat or timeFormat must be passed, but not both |
| df         | Dataframe on which to run the metric, None to have this function return a Task instance containing this metric to be run later                                                                                                                                      |


Example

```python
from haychecker.dhc.metrics import freshness
r1 = freshness(["birthDate"], dateFormat="yyyy/MM/dd", df=df)[0]
print("Freshness birthDate: {}".format(r1))
```
Output
```text
Freshness birthDate: 23434.571428571428 days
```
---
### Entropy *(of a column)*

Shannon entropy of a column.

- column X

![alt text](figures/entropy/column.gif)


***method* metrics.entropy(column, df=None)**

Arguments

|        |                                                                                                                               |
|--------|-------------------------------------------------------------------------------------------------------------------------------|
| column | Columns on which to run the metric                                                                                            |
| df     | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python
from haychecker.dhc.metrics import entropy
r1 = entropy("firstName", df)[0]
print("Entropy firstName: {}".format(r1))
```
Output
```text
Entropy firstName: 2.807354922057604
```
---
### Mutual information *(between two columns)*

- two columns X and Y

![alt text](figures/mutual_info/two_columns.gif)

***method* metrics.mutual_info(when, then, df=None)**

Arguments

|      |                                                                                                                               |
|------|-------------------------------------------------------------------------------------------------------------------------------|
| when | First column on which to compute MI                                                                                           |
| then | Second column on which to compute MI                                                                                          |
| df   | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python
from haychecker.dhc.metrics import mutual_info
r1 = mutual_info("title", "salary", df)[0]
print("Mutual information title/salary: {}".format(r1))
```
Output
```text
Mutual information title/salary: 0.7963116401738128
```
---
### Constraint

Measures how many tuples satisfy/break a given constraint. 
The constraints are functional dependencies. 
A functional dependency is a dependency where values of one
 set of columns imply values of another set of columns.
 
- table

![alt text](figures/constraint/table.gif)

***method* metrics.constraint(when, then, conditions=None, df=None)**

Arguments

|            |                                                                                                                               |
|------------|-------------------------------------------------------------------------------------------------------------------------------|
| when       | A list of columns in the df to use as the precondition of a functional constraint. No columnshould be in both when and then   |
| then       | A list of columns in the df to use as the postcondition of a functional constraint. No columnshould be in both when and then  |
| conditions | Conditions on which to filter data before applying the metric                                                                 |
| df         | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python
from haychecker.dhc.metrics import constraint
r1 = constraint(["title"], ["salary"], df=df)[0]
print("Constraint title/salary: {}".format(r1))
```
Output
```text
Constraint title/salary: 100.0
```
---
### Rule check

Measures how many values satisfy a rule

- table

![alt text](figures/rulecheck/table.gif)

***method* metrics.rule(conditions, df=None)**

Arguments

|            |                                                                                                                                |
|------------|--------------------------------------------------------------------------------------------------------------------------------|
| conditions | Conditions on which to run the metric                                                                                          |
| df         | Dataframe on which to run the metric, None to have this function return a Task instance containing this metric to be run later |

Example

```python
from haychecker.dhc.metrics import rule
condition1 = {"column": "salary", "operator": "gt", "value": 2100}
conditions = [condition1]
r1 = rule(conditions, df)[0]
print("Rule salary>2100: {}".format(r1))
```
Output
```text
Rule salary>2100: 28.57142857142857
```
---
### Grouprule check

Measures how many values satisfy a rule

- table 

![alt text](figures/grouprulecheck/table.gif)

***method* metrics.contains_date(format)**

Arguments

|            |                                                                                                                                |
|------------|--------------------------------------------------------------------------------------------------------------------------------|
| columns    | Columns on which to run the metric, grouping data                                                                              |
| conditions | Conditions on which to run the metric, filtering data before grouping, can be None                                             |
| having     | Conditions to apply to groups                                                                                                  |
| df         | Dataframe on which to run the metric, None to have this function return a Task instance containing this metric to be run later |

Example
```python
from haychecker.dhc.metrics import grouprule
condition1 = {"column": "city", "operator": "eq", "value": "London"}
conditions = [condition1]
having1 = {"column": "*", "operator": "gt", "value": 1, "aggregator": "count"}
havings = [having1]
r1 = grouprule(["title"], havings, conditions, df)[0]
print("Grouprule groupby \'title\' where city=\'London\' having count * > 1: {}".format(r1))
```
Output
```text
Grouprule groupby 'title' where city='London' having count * > 1: 50.0
```
---

## Authors

* Jacopo Gobbi
* Kateryna Konotopska

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details



