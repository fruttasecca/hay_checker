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


###Completeness
Measures how complete is the dataset by counting which entities are not 
missing in a column or in the whole table


- column:

 <img src="https://latex.codecogs.com/gif.latex?(1-\frac{|nulls\ in\ the\ column|}{|rows|}) \cdot 100"/> 
        
- table: 

<img src="https://latex.codecogs.com/gif.latex?(1-\frac{|nulls\ in\ the\ dataset|}{|rows| \cdot|columns|}) \cdot 100"/> 


***method* metrics.completeness(columns=None, df=None)**

Arguments

|         |                                                                                                                                |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the completenessmetric on the whole table.                                     |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later. |

Example

```python

```

---
###Deduplication
Measures how many values are duplicated within a column/dataset

- column:

<img src="https://latex.codecogs.com/gif.latex?(1-\frac{|deduplicated\ values|}{|values|}) \cdot 100"/> 
        
- table: 

<img src="https://latex.codecogs.com/gif.latex?(1-\frac{|duplicated\ rows|}{|rows|}) \cdot 100"/> 



***method* metrics.deduplication(columns=None, df=None)**

Arguments

|         |                                                                                                                                |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the deduplicationmetric on the whole table.                                    |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later  |

Example

```python

```

---
###Deduplication_approximated
Deduplication metric implementation using approximated count of distinct values,
 much faster then the standard deduplication implementation

***method* metrics.deduplication_approximated(columns=None, df=None)**

Arguments

|         |                                                                                                                               |
|---------|-------------------------------------------------------------------------------------------------------------------------------|
| columns | Columns on which to run the metric, None to run the deduplication_approximatedmetric on the whole table                       |
| df      | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python

```
---
###Timeliness
Reflects how up-to-date the dataset is with respect to the given date/time


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

```
---
###Freshness
Measures how fresh is the dataset by taking the average distance of
 the tuples from the current date/time

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

```
---
###Entropy *(of a column)*

Shannon entropy of a column

***method* metrics.entropy(column, df=None)**

Arguments

|        |                                                                                                                               |
|--------|-------------------------------------------------------------------------------------------------------------------------------|
| column | Columns on which to run the metric                                                                                            |
| df     | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python

```
---
###Mutual information *(between two columns)*

[Mutual information](https://en.wikipedia.org/wiki/Mutual_information) between two columns

***method* metrics.mutual_info(when, then, df=None)**

Arguments

|      |                                                                                                                               |
|------|-------------------------------------------------------------------------------------------------------------------------------|
| when | First column on which to compute MI                                                                                           |
| then | Second column on which to compute MI                                                                                          |
| df   | Dataframe on which to run the metric, None to have this function return a Task instance containingthis metric to be run later |

Example

```python

```
---
###Constraint

***method* metrics.constraint(when, then, conditions=None, df=None)**

Arguments

Example

```python

```
---
###Conditions
***method* metrics.rule(conditions, df=None)**

Arguments

Example

```python

```
---

***method* metrics.contains_date(format)**

Arguments

Example

---

## Authors

* Jacopo Gobbi
* Kateryna Konotopska

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details


