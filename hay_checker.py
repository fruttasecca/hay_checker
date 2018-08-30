#!/usr/bin/python3
import sys

import requests
import json
from pyspark.sql import SparkSession
import __main__

from hc._common.config import Config
from hc.dhc.task import Task as dTask
from hc.chc.task import Task as cTask


def import_data(spark, path, has_header, delimiter, infer_schema):
    is_http = path[:4] == "http"
    is_jdbc = path[:4] == "jdbc"

    if is_http:
        print("Importing from http")

        if "json" in path:
            file = requests.get(path).text
            file = file.split("\n")
            rdd = spark.sparkContext.parallelize(file)
            df = spark.read.json(rdd)
        elif "csv" in path:
            file = requests.get(path).content.decode("utf-8")
            file = file.split("\n")
            rdd = spark.sparkContext.parallelize(file)
            df = spark.read.csv(rdd, header=has_header, sep=delimiter, inferSchema=infer_schema)
        else:
            print("Can't download this format.")
    elif is_jdbc:
        print("Importing from jdbc...")
        split = path.split(" ")  # jdbc url first, table name second
        df = spark.read.jdbc(url=split[0], table=split[1])
    else:
        type = path.split(".")[-1]

        if type == "parquet":
            df = spark.read.parquet(path)
        elif type == "json":
            df = spark.read.json(path)
        elif type == "csv":
            df = spark.read.csv(path=path, header=has_header, sep=delimiter, inferSchema=infer_schema)
        else:
            #  assume it's a table
            print("Importing table...")
            df = spark.read.table(path)
    return df


if __name__ == '__main__':
    print("____HAY_CHECKER____")

    # PARSING CONFIG
    args = sys.argv[1:]
    print("Parsing configuration file")
    if len(args) != 2 or args[0] != "--config":
        print("Usage:")
        print("spark-submit your-spark-parameters hay_checker.py --config <path to config.json>")
        print("or")
        print("python3 hay_checker.py --config <path to config.json")
        exit()
    config = Config(args[1])

    # LAUNCH SPARK, detect if running in distributed or centralized version
    print("Launching spark")
    spark = SparkSession.builder
    spark = spark.getOrCreate()

    # IMPORT DATA
    print("Getting data")
    df = import_data(spark, config["table"], config["header"], config["delimiter"], config["inferSchema"])

    # EITHER GO WITH SPARK OR PANDAS
    name = spark.conf.get("spark.app.name")
    filename = __main__.__file__.split("/")[-1]
    if name == filename:
        # it has been launched using spark-submit
        print("Launching distributed version of hay_checker...")
        dtask = dTask(config["metrics"])
        results = dtask.run(df)
    else:
        # it has been launched as python3 hay_checker.py
        print("Launching centralized version of hay_checker...")
        print("Converting spark dataframe to pandas dataframe...")
        df = df.toPandas()
        ctask = cTask(config["metrics"])
        results = ctask.run(df)

    print("Writing results to %s" % config["output"])
    with open(config["output"], 'w') as outfile:
        json.dump(results, outfile, sort_keys=True)
