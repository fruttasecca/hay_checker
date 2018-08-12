#!/usr/bin/python3
"""
Simple script to generate data for benchmarking, tests, etc.
"""

import pandas as pd
import numpy as np
from random import shuffle


def generate_df(rows, val, completeness, deduplication, before_date, before_time):
    assert rows > val
    df = pd.DataFrame()

    # do completeness columns, set to null 'val' values and shuffle
    compl = np.random.rand(rows)
    for i in range(val):
        compl[i] = np.nan
    for i in range(completeness):
        np.random.shuffle(compl)
        df["c%i" % i] = compl

    # do deduplication columns, create 'val' unique values and shuffle
    dedu = np.zeros(rows)
    for i in range(val):
        dedu[i] = i
    for i in range(deduplication):
        np.random.shuffle(dedu)
        df["d%i" % i] = dedu

    # do date columns, set to 10/10/1980 'val' values and the rest to 30/05/2018
    date = ["30/05/2018" for _ in range(rows)]
    for i in range(val):
        date[i] = "10/10/1980"
    for i in range(before_date):
        shuffle(date)
        df["date%i" % i] = date

    # do time columns, set to 12:56:25 'val' values and the rest to 20:41:00
    time = ["20:41:00" for _ in range(rows)]
    for i in range(val):
        time[i] = "12:56:25"
    for i in range(before_time):
        shuffle(time)
        df["time%i" % i] = time

    # expected values
    exc = (rows - val) / rows
    edup = val / rows
    ebdate = val / rows
    ebtime = val / rows
    print("expected")
    print("completeness %s" % exc)
    print("deduplication %s" % edup)
    print("before date %s" % ebdate)
    print("before time %s" % ebtime)

    return df


def save(df, name, format):
    if format == "csv":
        df.to_csv(name + ".csv", header=True, mode='w', index=False)
    elif format == "json":
        df.to_json(name + ".json", lines=True, orient="records")
    elif format == "parquet":
        df.to_parquet(name + ".parquet", compression="UNCOMPRESSED")


# df = generate_df(10000000, 500000, 2, 2, 2, 2)
# save(df, "data", "parquet")
