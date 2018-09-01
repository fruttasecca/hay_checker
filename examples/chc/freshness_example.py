#!/usr/bin/python3

import pandas as pd

from haychecker.chc.metrics import freshness

df = pd.read_csv("examples/resources/employees.csv")

pd.set_option('max_columns', 10)
print(df)

# be careful with dateFormat, lowercase and uppercase letters have different
# meanings, a compete specification of strftime directives can be found
# at http://strftime.org/

r1 = freshness(["birthDate"], dateFormat="%Y/%m/%d", df=df)[0]

print("Freshness birthDate: {}".format(r1))