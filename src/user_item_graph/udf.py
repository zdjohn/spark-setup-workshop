import pandas as pd
import pyspark.sql.functions as F

from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql import DataFrame
from itertools import combinations
from pyspark.sql.types import ArrayType, StructType, IntegerType, StructField, StringType


@udf(ArrayType(StringType()))
def udf_combination(v):
    return [f'{x}-{y}' for x, y in combinations(v, 2)]


@pandas_udf(ArrayType(StringType()))
def pandas_udf_combination(v):
    res = []
    for row in v:
        res.append([f'{x}-{y}' for x, y in combinations(row, 2)])
    return pd.Series(res)
