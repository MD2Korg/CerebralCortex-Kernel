from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from typing import List
import re


lst1 = [
    [1, 202, 10,20, 6],
    [2,202,20,30, 7],
    [3,202, 30,40,6],
    [4,203,40,50,7],
    [5,203,50,80,6]
]

lst2 = [
    [10,202, 1,2,3],
    [12, 202,4,5,6],
    [21, 202,23,45,12],
    [35,202,12,22,77],
    [42,203,3,1,7],
    [48,203,12,44,22],
    [58,203,6,4,2]
]

sqlContext = get_or_create_sc(type="sqlContext")

df1 = sqlContext.createDataFrame(lst1, schema=['id', 'usr', 'st','et','val'])
df2 = sqlContext.createDataFrame(lst2, schema=['id', 'usr', 'v1','v2','v3'])

# df1.show(truncate=False)
#
# df2.show(truncate=False)
#
# #df3 = df1.join(df2, [(df1.et <= df2.id) & (df1.st >= df2.id)])
# dd = [str(df2.v2._jc),str(df1.et._jc)]
# df3=df1.join(df2, df2.id.between(F.col("st"), F.col("et")))
# df3.show(truncate=False)

@pandas_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
def mean_udf(v):
    return v.mean()

win = F.window("timestamp", windowDuration=windowDuration, slideDuration=slideDuration, startTime=startTime)