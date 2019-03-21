from cerebralcortex.core.util.spark_helper import get_or_create_sc
lst1 = [
    [1, 10,20, 6],
    [2,20,30, 7],
    [3, 30,40,6],
    [4,40,50,7],
    [5,50,60,6]
]

lst2 = [
    [10, 1,2,3],
    [12, 4,5,6],
    [21, 23,45,12],
    [35,12,22,77],
    [42,3,1,7],
    [48,12,44,22],
    [58,6,4,2]
]

sqlContext = get_or_create_sc(type="sqlContext")

df1 = sqlContext.createDataFrame(lst1, schema=['id', 'st','et','val'])
df2 = sqlContext.createDataFrame(lst1, schema=['id', 'v1','v2','v3'])

df1.show(truncate=False)

df2.show(truncate=False)

df3 = df1.join(df2, [(df1.et <= df2.id) & (df1.st >= df2.id)])

df3.show(truncate=False)