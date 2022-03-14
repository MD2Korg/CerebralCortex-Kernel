
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()

from datetime import datetime
def f():
    t1 = datetime.now()
    df2 = spark.read.load("/Users/ali/tmp/t/")
    df2.filter("user=='usr2'").explain(True)
    print("Total Time", datetime.now()-t1)


f()