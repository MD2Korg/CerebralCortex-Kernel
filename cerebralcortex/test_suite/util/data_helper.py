from random import random
from datetime import datetime, timedelta
from cerebralcortex.core.util.spark_helper import get_or_create_sc
def gen_phone_battery_data():
    column_name = ["timestamp", "offset", "battery_level", "ver", "user"]
    sample_data = []
    timestamp = datetime(2019,1,9,11,34,59)
    tmp = 1
    sample = 100
    sqlContext = get_or_create_sc("sqlContext")
    for row in range(1000, 1, -1):
        tmp +=1
        if tmp==100:
            sample = sample-1
            tmp = 1
        timestamp = timestamp+timedelta(0,1)
        sample_data.append((timestamp, "21600000", sample, 1, "00000000-afb8-476e-9872-6472b4e66b68"))
    df = sqlContext.createDataFrame(sample_data, column_name)
    print(sample_data)


def gen_phone_gyro_data():
    pass


gen_phone_battery_data()