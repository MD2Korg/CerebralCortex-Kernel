import pandas as pd
import os, sys
import numpy as np
import ast
import binascii

from cerebralcortex import Kernel
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.algorithms.stress_from_raw_ppg.raw_ppg import process_decoded_ppg


# Initialize CerebralCortex Kernel
CC = Kernel("/home/nndugudi/samsung_test/CerebralCortex-Kernel/conf/")


df = CC.get_stream('MOTION_SEQUENCE_NUMBER--org.md2k.selflytics.decisions.dev--motionsense--MOTION_SENSE_HRV--LEFT_WRIST').data
#rows = df.orderBy('localtime').collect()
#df.show(truncate=False)
df_seq = df.filter((df['localtime'] >= '2019-08-16 09:30:00')  & (df['user'] == 'cb23a061-61f4-3600-b33e-7ae5e92890ad')).orderBy(df['localtime'])

df_seq = df_seq.drop(*['timestamp', 'version', 'user'])
df_seq = df_seq.dropDuplicates(subset=['localtime'])

df_acc = CC.get_stream('ACCELEROMETER--org.md2k.selflytics.decisions.dev--motionsense--MOTION_SENSE_HRV--LEFT_WRIST').data
df_acc = df_acc.filter((df_acc['localtime'] >= '2019-08-16 09:30:00')  & (df_acc['user'] == 'cb23a061-61f4-3600-b33e-7ae5e92890ad')).orderBy(df_acc['localtime'])
df_acc = df_acc.drop(*['timestamp', 'version', 'user'])
df_acc = df_acc.dropDuplicates(subset=['localtime'])
df_acc = df_acc.withColumnRenamed('x','a_x').withColumnRenamed('y','a_y').withColumnRenamed('z','a_z')

df_gyro = CC.get_stream('GYROSCOPE--org.md2k.selflytics.decisions.dev--motionsense--MOTION_SENSE_HRV--LEFT_WRIST').data
df_gyro = df_gyro.filter((df_gyro['localtime'] >= '2019-08-16 09:30:00')  & (df_gyro['user'] == 'cb23a061-61f4-3600-b33e-7ae5e92890ad')).orderBy(df_gyro['localtime'])
df_gyro = df_gyro.drop(*['timestamp', 'version', 'user'])
df_gyro = df_gyro.dropDuplicates(subset=['localtime'])
df_gyro = df_gyro.withColumnRenamed('x','g_x').withColumnRenamed('y','g_y').withColumnRenamed('z','g_z')

df_ppg = CC.get_stream('PPG--org.md2k.selflytics.decisions.dev--motionsense--MOTION_SENSE_HRV--LEFT_WRIST').data
#rows = df.orderBy('timestamp').collect()

df_fil = df_ppg.filter((df_ppg['localtime'] >= '2019-08-16 09:30:00')  & (df_ppg['user'] == 'cb23a061-61f4-3600-b33e-7ae5e92890ad')).orderBy(df_ppg['localtime'])
df_fil = df_fil.dropDuplicates(subset=['localtime'])

df_joined = df_fil.join(df_seq, df_fil.localtime == df_seq.localtime).drop(df_seq.localtime)

df_joined_acc = df_joined.join(df_acc, df_joined.localtime == df_acc.localtime).drop(df_acc.localtime)

df_joined_gyro = df_joined_acc.join(df_gyro, df_joined_acc.localtime == df_gyro.localtime).drop(df_gyro.localtime)

ds = DataStream(data=df_joined_gyro, metadata=Metadata())
#df_joined_acc.show(2056)


windowed_raw_ppg_ds = ds.create_windows()
stress_predictions = windowed_raw_ppg_ds.compute(process_decoded_ppg)

r = stress_predictions.data.toPandas()
print(r)