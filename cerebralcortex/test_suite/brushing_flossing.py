from datetime import datetime, timedelta

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.util.spark_helper import get_or_create_sc
import re
import sys
from typing import List
import numpy as np
import os
import pathlib
import unittest
import warnings

from cerebralcortex import Kernel
from cerebralcortex.test_suite.test_object_storage import TestObjectStorage
from cerebralcortex.test_suite.test_sql_storage import SqlStorageTest
from cerebralcortex.test_suite.test_stream import DataStreamTest
from functools import reduce
import math
from datetime import timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
# from pyspark.sql.functions import pandas_udf,PandasUDFType
from operator import attrgetter
from pyspark.sql.types import StructType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.window import Window

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates
from cerebralcortex.core.plotting.basic_plots import BasicPlots
from cerebralcortex.core.plotting.stress_plots import StressStreamPlots

#df3=reduce(lambda x, y: x.join(y, ['timestamp'], how='left'), dfs)

sqlContext = get_or_create_sc("sqlContext")
dfa=sqlContext.read.parquet("/home/ali/IdeaProjects/MD2K_DATA/cc3/moral_sample_data/accel/")
dfg=sqlContext.read.parquet("/home/ali/IdeaProjects/MD2K_DATA/cc3/moral_sample_data/gyro/")

CC = Kernel("./../../conf/", auto_offset_reset="smallest", study_name="default")

accel = DataStream(data=dfa, metadata=Metadata())
gyro = DataStream(data=dfg, metadata=Metadata())

schemaa = dfa.schema
schemag = dfg.schema

accel2 = get_orientation_data(ds=accel, sensor_type="accel", wrist="left")
gyro2 = get_orientation_data(ds=gyro, sensor_type="gyro", wrist="left")

#compute magnitude
accel3 = accel2.compute_magnitude(col_names=["accelerometer_x", "accelerometer_y", "accelerometer_z"],magnitude_col_name="accel_magnitude")
gyro3 = gyro2.compute_magnitude(col_names=["gyroscope_x", "gyroscope_y", "gyroscope_z"], magnitude_col_name="gyro_magnitude")


# interpolation
accel4 = accel3.interpolate()
gyro4 = gyro3.interpolate()

# join accel and gyro streams
ag = accel4.join(gyro4, on=['user', 'timestamp', 'version'], how='full').dropna()

# apply complementary filter
agc = ag.complementary_filter()

# generate candidates

agcc = get_candidates(agc)
agcc.show(10,truncate=False)
agcc.filter(agcc.candidate==1).show(10,truncate=False)
#
#agc.show(100,truncate=False)
