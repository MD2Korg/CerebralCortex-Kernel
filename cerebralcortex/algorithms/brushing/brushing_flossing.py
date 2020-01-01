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
import numpy as np
import math
from scipy.stats import skew
from scipy.stats import kurtosis

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
from cerebralcortex.algorithms.brushing.helper import get_orientation_data, get_candidates, classify_brushing, get_max_features, reorder_columns
from cerebralcortex.core.plotting.basic_plots import BasicPlots
from cerebralcortex.core.plotting.stress_plots import StressStreamPlots

#df3=reduce(lambda x, y: x.join(y, ['timestamp'], how='left'), dfs)

sqlContext = get_or_create_sc("sqlContext")
dfa=sqlContext.read.parquet("/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/study=moral/stream=accelerometer--org.md2k.motionsense--motion_sense--left_wrist/version=1/user=820c/")
dfg=sqlContext.read.parquet("/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/study=moral/stream=gyroscope--org.md2k.motionsense--motion_sense--left_wrist/version=1/user=820c/")

dfa = dfa.withColumn("version", F.lit(1))
dfa = dfa.withColumn("user", F.lit("820c"))

dfg = dfg.withColumn("version", F.lit(1))
dfg = dfg.withColumn("user", F.lit("820c"))

dfa = dfa.dropDuplicates(subset=['timestamp'])
dfg = dfg.dropDuplicates(subset=['timestamp'])
##########################################################################################################

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", auto_offset_reset="smallest", study_name="default")

ds_accel = DataStream(data=dfa, metadata=Metadata())
ds_gyro = DataStream(data=dfg, metadata=Metadata())

ds_gyro.show(1)
ds_accel.show(1)
# interpolation
ds_accel_interpolated = ds_accel.interpolate()
ds_gyro_interpolated = ds_gyro.interpolate()

##compute magnitude
ds_accel_magnitude = ds_accel_interpolated.compute_magnitude(col_names=["accelerometer_x", "accelerometer_y", "accelerometer_z"], magnitude_col_name="accel_magnitude")
ds_gyro_magnitude = ds_gyro_interpolated.compute_magnitude(col_names=["gyroscope_x", "gyroscope_y", "gyroscope_z"], magnitude_col_name="gyro_magnitude")

# join accel and gyro streams
ds_ag = ds_accel_magnitude.join(ds_gyro_magnitude, on=['user', 'timestamp', 'localtime', 'version'], how='full').dropna()

# get orientation
ds_ag_orientation = get_orientation_data(ds_ag,wrist="left")

## apply complementary filter
ds_ag_complemtary_filtered = ds_ag_orientation.complementary_filter()

# get brushing candidate groups
ds_ag_candidates = get_candidates(ds_ag_complemtary_filtered)

#ds_ag_candidates.show(1)
#remove where group==0 - non-candidates
ds_ag_candidates=ds_ag_candidates.filter(ds_ag_candidates.candidate==1)


## compute features
ds_fouriar_features=ds_ag_candidates.compute_fouriar_features(exclude_col_names=['group', 'candidate',"accel_magnitude","gyro_magnitude"], groupByColumnName=["group"])

ds_statistical_features = ds_ag_candidates.compute_statistical_features(exclude_col_names=['group','candidate',"accel_magnitude","gyro_magnitude"], groupByColumnName=["group"],feature_names = ['mean', 'median', 'stddev', 'skew',
                         'kurt', 'power', 'zero_cross_rate'])

ds_corr_mse_features = ds_ag_candidates.compute_corr_mse_accel_gyro(exclude_col_names=['group','candidate',"accel_magnitude","gyro_magnitude"], groupByColumnName=["group"])

ds_features = ds_fouriar_features\
    .join(ds_statistical_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'], how='full')\
    .join(ds_corr_mse_features, on=['user', 'timestamp', 'localtime', 'version', 'start_time', 'end_time'], how='full')

ds_features = ds_features.withColumn("duration", (ds_features.end_time.cast("long") - ds_features.start_time.cast("long")))

ds_features = get_max_features(ds_features)

ds_features = reorder_columns(ds_features)

ds_features._data.repartition(1).write.csv("/home/ali/IdeaProjects/MD2K_DATA/moral_parsed/features/user=820c/brushing.csv")
#pdf_features = ds_features.toPandas()

#pdf_predictions = classify_brushing(pdf_features,model_file_name="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/algorithms/brushing/model/AB_model_brushing_all_features.model")

#print(pdf_predictions)

#print(pdf_predictions)

