from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
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
import pickle
import pandas as pd
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



def get_orientation_data(ds, wrist, ori=1, is_new_device=False,
                         accelerometer_x="accelerometer_x",accelerometer_y="accelerometer_y",accelerometer_z="accelerometer_z",
                         gyroscope_x="gyroscope_x",gyroscope_y="gyroscope_y",gyroscope_z="gyroscope_z"):
    left_ori = {"old": {0: [1, 1, 1], 1: [1, 1, 1], 2: [-1, -1, 1], 3: [-1, 1, 1], 4: [1, -1, 1]},
                "new": {0: [-1, 1, 1], 1: [-1, 1, 1], 2: [1, -1, 1], 3: [1, 1, 1], 4: [-1, -1, 1]}}
    right_ori = {"old": {0: [1, -1, 1], 1: [1, -1, 1], 2: [-1, 1, 1], 3: [-1, -1, 1], 4: [1, 1, 1]},
                 "new": {0: [1, 1, 1], 1: [1, 1, 1], 2: [-1, -1, 1], 3: [-1, 1, 1], 4: [1, -1, 1]}}
    if is_new_device:
        left_fac = left_ori.get("new").get(ori)
        right_fac = right_ori.get("new").get(ori)

    else:
        left_fac = left_ori.get("old").get(ori)
        right_fac = right_ori.get("old").get(ori)

    if wrist == "left":
        fac = left_fac
    elif wrist == "right":
        fac = right_fac
    else:
        raise Exception("wrist can only be left or right.")

    data = ds.withColumn(gyroscope_x, ds[gyroscope_x] * fac[0]) \
        .withColumn(gyroscope_y, ds[gyroscope_y] * fac[1]) \
        .withColumn(gyroscope_z, ds[gyroscope_z] * fac[2])\
        .withColumn(accelerometer_x, ds[accelerometer_x] * fac[0]) \
        .withColumn(accelerometer_y, ds[accelerometer_y] * fac[1]) \
        .withColumn(accelerometer_z, ds[accelerometer_z] * fac[2])

    return data


def get_candidates(ds, uper_limit:float=0.1, lower_limit:float=0.1, threshold:float=0.5):
    window = Window.partitionBy(["user", "version"]).rowsBetween(-3, 3).orderBy("timestamp")
    window2 = Window.orderBy("timestamp")

    @pandas_udf(IntegerType(), PandasUDFType.GROUPED_AGG)
    def generate_candidates(accel_y):
        accel_y[accel_y > uper_limit] = 1
        accel_y[accel_y <= lower_limit] = 0

        if accel_y.mean() >= threshold:
            return 1
        else:
            return 0

    df = ds.withColumn("candidate", generate_candidates(ds.accelerometer_y).over(window))

    df2 = df.withColumn(
        "userChange",
        (F.col("user") != F.lag("user").over(window2)).cast("int")
    ) \
        .withColumn(
        "candidateChange",
        (F.col("candidate") != F.lag("candidate").over(window2)).cast("int")
    ) \
        .fillna(
        0,
        subset=["userChange", "candidateChange"]
    ) \
        .withColumn(
        "indicator",
        (~((F.col("userChange") == 0) & (F.col("candidateChange") == 0))).cast("int")
    ) \
        .withColumn(
        "group",
        F.sum(F.col("indicator")).over(window2.rangeBetween(Window.unboundedPreceding, 0))
    ).drop("userChange").drop("candidateChange").drop("indicator")

    # df3=df2.groupBy("user", "group") \
    #     .agg(
    #     F.min("timestamp").alias("start_time"),
    #     F.max("timestamp").alias("end_time"),
    #     F.min("candidate").alias("candidate")
    # ) \
    #     .drop("group")

    return df2

def get_max_features(ds):
    return ds.withColumn("max_accl_mean",
                         F.greatest(ds.accelerometer_x_mean, ds.accelerometer_y_mean,
                                    ds.accelerometer_z_mean)) \
        .withColumn("max_accl_median",
                    F.greatest(ds.accelerometer_x_median, ds.accelerometer_y_median,
                               ds.accelerometer_z_median)) \
        .withColumn("max_accl_stddev",
                    F.greatest(ds.accelerometer_x_stddev, ds.accelerometer_y_stddev,
                               ds.accelerometer_z_stddev)) \
        .withColumn("max_accl_skew", F.greatest(ds.accelerometer_x_skew, ds.accelerometer_y_skew,
                                                ds.accelerometer_z_skew)) \
        .withColumn("max_accl_kurt", F.greatest(ds.accelerometer_x_kurt, ds.accelerometer_y_kurt,
                                                ds.accelerometer_z_kurt)) \
        .withColumn("max_accl_power", F.greatest(ds.accelerometer_x_power, ds.accelerometer_y_power,
                                                 ds.accelerometer_z_power)) \
        .withColumn("max_accl_zero_cross_rate",
                    F.greatest(ds.accelerometer_x_zero_cross_rate, ds.accelerometer_y_zero_cross_rate,
                               ds.accelerometer_z_zero_cross_rate)) \
        .withColumn("max_accl_fft_centroid",
                    F.greatest(ds.accelerometer_x_fft_centroid, ds.accelerometer_y_fft_centroid,
                               ds.accelerometer_z_fft_centroid)) \
        .withColumn("max_accl_fft_spread",
                    F.greatest(ds.accelerometer_x_fft_spread, ds.accelerometer_y_fft_spread,
                               ds.accelerometer_z_fft_spread)) \
        .withColumn("max_accl_spectral_entropy", F.greatest(ds.accelerometer_x_spectral_entropy,
                                                            ds.accelerometer_y_spectral_entropy,
                                                            ds.accelerometer_z_spectral_entropy)) \
        .withColumn("max_accl_spectral_entropy_old", F.greatest(ds.accelerometer_x_spectral_entropy_old,
                                                                ds.accelerometer_y_spectral_entropy_old,
                                                                ds.accelerometer_z_spectral_entropy_old)) \
        .withColumn("max_accl_fft_flux",
                    F.greatest(ds.accelerometer_x_fft_flux, ds.accelerometer_y_fft_flux,
                               ds.accelerometer_z_fft_flux)) \
        .withColumn("max_accl_spec_rolloff", F.greatest(ds.accelerometer_x_spectral_folloff,
                                                        ds.accelerometer_y_spectral_folloff,
                                                        ds.accelerometer_z_spectral_folloff))


def classify_brushing(X: pd.DataFrame,model_file_name:str):
    with open(model_file_name, 'rb') as handle:
        clf = pickle.load(handle)
    X=X.values
    X = X[:,4:]
    preds = clf.predict(X)

    return preds
