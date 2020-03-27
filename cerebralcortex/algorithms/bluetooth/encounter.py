# Copyright (c) 2019, MD2K Center of Excellence
# - Md Azim Ullah <aungkonazim@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, DoubleType,StringType,FloatType, TimestampType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
import pandas as pd, numpy as np, os, csv, glob


def get_bluetooth_encounter(data,
                            distance_threshold=5,
                            count_threshold = 5,
                            n_rows_threshold = 2,
                            time_threshold=5*60,
                            epsilon = 1e-3):
    """

    :param ds: Input Datastream
    :param distance_threshold: Threshold on mean distance per encounter
    :param n_rows_threshold: No of rows per group/encounter
    :param time_threshold: Minimum Duration of time per encounter
    :param epsilon: A simple threshold
    :param count_threshold: Threshold on count
    :return: A Sparse representation of the Bluetooth Encounter
    """

    data['time'] = data['time'].apply(lambda a:datetime.fromtimestamp(a))
    data['timestamp'] = data['time'].apply(lambda d:d.timestamp())
    data['hour'] = data['time'].apply(lambda d:d.hour)
    data['day'] = data['time'].apply(lambda d:d.strftime('%Y%m%d'))


