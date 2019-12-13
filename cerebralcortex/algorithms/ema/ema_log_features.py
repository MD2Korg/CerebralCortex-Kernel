# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

import numpy as np
import pandas as pd
import json
from geopy.distance import great_circle
from pyspark.sql.functions import pandas_udf, PandasUDFType
from shapely.geometry.multipoint import MultiPoint
from sklearn.cluster import DBSCAN
from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType,DateType,TimestampType
from cerebralcortex.algorithms.ema.ema_incentive_features import get_ema_incentive_features
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("localtime", TimestampType()),
    StructField("user", StringType()),
    StructField("version", IntegerType()),
    StructField("status", StringType()),
    StructField("ema_id", StringType()),
    StructField("schedule_timestamp", TimestampType()),
    StructField("operation", StringType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def get_ema_log_features(user_data):
    all_vals = []
    for index, row in user_data.iterrows():
        ema = row["log"]
        if not isinstance(ema, dict):
            ema = json.loads(ema)

        operation = ema["operation"].lower()
        if operation !="condition":
            status = ema.get("status","")
            ema_id = ema["id"]
            schedule_timestamp = ema.get("logSchedule",{}).get("scheduleTimestamp")
            if schedule_timestamp:
                schedule_timestamp = pd.to_datetime(schedule_timestamp, unit='ms')



            all_vals.append([row["timestamp"],row["localtime"], row["user"],1,status,ema_id,schedule_timestamp,operation])

    return pd.DataFrame(all_vals,columns=['timestamp','localtime', 'user', 'version','status','ema_id','schedule_timestamp','operation'])

