# Copyright (c) 2020, MD2K Center of Excellence
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

import json

import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.group import GroupedData
from pyspark.sql.types import StructField, StructType, StringType, FloatType, TimestampType, IntegerType

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def ema_incentive(ds):
    """
    Parse stream name 'incentive--org.md2k.ema_scheduler--phone'. Convert json column to multiple columns.

    Args:
        ds: Windowed/grouped DataStream object

    Returns:
        ds: Windowed/grouped DataStream object.
    """
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("localtime", TimestampType()),
        StructField("user", StringType()),
        StructField("version", IntegerType()),
        StructField("incentive", FloatType()),
        StructField("total_incentive", FloatType()),
        StructField("ema_id", StringType()),
        StructField("data_quality", FloatType())
    ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def parse_ema_incentive(user_data):
        all_vals = []
        for index, row in user_data.iterrows():
            ema = row["incentive"]
            if not isinstance(ema, dict):
                ema = json.loads(ema)

            incentive = ema["incentive"]
            total_incentive = ema["totalIncentive"]
            ema_id = ema["emaId"]
            data_quality = ema["dataQuality"]


            all_vals.append([row["timestamp"],row["localtime"], row["user"],1,incentive,total_incentive,ema_id,data_quality])

        return pd.DataFrame(all_vals,columns=['timestamp','localtime', 'user', 'version','incentive','total_incentive','ema_id','data_quality'])

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(parse_ema_incentive)
    return DataStream(data=data, metadata=Metadata())

def ema_logs(ds):
    """
    Convert json column to multiple columns.

    Args:
        ds (DataStream): Windowed/grouped DataStream object

    Returns:

    """
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
    def parse_ema_logs(user_data):
        all_vals = []
        for index, row in user_data.iterrows():
            ema = row["log"]
            if not isinstance(ema, dict):
                ema = json.loads(ema)

            operation = ema["operation"].lower()
            if operation != "condition":
                status = ema.get("status", "")
                ema_id = ema["id"]
                schedule_timestamp = ema.get("logSchedule", {}).get("scheduleTimestamp")
                if schedule_timestamp:
                    schedule_timestamp = pd.to_datetime(schedule_timestamp, unit='ms')

                all_vals.append(
                    [row["timestamp"], row["localtime"], row["user"], 1, status, ema_id, schedule_timestamp, operation])

        return pd.DataFrame(all_vals, columns=['timestamp', 'localtime', 'user', 'version', 'status', 'ema_id',
                                               'schedule_timestamp', 'operation'])

    # check if datastream object contains grouped type of DataFrame
    if not isinstance(ds._data, GroupedData):
        raise Exception(
            "DataStream object is not grouped data type. Please use 'window' operation on datastream object before running this algorithm")

    data = ds._data.apply(parse_ema_logs)
    return DataStream(data=data, metadata=Metadata())
