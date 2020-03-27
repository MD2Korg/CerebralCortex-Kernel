# Copyright (c) 2019, MD2K Center of Excellence
# - Md Azim Ullah <mullah@memphis.edu>
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


from pyspark.sql.types import StructField, StructType, DoubleType,StringType, TimestampType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
import pandas as pd, numpy as np
from datetime import datetime


def bluetooth_encounter(data,
                        st:datetime,
                        et:datetime,
                        distance_threshold=5,
                        average_count_threshold = 20,
                        n_rows_threshold = 2,
                        time_threshold=5*60,
                        epsilon = 1e-3):
    """

    :param ds: Input Datastream
    :param st: Start Time the time window in UTC
    :param et: End Time of time window in UTC
    :param distance_threshold: Threshold on mean distance per encounter
    :param n_rows_threshold: No of rows per group/encounter
    :param time_threshold: Minimum Duration of time per encounter
    :param epsilon: A simple threshold
    :param count_threshold: Threshold on count
    :return: A Sparse representation of the Bluetooth Encounter
    """
    def get_data(data,i,k,distance_threshold):
        distances = data['distance_estimate'].values[k:i]
        rssi = data['RSSI'].values[k:i]
        weights1 = rssi  + np.abs(np.min(rssi)) + epsilon
        weights2 = 100*(distances-np.min(distances)) + epsilon
        weights = 1/(weights1+weights2)
        mean_distance = np.average(distances,weights=weights)
        if mean_distance <= distance_threshold:
            return [data['user'].iloc[k],data['participant_identifier'].iloc[k],data['localtime'].iloc[k],
                    data['localtime'].iloc[i-1],data['version'].iloc[k],mean_distance,data['timestamp'].iloc[int((i+k)/2)],
                    data['localtime'].iloc[int((i+k)/2)],np.mean(data['latitude'].values[k:i]),
                    np.mean(data['longitude'].values[k:i]),data['indicator'].iloc[k],np.mean(data['count'].values[k:i])]
        return []

    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('start_time', TimestampType()),
                         StructField('end_time', TimestampType()),
                         StructField('user', StringType()),
                         StructField('participant_identifier', StringType()),
                         StructField('version', IntegerType()),
                         StructField('indicator', StringType()),
                         StructField('latitude', DoubleType()),
                         StructField('mean_distance', DoubleType()),
                         StructField('longitude', DoubleType()),
                         StructField('average_count', DoubleType())
                         ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_enconters(data):
        if data.shape[0]<n_rows_threshold:
            return pd.DataFrame([],columns = ['user','participant_identifier','start_time','end_time','version',
                                              'mean_distance','timestamp','localtime','latitude','longitude','indicator','average_count'])
        data = data.sort_values('timestamp').reset_index(drop=True)
        data_all = []
        for i,row in data.iterrows():
            if i==0:
                k = int(i)
                start_time = row['time']
                count = 0
            else:
                if row['time'] - start_time < time_threshold/2:
                    start_time = row['time']
                    count+=1
                else:
                    if i-k>n_rows_threshold and data['time'].iloc[i]-data['time'].iloc[k]>time_threshold and np.mean(data['count'].values[k:i])>average_count_threshold:
                        temp = get_data(data,i,k,distance_threshold)
                        if len(temp)>0:
                            data_all.append(temp)
                    count = 0
                    k = i
                    start_time = row['time']
        if count>0:
            i = data.shape[0]-1
            temp = get_data(data,i,k,distance_threshold)
            if len(temp)>0:
                data_all.append(temp)

        return pd.DataFrame(data_all,columns = ['user','participant_identifier','start_time','end_time','version',
                                                'mean_distance','timestamp','localtime','latitude','longitude','indicator','average_count'])



    data = data.withColumn('time',F.col('timestamp').cast('double'))
    data_filtered = data._data.filter((data.timestamp>=st) & (data.timestamp<et))
    data_result = data_filtered.groupBy(['user','participant_identifier','version']).apply(get_enconters)
    return DataStream(data=data_result, metadata=Metadata())



def remove_duplicate_encounters(ds,
                                owner_name='user',
                                transmitter_name='participant_identifier',
                                start_time_name='start_time',
                                end_time_name='end_time',
                                centroid_id_name='centroid_id'):
    """

    :param ds: Input Datastream
    :param owner_name: Owner of the encounter (data being uploaded from whose phone)
    :param transmitter_name: Participant of the encounter alongside owner
    :param start_time_name: Start time of the encounter
    :param end_time_name: End time of the encounter
    :param centroid_id_name: Name of the Id column indicating centroid id
    :return:
    """
    schema = ds._data.schema
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def remove_duplicates(data):
        if len(np.intersect1d(data[owner_name].values,data[transmitter_name].values))==0:
            return data
        data = data.sort_values('start_time').reset_index(drop=True)
        not_visited = []
        for i,row in data.iterrows():
            if i in not_visited:
                continue
            temp_df = data[data[transmitter_name]==row[owner_name]]
            temp_df = temp_df[temp_df[start_time_name]>row[end_time_name]]
            temp_df = temp_df[row[end_time_name]>temp_df[start_time_name]]
            if temp_df.shape[0]==0:
                continue
            else:
                not_visited+=list(temp_df.index.values)
        return data[~data.index.isin(not_visited)]
    data = ds._data.groupBy([centroid_id_name,'version']).apply(remove_duplicates)
    return DataStream(data=data, metadata=Metadata())


