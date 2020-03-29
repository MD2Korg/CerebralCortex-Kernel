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


from pyspark.sql.types import StructField, StructType, DoubleType,StringType, TimestampType, IntegerType, ArrayType, BooleanType, LongType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
import pandas as pd, numpy as np
from datetime import datetime


def bluetooth_encounter(data,
                        st:datetime,
                        et:datetime,
                        distance_threshold=4,
                        n_rows_threshold = 3,
                        time_threshold=10*60,
                        localtime=True):
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
    def check_(arr,indexing,time_threshold):
        check = False
        for m in range(len(indexing)):
            if m==0:
                temp_arr = arr[indexing[m]:indexing[m+1]]
            elif m<len(indexing)-1:
                temp_arr = arr[(indexing[m]+1):indexing[m+1]]
            else:
                temp_arr = arr[(indexing[m]+1):]
            if len(temp_arr)>n_rows_threshold and temp_arr[-1] - temp_arr[0]>=time_threshold:
                check = True
        return check

    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('start_time', TimestampType()),
                         StructField('end_time', TimestampType()),
                         StructField('user', StringType()),
                         StructField('participant_identifier', StringType()),
                         StructField('version', IntegerType()),
                         StructField('os', StringType()),
                         StructField('latitude', DoubleType()),
                         StructField('distances', ArrayType(DoubleType())),
                         StructField('longitude', DoubleType()),
                         StructField('average_count', DoubleType()),
                         ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_enconters(data):
        if data.shape[0]<n_rows_threshold:
            return pd.DataFrame([],columns = ['user','participant_identifier','start_time','end_time','version',
                                              'distances','timestamp','localtime','latitude','longitude','os','average_count'])
        data = data.sort_values('timestamp').reset_index(drop=True)
        data_all = []
        for i,row in data.iterrows():
            if i==0:
                k = int(i)
                start_time = row['time']
                count = 0
                indexing = [0]
                arr = []
            else:
                if row['time'] - start_time < time_threshold/2:
                    start_time = row['time']
                    if row['distance_estimate']-distance_threshold>0:
                        indexing.append(i-k)
                    arr.append(row['time'])
                    count+=1
                else:
                    if i-k>n_rows_threshold and data['time'].iloc[i]-data['time'].iloc[k]>=time_threshold:
                        if len(indexing)<=1:
                            check = True
                        else:
                            check = check_(arr,indexing,time_threshold)
                        if check:
                            data_all.append([data['user'].iloc[k],data['participant_identifier'].iloc[k],data['localtime'].iloc[k],
                                             data['localtime'].iloc[i-1],data['version'].iloc[k],data['distance_estimate'].iloc[k:i].values,data['timestamp'].iloc[int((i+k)/2)],
                                             data['localtime'].iloc[int((i+k)/2)],np.mean(data['latitude'].values[k:i]),
                                             np.mean(data['longitude'].values[k:i]),data['os'].iloc[k],np.mean(data['count'].values[k:i])])
                    count = 0
                    k = i
                    start_time = row['time']
                    indexing = [0]
                    arr = []
        if count>0:
            i = data.shape[0]-1
            if i-k>n_rows_threshold and data['time'].iloc[i]-data['time'].iloc[k]>time_threshold:
                if len(indexing)<=1:
                    check = True
                else:
                    check = check_(arr,indexing,time_threshold)
                if check:
                    data_all.append([data['user'].iloc[k],data['participant_identifier'].iloc[k],data['localtime'].iloc[k],
                                     data['localtime'].iloc[i-1],data['version'].iloc[k],data['distance_estimate'].iloc[k:i].values,data['timestamp'].iloc[int((i+k)/2)],
                                     data['localtime'].iloc[int((i+k)/2)],np.mean(data['latitude'].values[k:i]),
                                     np.mean(data['longitude'].values[k:i]),data['os'].iloc[k],np.mean(data['count'].values[k:i])])

        return pd.DataFrame(data_all,columns = ['user','participant_identifier','start_time','end_time','version',
                                                'distances','timestamp','localtime','latitude','longitude','os','average_count'])



    data = data.withColumn('time',F.col('timestamp').cast('double'))
    if localtime:
        data_filtered = data._data.filter((data.localtime>=st) & (data.localtime<et))
    else:
        data_filtered = data._data.filter((data.timestamp>=st) & (data.timestamp<et))
    data_result = data_filtered.groupBy(['user','participant_identifier','version']).apply(get_enconters)
    return DataStream(data=data_result, metadata=Metadata())



def remove_duplicate_encounters(ds,
                                owner_name='user',
                                transmitter_name='participant_identifier',
                                start_time_name='start_time',
                                end_time_name='end_time',
                                centroid_id_name='centroid_id'):
    schema = ds._data.schema
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def remove_duplicates(data):
        if len(np.intersect1d(data[owner_name].values,data[transmitter_name].values))==0:
            data[start_time_name] = pd.to_datetime(data[start_time_name],unit='s')
            data[end_time_name] = pd.to_datetime(data[end_time_name],unit='s')
            return data
        data = data.sort_values(start_time_name).reset_index(drop=True)
        not_visited = []
        for i,row in data.iterrows():
            if i in not_visited:
                continue
            temp_df = data[data.participant_identifier.isin([row[owner_name]]) & data.user.isin([row[transmitter_name]]) & (~((data.start_time>=row[end_time_name]) | (data.end_time<row[start_time_name])))]
            if temp_df.shape[0]==0:
                continue
            else:
                not_visited+=list(temp_df.index.values)
        data =  data[~data.index.isin(not_visited)]
        data[start_time_name] = pd.to_datetime(data[start_time_name],unit='s')
        data[end_time_name] = pd.to_datetime(data[end_time_name],unit='s')
        return data

    ds = ds._data.withColumn(start_time_name,F.col(start_time_name).cast('double')).withColumn(end_time_name,F.col(end_time_name).cast('double'))
    data = ds.groupBy([centroid_id_name,'version']).apply(remove_duplicates)
    data = data.withColumn('covid',F.lit(0))
    return DataStream(data=data, metadata=Metadata())


def count_encounters_per_cluster(ds):
    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('version', IntegerType()),
                         StructField('latitude', DoubleType()),
                         StructField('longitude', DoubleType()),
                         StructField('n_users', IntegerType()),
                         StructField('total_encounters', DoubleType()),
                         StructField('avg_encounters', DoubleType()),
                         ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def count_encounters(data):
        if data.shape[0]==0:
            return pd.DataFrame([],columns = ['version','latitude','longitude','n_users',
                                              'total_encounters','avg_encounters','timestamp','localtime'])
        data = data.sort_values('localtime').reset_index(drop=True)
        centroid_id = data['centroid_id'].iloc[0]
        centroid_latitude = data['centroid_latitude'].iloc[0]
        centroid_longitude = data['centroid_longitude'].iloc[0]
        unique_users = np.unique(list(data['user'].unique())+list(data['participant_identifier'].unique()))
        data['count'] = 1
        total_encounters = data.groupby('user',as_index=False).sum()['count'].sum() + data.groupby('participant_identifier',as_index=False).sum()['count'].sum()
        average_encounter = (total_encounters)/len(unique_users)
        total_encounters = data.shape[0]
        timestamp = data['timestamp'].iloc[data.shape[0]//2]
        localtime = data['localtime'].iloc[data.shape[0]//2]
        version = data['version'].iloc[0]
        return pd.DataFrame([[version,centroid_latitude,centroid_longitude,len(unique_users),total_encounters,average_encounter,timestamp,localtime]],
                            columns = ['version','latitude','longitude','n_users',
                                       'total_encounters','avg_encounters','timestamp','localtime'])
    data = ds._data.groupBy(['centroid_id','version']).apply(count_encounters)

    return DataStream(data=data, metadata=Metadata())


def get_notification_messages(ds, day, day_offset=5):
    """

    :param ds: Input Datastream
    :param day: test date as datetime object
    :param day_offset: number of days to be considered before the test day
    :return:
    """


    ds = ds.filter(F.udf(lambda x: datetime(x.year, x.month, x.day+day_offset) >= day, BooleanType())(F.col('timestamp')))
    ds1 = ds.filter('covid==1').select(F.col('participant_identifier').alias('user'), F.col('timestamp'), F.col('localtime'))
    ds2 = ds.filter('covid==2').select(F.col('user'), F.col('timestamp'), F.col('localtime'))
    merged_ds = ds1.union(ds2)
    notif = merged_ds.withColumn('day', F.udf(lambda x: datetime(x.year, x.month, x.day), TimestampType())(F.col('localtime'))) \
        .withColumn('message', F.udf(lambda x: 'On '+x.strftime('%B %d, %Y')+' you were in close proximity of a COVID-19 positive individual for 10 minutes or longer', StringType())('localtime')) \
        .withColumn('time_offset', F.col('localtime').cast(LongType())-F.col('timestamp').cast(LongType())).drop('timestamp').drop_duplicates().withColumn('timestamp', F.lit(F.current_timestamp())).drop('localtime') \
        .withColumn('localtime', (F.col('timestamp').cast(LongType())+F.col('time_offset')).cast(TimestampType())).withColumn('version', F.lit(1)).drop('time_offset')
    return notif


def get_user_encounter_count(ds, user_id, start_time, end_time):
    """

    :param ds: Input Datastream
    :param user_id: the user id of the participant whom the encounters are to be calculated
    :param start_time: start time (datetime object) of the calculation
    :param end_time: end time of the calculation
    :return:
    """
    ds = ds.filter((ds.localtime >= start_time) & (ds.localtime <= end_time))
    ds = ds.filter((ds.user == user_id) | (ds.participant_identifier == user_id))
    cnt = ds.count()
    ds = ds.limit(1).select(ds.timestamp, ds.localtime, ds.user, ds.version).withColumn('encounter_count', F.lit(cnt))
    ds = ds.withColumn('time_offset', F.col('localtime').cast(LongType())-F.col('timestamp').cast(LongType()))\
        .drop('timestamp', 'localtime').drop_duplicates().withColumn('timestamp', F.lit(F.current_timestamp())) \
        .withColumn('localtime', (F.col('timestamp').cast(LongType())+F.col('time_offset')).cast(TimestampType()))\
        .drop('time_offset').withColumn('start_time', F.lit(start_time)).withColumn('end_time', F.lit('end_time'))
    return ds





