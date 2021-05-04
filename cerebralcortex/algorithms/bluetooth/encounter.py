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


from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, TimestampType, IntegerType, ArrayType, \
    BooleanType, LongType

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def bluetooth_encounter(data,
                        st:datetime,
                        et:datetime,
                        distance_threshold=12,
                        n_rows_threshold = 8,
                        time_threshold=10*60,
                        ltime=True):
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
    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('start_time', TimestampType()),
                         StructField('end_time', TimestampType()),
                         StructField('user', StringType()),
                         StructField('version', IntegerType()),
                         StructField('latitude', DoubleType()),
                         StructField('distances', ArrayType(DoubleType())),
                         StructField('longitude', DoubleType()),
                         StructField('average_count', DoubleType()),
                         StructField('major',LongType()),
                         StructField('minor',LongType())])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_enconters(data):
        if data.shape[0]<n_rows_threshold:
            return pd.DataFrame([],columns = ['user','major','minor','start_time','end_time','version',
                                              'distances','timestamp','localtime','latitude','longitude','average_count'])
        data = data.sort_values('time').reset_index(drop=True)
        data_filtered = data[data.distance_estimate<distance_threshold]
        #         data_filtered = data
        if data_filtered.shape[0]<n_rows_threshold or data_filtered['time'].max() - data_filtered['time'].min()<time_threshold:
            return pd.DataFrame([],columns = ['user','major','minor','start_time','end_time','version',
                                              'distances','timestamp','localtime','latitude','longitude','average_count'])
        else:
            data_all = []
            #             data = data_filtered
            k = 0
            i = data.shape[0]
            c = 'localtime'
            data_all.append([data_filtered['user'].iloc[k],data['major'].iloc[k],data['minor'].iloc[k],data[c].iloc[k],
                             data[c].iloc[i-1],data['version'].iloc[k],data['distance_estimate'].iloc[k:i].values,data['timestamp'].iloc[int((i+k)/2)],
                             data['localtime'].iloc[int((i+k)/2)],np.mean(data['latitude'].values[k:i]),
                             np.mean(data['longitude'].values[k:i]),np.mean(data['count'].values[k:i])])
            return pd.DataFrame(data_all,columns = ['user','major','minor','start_time','end_time','version',
                                                    'distances','timestamp','localtime','latitude','longitude','average_count'])

    #     print(st,et)
    data = data.withColumn('time',F.col('timestamp').cast('double'))
    #     data.show(100,False)
    #     print('--'*40)
    if ltime:
        data_filtered = data.filter((data.localtime>=F.lit(st)) & (data.localtime<F.lit(et)))
    else:
        data_filtered = data.filter((data.timestamp>=F.lit(st)) & (data.timestamp<F.lit(et)))
    #     data.show(100,False)
    #     print(data_filtered.count(),'filtered data count')
    data_filtered = data_filtered.filter(data_filtered.longitude!=200)
    data_result = data_filtered.groupBy(['user','major','minor','version']).apply(get_enconters)
    #     data_filtered.sort('timestamp').show(1000,False)
    # print(data_result.count(),'encounter count')
    #     data_result.show(5,False)
    return DataStream(data=data_result, metadata=Metadata())

def remove_duplicate_encounters(ds,
                                owner_name='user',
                                transmitter_name='participant_identifier',
                                start_time_name='start_time',
                                end_time_name='end_time',
                                centroid_id_name='centroid_id',
                                distance_threshold=12):


    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('start_time', DoubleType()),
                         StructField('end_time', DoubleType()),
                         StructField('user', StringType()),
                         StructField('participant_identifier', StringType()),
                         StructField('version', IntegerType()),
                         StructField('os', StringType()),
                         StructField('latitude', DoubleType()),
                         StructField('distances', ArrayType(DoubleType())),
                         StructField('longitude', DoubleType()),
                         StructField('average_count', DoubleType()),
                         StructField('centroid_longitude', DoubleType()),
                         StructField('centroid_latitude', DoubleType()),
                         StructField('centroid_id', IntegerType()),
                         StructField('centroid_area', DoubleType()),
                         StructField('distance_mean', DoubleType()),
                         StructField('distance_std', DoubleType()),
                         StructField('distance_count', DoubleType())
                         ])
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def remove_duplicates(data):
        data['distance_mean'] = data['distances'].apply(lambda a:np.mean(a))
        data['distance_std'] = data['distances'].apply(lambda a:np.std(a))
        data['distance_count'] = data['distances'].apply(lambda a:len(np.array(a)[np.array(a)<distance_threshold]))
        if data.shape[0]<2:
            return data
        if len(np.intersect1d(data[owner_name].values,data[transmitter_name].values))==0:
            return data
        data = data.sort_values('distance_mean').reset_index(drop=True)
        not_visited = []
        for i,row in data.iterrows():
            if i in not_visited:
                continue
            temp_df = data[data.participant_identifier.isin([row[owner_name],row[transmitter_name]]) & data.user.isin([row[owner_name],row[transmitter_name]])]
            if temp_df.shape[0]==0:
                continue
            else:
                indexes = [u for u in list(temp_df.index.values) if u!=i]
                not_visited+=indexes
        data =  data[~data.index.isin(not_visited)]
        return data

    ds = ds.withColumn(start_time_name,F.col(start_time_name).cast('double')).withColumn(end_time_name,F.col(end_time_name).cast('double'))
    data = ds.groupBy([centroid_id_name,'version']).apply(remove_duplicates)
    data = data.withColumn(start_time_name,F.col(start_time_name).cast('timestamp')).withColumn(end_time_name,F.col(end_time_name).cast('timestamp'))
    data = data.withColumn('covid',F.lit(0))
    return DataStream(data=data, metadata=Metadata())


def count_encounters_per_cluster(ds,multiplier=10):
    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('version', IntegerType()),
                         StructField('latitude', DoubleType()),
                         StructField('longitude', DoubleType()),
                         StructField('n_users', IntegerType()),
                         StructField('total_encounters', DoubleType()),
                         StructField('avg_encounters', DoubleType()),
                         StructField('normalized_total_encounters', DoubleType())
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
        normalized_total_encounters = total_encounters*multiplier/data['centroid_area'].iloc[0]
        timestamp = data['timestamp'].iloc[data.shape[0]//2]
        localtime = data['localtime'].iloc[data.shape[0]//2]
        version = data['version'].iloc[0]
        return pd.DataFrame([[normalized_total_encounters,version,centroid_latitude,centroid_longitude,len(unique_users),
                              total_encounters,average_encounter,timestamp,localtime]],
                            columns = ['normalized_total_encounters','version','latitude','longitude','n_users',
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
    merged_ds = ds1.unionByName(ds2)
    notif = merged_ds.withColumn('day', F.udf(lambda x: datetime(x.year, x.month, x.day), TimestampType())(F.col('localtime'))) \
        .withColumn('message', F.udf(lambda x: 'On '+x.strftime('%B %d, %Y')+' you were in close proximity of a COVID-19 positive individual for 10 minutes or longer', StringType())('localtime')) \
        .withColumn('time_offset', F.col('localtime').cast(LongType())-F.col('timestamp').cast(LongType())).drop('timestamp').drop_duplicates().withColumn('timestamp', F.lit(F.current_timestamp())).drop('localtime') \
        .withColumn('localtime', (F.col('timestamp').cast(LongType())+F.col('time_offset')).cast(TimestampType())).withColumn('version', F.lit(1)).drop('time_offset')
    return notif


def get_encounter_count_all_user(data_ds, user_list_ds, start_time, end_time):
    data_ds = data_ds.filter((data_ds.localtime>=start_time)&(data_ds.localtime<=end_time))
    pdf = user_list_ds.toPandas()
    cnt_ds = None
    for idx, row in pdf.iterrows():
        user_id = row['user']
        ds = data_ds.filter((data_ds.user==user_id)|(data_ds.participant_identifier==user_id))
        cnt = ds.count()
        if cnt != 0:
            ds = ds.limit(1).select(ds.timestamp, ds.localtime, ds.user, ds.version).withColumn('encounter_count', F.lit(cnt))
            ds = ds.withColumn('time_offset', F.col('localtime').cast(DoubleType())-F.col('timestamp').cast(DoubleType())).drop('timestamp', 'localtime').drop_duplicates().withColumn('timestamp', F.lit(F.current_timestamp())) \
                .withColumn('localtime', (F.col('timestamp').cast(DoubleType())+F.col('time_offset')).cast(TimestampType())).drop('time_offset').withColumn('start_time', F.lit(start_time)).withColumn('end_time', F.lit(end_time))
            if cnt_ds is None:
                cnt_ds = ds
            else:
                cnt_ds = cnt_ds.unionByName(ds)

    if cnt_ds is None:
        tz = pytz.timezone('US/Central')
        tz_utc = pytz.timezone('UTC')
        tz_d = datetime.now(tz).replace(tzinfo=None)
        u_d = datetime.now(tz_utc).replace(tzinfo=None)
        time_offset = (u_d - tz_d).seconds
        no_cnt_ds = user_list_ds.select('user')
    else:
        time_offset = cnt_ds.limit(1).select((F.col('timestamp').cast(DoubleType())-F.col('localtime').cast(DoubleType())).alias('time_diff')).collect()[0].asDict()['time_diff']
        no_cnt_ds = user_list_ds.select('user').subtract(cnt_ds.select('user'))

    no_cnt_ds = no_cnt_ds.withColumn('start_time', F.lit(start_time)).withColumn('end_time', F.lit(end_time)).withColumn('localtime', F.lit(start_time)) \
        .withColumn('timestamp', (F.col('localtime').cast(DoubleType())+F.lit(time_offset)).cast(TimestampType())).withColumn('version', F.lit(1)) \
        .withColumn('encounter_count', F.lit(0))
    if cnt_ds is None:
        return no_cnt_ds
    return cnt_ds.unionByName(no_cnt_ds)