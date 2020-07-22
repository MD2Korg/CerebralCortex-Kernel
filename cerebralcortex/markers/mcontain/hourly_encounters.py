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


from datetime import datetime, timedelta

import pandas as pd
from dateutil import parser as dateparser
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, TimestampType, IntegerType, ArrayType

from cerebralcortex.algorithms.bluetooth.encounter import bluetooth_encounter, remove_duplicate_encounters
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import DataDescriptor, ModuleMetadata
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


def generate_metadata_hourly():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--visualization-stats--time-window').set_description('Computes visualization stats every time window defined by start time and end time') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the time window localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("latitude").set_type("double").set_attribute("description", \
                                                                               "Latitude of centroid location, a gps cluster output grouping encounters in similar location together")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("longitude").set_type("double").set_attribute("description", \
                                                                                "Longitude of centroid location, a gps cluster output grouping encounters in similar location together")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("n_users").set_type("integer").set_attribute("description", \
                                                                               "Number of unique users in that cluster centroid")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("total_encounters").set_type("double").set_attribute("description", \
                                                                                       "Total encounters happening in the time window in this specific location")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("normalized_total_encounters").set_type("double").set_attribute("description", \
                                                                                                  "Total encounters normalized by the centroid area. (encounters per 10 square meter)")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("avg_encounters").set_type("double").set_attribute("description", \
                                                                                     "average encounter per participant(participants who had at least one encounter)"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Visualization stats computation in a time window between start time and end time') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata


def generate_metadata_encounter():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k-encounter--bluetooth-gps').set_description('Contains each unique encounters between two persons along with the location of encounter') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the encounter in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the encounter in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("participant_identifier").set_type("string").set_attribute("description", \
                                                                                             "Participant with whom encounter happened")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("os").set_type("string").set_attribute("description", \
                                                                         "Operating system of the phone belonging to user")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("latitude").set_type("double").set_attribute("description", \
                                                                               "Latitude of encounter location")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("longitude").set_type("double").set_attribute("description", \
                                                                                "Longitude of encounter location")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("distances").set_type("array").set_attribute("description", \
                                                                               "Mean distance between participants in encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("distance_mean").set_type("array").set_attribute("description", \
                                                                                   "Mean distance in the encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("distance_std").set_type("array").set_attribute("description", \
                                                                                  "Standard deviation of distances in encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("distance_count").set_type("array").set_attribute("description", \
                                                                                    "Number of distances in encounter less than distance threshold")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("average_count").set_type("double").set_attribute("description", \
                                                                                    "Average count of values received in phone per minute - average across the encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("covid").set_type("integer").set_attribute("description", \
                                                                             "0, 1 or 2 indicating if this encounter contained a covid user -- 0 - no covid-19 affected, 1 - user is, 2 - participant identifier is"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Encounter computation after parsing raw bluetooth-gps data, clustering gps locations and removing double counting') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata


def drop_centroid_columns(data_result, centroid_present=True):
    if centroid_present:
        columns = ['centroid_id',
                   'centroid_latitude',
                   'centroid_longitude',
                   'centroid_area']
        for c in columns:
            if c in data_result.columns:
                data_result = data_result.drop(*[c])
    return data_result



def get_utcoffset():
    import time
    ts = time.time()
    utc_offset = (datetime.utcfromtimestamp(ts) -
                  datetime.fromtimestamp(ts)).total_seconds()/3600
    return utc_offset

def get_key_stream(data_key_stream,start_time,end_time,datetime_format = '%Y-%m-%d %H:%m'):
    st = dateparser.parse(start_time)
    st = st + timedelta(hours=int(get_utcoffset())) - timedelta(hours=50)
    start_time = st.strftime(datetime_format) ## get the pyspark format
    start_time = start_time[:-2] + '00'
    end_time = st + timedelta(hours=70) ## get the end time boundary
    end_time = end_time.strftime(datetime_format) ## get the pyspark format
    end_time = end_time[:-2] + '00'
    data_key_stream = data_key_stream.filter((data_key_stream.timestamp>=F.lit(start_time)) & (data_key_stream.timestamp<F.lit(end_time)))
    #     data_key_stream.sort(F.col('timestamp').desc()).show(100,False)
    return data_key_stream


def groupby_final(data_key_stream):
    #     schema = StructType([StructField('major-minor', StringType()),
    #                          StructField('participant_identifier', StringType()),
    #                          StructField('os', StringType())
    #                          ])
    #     @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    #     def count_encounters(df):
    #         if df.shape[0]==1:
    #             return pd.DataFrame([[]],columns = ['version','latitude','longitude','n_users',
    #                                               'total_encounters','avg_encounters','timestamp','localtime'])

    #     data_key_stream.groupBy('major-minor').apply(get_single_key)
    return data_key_stream.select('major-minor','participant_identifier','os')


def match_keys(base_encounters,data_key_stream):
    #     data_key_stream = get_key_stream(data_key_stream,start_time,end_time,datetime_format = '%Y-%m-%d %H:%m').drop('timestamp','localtime')
    base_encounters = base_encounters.withColumn('major-minor',F.concat('major','minor'))
    major_minor_list = list(base_encounters.select('major-minor').distinct().toPandas()['major-minor'])
    #     print(major_minor_list)
    data_key_stream = data_key_stream.withColumn('major-minor',F.concat('major','minor')).filter(F.col('major-minor').isin(major_minor_list))
    #     exprs = [F.collect_set(colName).alias(colName) for colName in ['participant_identifier','os']]
    #     key_stream_grouped = data_key_stream.groupBy('major-minor').agg(*exprs).select('participant_identifier','os','major-minor')
    key_stream_grouped = groupby_final(data_key_stream)
    #     key_stream_grouped = key_stream_grouped.withColumn('participant_identifier',F.lit('2e182487-e171-3191-be85-b15cbd96b77e'))
    #     key_stream_grouped.show(5,False)
    base_encounters = base_encounters.join(DataStream(data=key_stream_grouped,metadata=Metadata()),on=['major-minor'],how='left').drop(*['major-minor']).dropna()
    #     base_encounters.show(100,False)
    #     print(base_encounters.columns)
    return base_encounters

def combine_base_encounters(base_encounters,time_threshold=10*60):
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
                         StructField('participant_identifier',StringType()),
                         StructField('os',StringType())])
    columns = [a.name for a in schema.fields]
    #     print(columns)
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_enconters(data):
        if data.shape[0]==1:
            if (pd.Timestamp(data['end_time'].values[0]) - pd.Timestamp(data['start_time'].values[0])).total_seconds()<time_threshold:
                return pd.DataFrame([],columns=columns)
            return data[columns]
        data = data.sort_values('start_time').reset_index(drop=True)
        ts = data['timestamp'].astype('datetime64[ns]').quantile(.5)
        local_ts = data['localtime'].astype('datetime64[ns]').quantile(.5)
        st = data['start_time'].min()
        et = data['end_time'].max()
        if (pd.Timestamp(et) - pd.Timestamp(st)).total_seconds()<time_threshold:
            return pd.DataFrame([],columns=columns)
        user = data['user'].values[0]
        version = 1
        latitude = data['latitude'].mean()
        longitude = data['longitude'].mean()
        distances = []
        for i,row in data.iterrows():
            distances.extend(list(row['distances']))
        average_count = data['average_count'].mean()
        os = data['os'].values[0]
        participant_identifier = data['participant_identifier'].values[0]
        return pd.DataFrame([[ts,local_ts,st,et,user,version,latitude,distances,longitude,average_count,participant_identifier,os]],columns=columns)
    data_result = base_encounters.groupBy(['user','participant_identifier']).apply(get_enconters)
    return DataStream(data=data_result, metadata=Metadata())


def compute_encounters_only_v4(data_all_v4,data_key_stream,start_time,end_time,ltime=True):
    base_encounters = bluetooth_encounter(data_all_v4,start_time,end_time,ltime=ltime,n_rows_threshold=1,time_threshold=1)
    base_encounters = match_keys(base_encounters,data_key_stream)
    base_encounters = combine_base_encounters(base_encounters.drop('major','minor'))
    #     print('finalized')
    #     base_encounters.show(100,False)
    return base_encounters

def compute_encounters(data_all_v3,data_all_v4,data_map_stream,data_key_stream,start_time,end_time,ltime=True):
    #     print('Doing V3 now')
    data_encounter_v3 = bluetooth_encounter(data_all_v3,start_time,end_time,ltime=ltime)
    data_encounter_v3 = data_encounter_v3.join(data_map_stream,on=['major','minor'],how='left').drop(*['major','minor']).dropna()
    #     print(data_encounter_v3.count(),'encounters computed for version 3',data_encounter_v3.columns)
    #     data_encounter_v3.show(10,False)
    #     print('Doing V4 now')
    data_encounter_v4 = compute_encounters_only_v4(data_all_v4,data_key_stream,start_time,end_time,ltime=True)
    data_encounter = data_encounter_v3.unionByName(data_encounter_v4)
    #     data_encounter.show(100,False)
    print(data_encounter.count(),'total encounters')
    data_clustered = cluster_gps(data_encounter,minimum_points_in_cluster=1,geo_fence_distance=50)
    #     # data_clustered.drop(*['distances','centroid_longitude','centroid_latitude','centroid_id','centroid_area']).show(20,False)
    # #     print(data_encounter.count(),'clusters computed')
    data_result = remove_duplicate_encounters(data_clustered)
    #     data_result.drop(*['distances','centroid_longitude','centroid_latitude','centroid_id','centroid_area']).show(20,False)
    return data_result


def transform_beacon_data_columns(data_all):
    data_all = data_all.withColumnRenamed('avg_rssi','RSSI').withColumnRenamed('avg_distance','distance_estimate')
    data_all = data_all.withColumn('distance_estimate',F.col('distance_estimate').cast('double'))
    data_all = data_all.withColumn('minor',F.col('minor').cast('long')).withColumn('major',F.col('major').cast('long'))
    return data_all


def generate_visualization_hourly(data_all_v3,data_all_v4,data_map_stream,data_key_stream,start_time,end_time,ltime=True):
    data_all_v3 = transform_beacon_data_columns(data_all_v3)
    data_all_v4 = transform_beacon_data_columns(data_all_v4)
    if 'participant_identifier' in data_map_stream.columns:
        data_map_stream = data_map_stream.drop(*['participant_identifier'])
    data_map_stream = data_map_stream.withColumnRenamed('user','participant_identifier')
    data_map_stream = data_map_stream.select(*['participant_identifier','os','major','minor']).dropDuplicates()
    data_key_stream = data_key_stream.withColumnRenamed('user','participant_identifier').select(*['participant_identifier','timestamp','localtime','major','minor']).withColumn('os',F.lit('android'))

    unique_encounters = compute_encounters(data_all_v3,data_all_v4,data_map_stream,data_key_stream,start_time=start_time,end_time=end_time,ltime=ltime)
    return unique_encounters