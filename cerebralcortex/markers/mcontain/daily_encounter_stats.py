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

import time
from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, TimestampType, IntegerType

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.markers.mcontain.hourly_encounters import *


def generate_metadata_dailystats():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--daily-stats').set_description('Daily stats for website') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the day in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the day in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("number_of_app_users").set_type("double").set_attribute("description", \
                                                                                          "Total number of app users")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("encounter_per_user").set_type("double").set_attribute("description", \
                                                                                         "Average encounter per user")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("total_covid_encounters").set_type("double").set_attribute("description", \
                                                                                             "Total covid encounters on the day")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("maximum_concurrent_encounters").set_type("double").set_attribute("description", \
                                                                                                    "Maximum concurrent encounters"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Daily encounter stats for all the users to be shown in website') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata

def generate_metadata_notif():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--user-notifications').set_description('Notification generated for the Covid-19 encountered users.') \
        .add_dataDescriptor(
        DataDescriptor().set_name("user").set_type("string").set_attribute("description", \
                                                                           "user id")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("timestamp").set_type("timestamp").set_attribute("description", \
                                                                                   "Unix timestamp when the message was generated")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("localtime").set_type("timestamp").set_attribute("description", \
                                                                                   "Local timestamp when the message was generated.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("message").set_type("string").set_attribute("description", \
                                                                              "Generated notification message")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("day").set_type("timestamp").set_attribute("description", \
                                                                             "day of the encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("version").set_type("int").set_attribute("description", \
                                                                           "version"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Generated notification for a user encountered with Covid-19 participant') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Shiplu Hawlader", "shiplu.cse.du@gmail.com").set_version(1))
    return stream_metadata

def generate_metadata_user_encounter_count():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--user--encounter-count').set_description('Number of encounter in a given time window') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("encounter_count").set_type("int").set_attribute("description", \
                                                                                   "Total number of encounter for the user in the given time window"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Total number of encounter for a user in a given time window') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Shiplu Hawlader, Md Azim Ullah", "shiplu.cse.du@gmail.com, mullah@memphis.edu").set_version(1))
    return stream_metadata

def generate_metadata_encounter_daily():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k-encounter-daily--bluetooth-gps').set_description('Contains each unique encounters between two persons along with the location of encounter') \
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
        DataDescriptor().set_name("durations").set_type("array").set_attribute("description", \
                                                                               "Mean distance between participants in encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("covid").set_type("integer").set_attribute("description", \
                                                                             "0, 1 or 2 indicating if this encounter contained a covid user -- 0 - no covid-19 affected, 1 - user is, 2 - participant identifier is"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Encounter computation after parsing raw bluetooth-gps data, clustering gps locations and removing double counting') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata

def generate_metadata_visualization_daily():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--visualization-stats--daily').set_description('Computes visualization stats every time window defined by start time and end time') \
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
        ModuleMetadata().set_name('Visualization stats computation in a day between start time and end time') \
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


def assign_covid_user(data,covid_users):
    if not isinstance(covid_users,list):
        covid_users = [covid_users]
    data = data.withColumn('covid',F.when(F.col('user').isin(covid_users), 1 ).when(F.col('participant_identifier').isin(covid_users), 2).otherwise(0))
    return data

def remove_duplicate_encounters_day(data):
    schema = StructType([StructField('timestamp', TimestampType()),
                         StructField('localtime', TimestampType()),
                         StructField('user', StringType()),
                         StructField('participant_identifier', StringType()),
                         StructField('version', IntegerType()),
                         StructField('os', StringType()),
                         StructField('latitude', DoubleType()),
                         StructField('longitude', DoubleType()),
                         StructField('durations',DoubleType()),
                         StructField('covid', IntegerType()),
                         StructField('centroid_id', IntegerType()),
                         StructField('centroid_latitude', DoubleType()),
                         StructField('centroid_longitude', DoubleType()),
                         StructField('centroid_area', DoubleType())

                         ])
    columns = [a.name for a in schema]
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def compute_daily_encounter_stream(data):
        data = data.sort_values('distance_mean').reset_index(drop=True)
        df_final = pd.DataFrame([],columns=columns)
        for i,row in data.iterrows():
            temp_temp = df_final[df_final.participant_identifier.isin([row['user'],row['participant_identifier']]) & df_final.user.isin([row['user'],row['participant_identifier']])]
            if temp_temp.shape[0]>0:
                continue
            temp_df = data[data.participant_identifier.isin([row['user'],row['participant_identifier']]) & data.user.isin([row['user'],row['participant_identifier']])]
            save_this = temp_df[:1].reset_index(drop=True)
            save_this['latitude'].iloc[0] = temp_df['latitude'].median()
            save_this['longitude'].iloc[0] = temp_df['longitude'].median()
            save_this['durations'] = 1
            save_this['durations'].iloc[0] = np.sum([row['end_time']-row['start_time'] for i,row in temp_df.iterrows()])/3600
            save_this.drop(columns=['start_time',
                                    'end_time',
                                    'distances',
                                    'average_count',
                                    'distance_mean',
                                    'distance_std',
                                    'distance_count'],axis=1,inplace=True)
            df_final = pd.concat([df_final,save_this])
        return df_final
    columns = [a.name for a in schema]
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def compute_daily_encounter_stream_v2(data):
        data = data.sort_values('durations').reset_index(drop=True)
        df_final = pd.DataFrame([],columns=columns)
        for i,row in data.iterrows():
            temp_temp = df_final[df_final.participant_identifier.isin([row['user'],row['participant_identifier']]) & df_final.user.isin([row['user'],row['participant_identifier']])]
            if temp_temp.shape[0]>0:
                continue
            temp_df = data[data.participant_identifier.isin([row['user'],row['participant_identifier']]) & data.user.isin([row['user'],row['participant_identifier']])]
            save_this = temp_df[:1].reset_index(drop=True)
            save_this['latitude'].iloc[0] = temp_df['latitude'].median()
            save_this['longitude'].iloc[0] = temp_df['longitude'].median()
            save_this['durations'].iloc[0] = temp_df['durations'].sum()
            df_final = pd.concat([df_final,save_this])
        return df_final
    data_gps = data.withColumn('start_time',F.col('start_time').cast('double')).withColumn('end_time',F.col('end_time').cast('double'))
    data_final = data_gps.groupBy(['version','user']).apply(compute_daily_encounter_stream)
    data_final = data_final.groupBy(['version']).apply(compute_daily_encounter_stream_v2)
    return DataStream(data=data_final,metadata=Metadata())

def get_notifications(encounter_final_data_with_gps,day,multiplier=10,column_name = 'total_encounters',metric_threshold=1):
    schema  = StructType(list([StructField('timestamp',TimestampType()),
                               StructField('localtime',TimestampType()),
                               StructField('start_time',TimestampType()),
                               StructField('end_time',TimestampType()),
                               StructField('participant_identifier',StringType()),
                               StructField('os',StringType()),
                               StructField('latitude',DoubleType()),
                               StructField('distances',ArrayType(DoubleType())),
                               StructField('longitude',DoubleType()),
                               StructField('average_count',DoubleType()),
                               StructField('distance_mean',DoubleType()),
                               StructField('distance_std',DoubleType()),
                               StructField('distance_count',DoubleType()),
                               StructField('covid',IntegerType()),
                               StructField('version',IntegerType()),
                               StructField('avg_encounters',DoubleType()),
                               StructField('unique_users',IntegerType()),
                               StructField('total_encounters',DoubleType()),
                               StructField('normalized_total_encounters',DoubleType()),
                               StructField('user',StringType()),
                               StructField('centroid_longitude',DoubleType()),
                               StructField('centroid_latitude',DoubleType()),
                               StructField('centroid_id',IntegerType()),
                               StructField('centroid_area',DoubleType())]))
    column_names = [a.name for a in schema.fields]
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def compute_cluster_metrics(data):
        if data.shape[0]==0:
            return pd.DataFrame([],columns=column_names)
        data = data.sort_values('start_time').reset_index(drop=True)
        unique_users = np.unique(list(data['user'].unique())+list(data['participant_identifier'].unique()))
        total_encounters = data.shape[0]
        average_encounter = (total_encounters*2)/len(unique_users)
        data['unique_users'] = unique_users.shape[0]
        data['avg_encounters'] = average_encounter
        data['total_encounters'] = total_encounters
        data['normalized_total_encounters'] = total_encounters*multiplier/data['centroid_area'].iloc[0]
        return data[column_names]
    encounter_final_data_with_gps = encounter_final_data_with_gps.filter(F.col('centroid_area')>1)
    encounter_final_data_with_gps = encounter_final_data_with_gps.withColumn('hour',F.hour('start_time'))
    encounter_personal_data = encounter_final_data_with_gps.groupBy(['centroid_id','version','hour']).apply(compute_cluster_metrics)
    drop_columns = ['os','latitude','distances',
                    'longitude','average_count',
                    'distance_mean','distance_std',
                    'distance_count','covid']
    encounter_personal_data_filtered = encounter_personal_data.filter(F.col(column_name)>=metric_threshold).drop(*drop_columns)

    encounter_personal_data_filtered_p1 = encounter_personal_data_filtered.withColumn('user_temp',
                                                                                      F.col('user')).withColumn('user',
                                                                                                                F.col('participant_identifier')).withColumn('participant_identifier',
                                                                                                                                                            F.col('user_temp')).drop('user_temp')

    encounter_all_data = encounter_personal_data_filtered.unionByName(encounter_personal_data_filtered_p1)

    encounter_all_data_with_durations = encounter_all_data.withColumn('durations',(F.col('end_time').cast('double')-F.col('start_time').cast('double')).cast('double')/3600)

    encounter_all_data_with_durations = encounter_all_data_with_durations.withColumn('hour',F.hour('start_time'))

    columns = ['version','user','hour','centroid_id']
    encounter_all_data_with_durations_all = encounter_all_data_with_durations.groupBy(columns).max()
    columns1 = ['centroid_latitude','centroid_longitude','centroid_area','durations','total_encounters','normalized_total_encounters','unique_users','avg_encounters']
    for c in columns1:
        encounter_all_data_with_durations_all = encounter_all_data_with_durations_all.withColumn(c,F.col('max('+c+')'))
    encounter_all_data_with_durations_all = encounter_all_data_with_durations_all.select(*(columns+columns1))

    schema  = StructType(list([StructField('version',IntegerType()),
                               StructField('avg_encounters',DoubleType()),
                               StructField('total_encounters',DoubleType()),
                               StructField('normalized_total_encounters',DoubleType()),
                               StructField('user',StringType()),
                               StructField('centroid_longitude',DoubleType()),
                               StructField('centroid_latitude',DoubleType()),
                               StructField('centroid_area',DoubleType()),
                               StructField('durations',DoubleType()),
                               StructField('unique_users',IntegerType())
                               ]))

    column_names = [a.name for a in schema.fields]
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_final_durations(data):
        if data.shape[0]==0:
            return pd.DataFrame([],columns=column_names)
        data1 = data[:1].reset_index(drop=True)
        data1['durations'].iloc[0] = np.double('{:.2f}'.format(data['durations'].sum()))
        return data1[column_names]
    final_data = encounter_all_data_with_durations_all.groupBy(['version','user','centroid_id']).apply(get_final_durations)
    return DataStream(data=final_data,metadata=Metadata())

def generate_metadata_notification_daily():
    stream_metadata = Metadata()
    stream_metadata.set_name('mcontain-md2k--crowd--notification--daily').set_description('Computes notifications for each user who dwelled in a crowded hotspot') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the time window localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_latitude").set_type("double").set_attribute("description", \
                                                                                        "Latitude of centroid location, a gps cluster output grouping encounters in similar location together")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_longitude").set_type("double").set_attribute("description", \
                                                                                         "Longitude of centroid location, a gps cluster output grouping encounters in similar location together")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_area").set_type("double").set_attribute("description", \
                                                                                    "area of centroid")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("durations").set_type("double").set_attribute("description", \
                                                                                "duration of stay in the centroid in hours")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("unique_users").set_type("integer").set_attribute("description", \
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
        ModuleMetadata().set_name('Notification messages to be shown to each user') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    return stream_metadata


def get_utcoffset():
    ts = time.time()
    utc_offset = (datetime.utcfromtimestamp(ts) -
                  datetime.fromtimestamp(ts)).total_seconds()/3600
    return utc_offset

def get_time_columns(encounter_final_data,start_time,end_time,utc_offset):
    encounter_final_data = encounter_final_data.withColumn('localtime',F.lit(start_time).cast('timestamp'))
    encounter_final_data = encounter_final_data.withColumn('start_time',F.lit(start_time).cast('timestamp'))
    encounter_final_data = encounter_final_data.withColumn('end_time',F.lit(end_time).cast('timestamp'))
    encounter_final_data = encounter_final_data.withColumn('timestamp',F.col('localtime')+F.expr("INTERVAL "+str(int(utc_offset))+" HOURS"))
    return encounter_final_data
