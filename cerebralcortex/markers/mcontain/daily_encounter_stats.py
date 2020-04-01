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

from cerebralcortex.algorithms.bluetooth.encounter import *
from cerebralcortex.markers.mcontain.hourly_encounters import *
from pyspark.sql.types import StructField, StructType, DoubleType,StringType, TimestampType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
import pandas as pd, numpy as np
import time
from datetime import datetime,timedelta

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
                         StructField('centroid_longitude', DoubleType()),
                         StructField('centroid_latitude', DoubleType()),
                         StructField('centroid_id', IntegerType()),
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
            save_this['covid'].iloc[0] = temp_df['covid'].max()
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
    data_gps = cluster_gps(data)
    data_gps = data_gps.withColumn('start_time',F.col('start_time').cast('double')).withColumn('end_time',F.col('end_time').cast('double'))
    data_final = data_gps.groupBy(['version','centroid_id']).apply(compute_daily_encounter_stream)
    return DataStream(data=data_final,metadata=Metadata())


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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Daily encounter stats calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)
    parser.add_argument('-a', '--input_stream_name_hour', help='Input Stream Name', required=True)
    parser.add_argument('-b', '--input_stream_name_encounter', help='Input Stream Name', required=True)
    parser.add_argument('-e', '--input_stream_name_user', help='Input Stream Name', required=True)
    parser.add_argument('-s', '--start_time', help='Start time refers to start of day in string format', required=True)
    parser.add_argument('-e', '--end_time', help='End time refers to end of day in localtime datetime format', required=True)

    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    hour_stream_name = str(args["input_stream_name_hour"]).strip()
    encounter_stream_name = str(args["input_stream_name_encounter"]).strip()
    user_stream_name = str(args["input_stream_name_user"]).strip()
    start_time = args['start_time']
    end_time = args['end_time']
    utc_offset = get_utcoffset()
    userid = start_time+'_to_'+ end_time ### user id is generated to be able to save the data

    CC = Kernel(config_dir, study_name='mcontain')  ## need to change the study name when
    data = CC.get_stream(hour_stream_name)
    metadata = data.metadata
    data_filtered = data._data.filter((F.col('localtime')>=F.lit(start_time)) & (F.col('localtime')<F.lit(end_time)))
    data_filtered = DataStream(data=data_filtered,metadata=metadata)
    data_gps = data_filtered.select('total_encounters','start_time','end_time')
    data_max = data_gps.groupBy(['start_time','end_time']).max().select('max(total_encounters)')
    data_all_users = CC.get_stream(user_stream_name)
    data_all_users = data_all_users._data.filter((F.col('localtime')>=F.lit(start_time)) & (F.col('localtime')<F.lit(end_time)))

    encounter_data = CC.get_stream(encounter_stream_name)
    encounter_data = encounter_data._data.filter((F.col('localtime')>=F.lit(start_time)) & (F.col('localtime')<F.lit(end_time)))
    encounter_final_data = remove_duplicate_encounters_day(DataStream(data=encounter_data,metadata=Metadata()))
    daily_visualization_stats = count_encounters_per_cluster(encounter_final_data)



    encounter_final_data = drop_centroid_columns(encounter_final_data)
    encounter_final_data = get_time_columns(encounter_final_data,start_time,end_time,utc_offset)
    encounter_final_data.printSchema()
    encounter_final_data.metadata = generate_metadata_encounter_daily()
    CC.save_stream(encounter_final_data,overwrite=False)

    daily_visualization_stats = daily_visualization_stats.withColumn('user',F.lit(userid).cast('string'))
    daily_visualization_stats = get_time_columns(daily_visualization_stats,start_time,end_time,utc_offset)
    daily_visualization_stats.printSchema()
    daily_visualization_stats.metadata = generate_metadata_visualization_daily()
    CC.save_stream(daily_visualization_stats,overwrite=False)


    encounter_covid_data = encounter_final_data.filter(F.col('covid')>0)
    number_of_app_users = data_all_users.select('user').distinct().count()
    max_concurrent_encounters = data_max.toPandas().max()['max(total_encounters)']
    proximity_encounter_per_user = encounter_final_data.count()*2/number_of_app_users
    total_covid_encounters = encounter_covid_data.count()*2

    version = 1
    df = pd.DataFrame([[userid,version,number_of_app_users,
                        proximity_encounter_per_user,total_covid_encounters,max_concurrent_encounters]],columns=['user','version','number_of_app_users',
                        'encounter_per_user','total_covid_encounters','maximum_concurrent_encounters'])
    data = CC.sqlContext.createDataFrame(df)
    data = get_time_columns(data,start_time,end_time,utc_offset)
    data_daily = DataStream(data=data,metadata=Metadata())
    data_daily.metadata = generate_metadata_dailystats()
    CC.save_stream(data_daily,overwrite=False)


    user_first = encounter_final_data.select(F.col('participant_identifier').alias('user')).withColumn('count',F.lit(1))
    user_second = encounter_final_data.select(F.col('user')).withColumn('count',F.lit(1))
    all_encounter_users = user_first.union(user_second)
    all_encounter_users_count = all_encounter_users.groupBy('user').sum().withColumnRenamed('sum(count)','count').select('user','count')
    all_users_count = data_all_users.select('user').distinct().withColumn('count1',F.lit(0))
    all_users = all_users_count.join(all_encounter_users_count,how='left',on=['user']).na.fill(0).withColumn("encounter_count",F.greatest('count','count1')).select('user',"encounter_count")
    all_users = get_time_columns(all_users,start_time,utc_offset)
    metadata_user_encounter = generate_metadata_user_encounter_count()
    all_users = all_users.withColumn('version',F.lit(0))
    all_users = DataStream(all_users, metadata_user_encounter)
    all_users.printSchema()
    CC.save_stream(all_users,overwrite=False)


    ## written by shiplu
    st = data_daily.select('localtime').take(1)[0]['localtime']
    et = st + timedelta(hours=24)
    notification_ds = get_notification_messages(encounter_final_data, datetime(st.year, st.month, st.day))
    md = generate_metadata_notif()
    ds = DataStream(notification_ds, md)
    CC.save_stream(ds,overwrite=False)

