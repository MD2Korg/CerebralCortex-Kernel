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

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata,DataDescriptor, ModuleMetadata
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from cerebralcortex.algorithms.bluetooth.encounter import get_user_encounter_count, get_notification_messages
from pyspark.sql import functions as F
from cerebralcortex.kernel import Kernel, DataStream
import argparse
import pandas as pd
import pytz
from datetime import datetime

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
    stream_metadata.set_name('mcontain-md2k--user-total-encounter').set_description('Number of encounter in a given time window') \
        .add_dataDescriptor(
        DataDescriptor().set_name("start_time").set_type("timestamp").set_attribute("description", \
                                                                                    "Start time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("end_time").set_type("timestamp").set_attribute("description", \
                                                                                  "End time of the time window in localtime")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("user").set_type("string").set_attribute("description", \
                                                                           "user id")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("timestamp").set_type("timestamp").set_attribute("description", \
                                                                                   "Unix timestamp when the calculation was done")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("localtime").set_type("timestamp").set_attribute("description", \
                                                                                   "Local timestamp when the calculation was done")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("encounter_count").set_type("int").set_attribute("description", \
                                                                                   "Total number of encounter for the user in the given time window")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("version").set_type("integer").set_attribute("description", \
                                                                               "version"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Total number of encounter for a user in a given time window') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Shiplu Hawlader", "shiplu.cse.du@gmail.com").set_version(1))
    return stream_metadata

def make_CC_object(config_dir="/home/jupyter/cc3_conf/",
                   study_name='mcontain'):
    CC = Kernel(config_dir, study_name=study_name)
    return CC

def save_data(CC,data_result,centroid_present=True,metadata=None):
    if centroid_present:
        columns = ['centroid_id',
                   'centroid_latitude',
                   'centroid_longitude',
                   'centroid_area']
        for c in columns:
            if c in data_result.columns:
                data_result = data_result.drop(*[c])
    data_result.metadata = metadata
    CC.save_stream(data_result,overwrite=False)
    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Daily encounter stats calculation")

    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)
    parser.add_argument('-a', '--input_stream_name_hour', help='Input Stream Name', required=True)
    parser.add_argument('-b', '--input_stream_name_encounter', help='Input Stream Name', required=True)
    parser.add_argument('-e', '--input_stream_name_user', help='Input Stream Name', required=True)
    parser.add_argument('-s', '--start_time', help='Start time refers to start of day in localtime datetime format', required=True)
    parser.add_argument('-e', '--end_time', help='End time refers to end of day in localtime datetime format', required=True)

    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    hour_stream_name = str(args["input_stream_name"]).strip()
    encounter_stream_name = str(args["input_stream_name"]).strip()
    user_stream_name = str(args["input_stream_name"]).strip()
    start_time = args['start_time']
    end_time = args['end_time']

    CC = make_CC_object(config_dir)
    data = CC.get_stream(hour_stream_name)
    metadata = data.metadata
    data_filtered = data._data.filter((F.col('start_time')>=start_time) & (F.col('start_time')<end_time))
    data_filtered = DataStream(data=data_filtered,metadata=metadata)
    data_gps = cluster_gps(data_filtered,minimum_points_in_cluster=1).select('centroid_id','total_encounters')
    data_max = data_gps.groupBy(['centroid_id']).sum().select('sum(total_encounters)')

    data_all_users = CC.get_stream(user_stream_name)
    data_all_users = data_all_users._data.filter((F.col('localtime')>=start_time) & (F.col('localtime')<end_time))

    encounter_data = CC.get_stream(encounter_stream_name)
    encounter_data = encounter_data._data.filter((F.col('localtime')>=start_time) & (F.col('localtime')<end_time))
    encounter_covid_data = encounter_data._data.filter(F.col('covid')>0)


    number_of_app_users = data_all_users.select('user').distinct().count()
    max_concurrent_encounters = data_max.toPandas().max()['sum(total_encounters)']
    proximity_encounter_per_user = encounter_data.count()*2/number_of_app_users
    total_covid_encounters = encounter_covid_data.count()*2

    userid = start_time.strftime("%Y/%m/%d, %H:%M:%S")+'_to_'+ end_time.strftime("%Y/%m/%d, %H:%M:%S") ### user id is generated to be able to save the data
    localtime = start_time
    timestamp = start_time.astimezone(pytz.UTC)
    version = 1
    df = pd.DataFrame([[userid,version,localtime,timestamp,
                         start_time,end_time,number_of_app_users,
                         proximity_encounter_per_user,total_covid_encounters,max_concurrent_encounters]],
                       columns=['user','version','localtime','timestamp',
                                'start_time','end_time','number_of_app_users',
                                'encounter_per_user','total_covid_encounters','maximum_concurrent_encounters'])
    data = CC.sqlContext.createDataFrame(df)
    data_daily = DataStream(data=data,metadata=Metadata())
    save_data(CC,data_daily,centroid_present=False,metadata=generate_metadata_dailystats())

    ## written by shiplu
    notification_ds = get_notification_messages(encounter_data, datetime(start_time.year, start_time.month, start_time.day))
    md = generate_metadata_notif()
    ds = DataStream(notification_ds, md)
    CC.save_stream(ds)
    user_encounter_ds = get_user_encounter_count(encounter_data, start_time, end_time)
    md = generate_metadata_user_encounter_count()
    ds = DataStream(user_encounter_ds, md)
    CC.save_stream(ds)


