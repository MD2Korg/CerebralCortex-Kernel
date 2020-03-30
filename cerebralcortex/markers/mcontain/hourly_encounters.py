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
from cerebralcortex.algorithms.bluetooth.encounter import bluetooth_encounter,remove_duplicate_encounters,count_encounters_per_cluster
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from pyspark.sql import functions as F
from cerebralcortex.kernel import Kernel
import argparse

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
        DataDescriptor().set_name("avg_encounters").set_type("double").set_attribute("description", \
                                                                             "average encounter per participant(participants who had at least one encounter)"))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Visualization stats computation in a time window between start time and end time') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
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

def compute_encounters(data,start_time,end_time):
    data_encounter = bluetooth_encounter(data,start_time,end_time)
    data_clustered = cluster_gps(data_encounter,minimum_points_in_cluster=1,geo_fence_distance=50)
    data_result = remove_duplicate_encounters(data_clustered)
    return data_result

def generate_visualization_hourly(CC,stream_name,map_stream_name,start_time,end_time):
    data_all = CC.get_stream(stream_name).withColumnRenamed('avg_rssi','RSSI').withColumnRenamed('avg_distance','distance_estimate')
    data_map_stream = CC.get_stream(map_stream_name).withColumnRenamed('user','participant_identifier').select(*['participant_identifier',
                                                                                                                 'major',
                                                                                                                 'minor']).dropDuplicates()
    data_all = data_all.join(data_map_stream,on=['major','minor'],how='left').drop(*['major','minor'])
    if 'os' not in data_all.columns:
        data_all = data_all.withColumn('os',F.lit('android'))
    unique_encounters = compute_encounters(data_all,start_time=start_time,end_time=end_time) ## we need to save this datastream
    metadata = generate_metadata_encounter()
    save_data(CC,data_result=unique_encounters,centroid_present=True,metadata=metadata)
    print('Hourly encounters saved from', start_time, 'to', end_time)
    hourly_stats = count_encounters_per_cluster(unique_encounters)
    return hourly_stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hourly encounter calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)
    parser.add_argument('-a', '--input_stream_name', help='Input Stream Name', required=True)
    parser.add_argument('-b', '--input_map_stream_name', help='Input Map Stream Name', required=True)
    parser.add_argument('-s', '--start_time', help='Start time refers to start of hour in localtime datetime format', required=True)
    parser.add_argument('-e', '--end_time', help='End time refers to start of hour in localtime datetime format', required=True)

    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    input_stream_name = str(args["input_stream_name"]).strip()
    map_stream_name = str(args["input_map_stream_name"]).strip()
    start_time = args['start_time']
    end_time = args['end_time']
    CC = make_CC_object(config_dir)
    hourly_stats = generate_visualization_hourly(CC,input_stream_name,map_stream_name,start_time,end_time)
    userid = start_time.strftime("%Y/%m/%d, %H:%M:%S")+'_to_'+ end_time.strftime("%Y/%m/%d, %H:%M:%S") ### user id is generated to be able to save the data
    hourly_stats = hourly_stats.withColumn('user',F.lit(userid)).withColumn('start_time',F.lit(end_time)).withColumn('end_time',F.lit(end_time))
    save_data(CC,hourly_stats,centroid_present=False,metadata=generate_metadata_hourly())
    print('Computation done')

