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
from datetime import timedelta
from dateutil import parser as dateparser
from cerebralcortex.algorithms.visualization.visualization import create_geojson_features


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

def compute_encounters(data,data_map_stream,start_time,end_time,ltime=False):
    data_encounter = bluetooth_encounter(data,start_time,end_time,ltime=ltime)
    data_encounter = data_encounter.join(data_map_stream,on=['major','minor'],how='left').drop(*['major','minor']).dropna()
    print(data_encounter.count(),'encounters computed')
    data_clustered = cluster_gps(data_encounter,minimum_points_in_cluster=1,geo_fence_distance=50)
    # data_clustered.drop(*['distances','centroid_longitude','centroid_latitude','centroid_id','centroid_area']).show(20,False)
    print(data_encounter.count(),'clusters computed')
    data_result = remove_duplicate_encounters(data_clustered)
    # data_result.drop(*['distances','centroid_longitude','centroid_latitude','centroid_id','centroid_area']).show(20,False)
    return data_result

def generate_visualization_hourly(data_all,data_map_stream,start_time,end_time,ltime=False):
    data_all = data_all.withColumnRenamed('avg_rssi','RSSI').withColumnRenamed('avg_distance','distance_estimate')

    data_all = data_all.withColumn('distance_estimate',F.col('distance_estimate').cast('double'))
    data_all = data_all.withColumn('minor',F.col('minor').cast('long')).withColumn('major',F.col('major').cast('long'))
    if 'participant_identifier' in data_map_stream.columns:
        data_map_stream = data_map_stream.drop(*['participant_identifier'])
    data_map_stream = data_map_stream.withColumnRenamed('user','participant_identifier').select(*['participant_identifier','os',
                                                                                                  'major',
                                                                                                  'minor']).dropDuplicates()

    unique_encounters = compute_encounters(data_all,data_map_stream,start_time=start_time,end_time=end_time,ltime=ltime) ## we need to save this datastream
    return unique_encounters


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hourly encounter calculation")
    parser.add_argument('-c', '--config_dir', help='CC Configuration directory path', required=True)
    parser.add_argument('-a', '--input_stream_name', help='Input Stream Name', required=True)
    parser.add_argument('-b', '--input_map_stream_name', help='Input Map Stream Name', required=True)
    parser.add_argument('-s', '--start_time', help='Start time refers to start of hour in string format %Y-%m-%d %H:%m', required=True)
    parser.add_argument('-e', '--end_time', help='End time refers to start of hour in string format %Y-%m-%d %H:%m', required=True)
    parser.add_argument('-l', '--ltime', help='if set to True then computation would be done on localtime or timestamp otherwise', required=True)
    parser.add_argument('-s', '--sdir', help='Output Directory to save the visualization html', required=True)
    parser.add_argument('-t', '--threshold', help='Threshold on total_encounters for visualization', required=True)


    args = vars(parser.parse_args())
    config_dir = str(args["config_dir"]).strip()
    input_stream_name = str(args["input_stream_name"]).strip()
    map_stream_name = str(args["input_map_stream_name"]).strip()
    start_time = args['start_time']
    end_time = args['end_time']
    ltime = args['ltime']
    sdir = args['sdir']
    threshold = args['threshold']

    CC = Kernel(config_dir, study_name='mcontain') ## need to change the study name
    data_all = CC.get_stream(input_stream_name)
    data_map_stream = CC.get_stream(map_stream_name)
    unique_encounters = generate_visualization_hourly(data_all,data_map_stream,start_time,end_time,ltime=ltime)
    hourly_stats = count_encounters_per_cluster(unique_encounters)
    unique_encounters = drop_centroid_columns(unique_encounters, centroid_present=True)

    metadata = generate_metadata_encounter()
    unique_encounters.metadata = metadata
    CC.save_stream(unique_encounters,overwrite=False)
    print('Hourly encounters saved from', start_time, 'to', end_time)

    print('Saving hourly stats now')
    userid = start_time +'_to_'+ end_time ### user id is generated to be able to save the data
    hourly_stats = hourly_stats.withColumn('user',F.lit(userid)).withColumn('start_time',F.lit(start_time).cast('timestamp')).withColumn('end_time',F.lit(end_time).cast('timestamp'))
    hourly_stats.metadata = generate_metadata_hourly()
    CC.save_stream(hourly_stats,overwrite=False)
    print('Computation done')

    datetime_format = '%Y-%m-%d %H:%m'
    stream_name_stats = generate_metadata_hourly().name
    hourly_visualization_stats = CC.get_stream(stream_name_stats)
    hourly_visualization_stats = hourly_visualization_stats.filter(hourly_visualization_stats.start_time<F.lit(end_time))
    start_time = dateparser.parse(end_time) - timedelta(hours=24)
    start_time = start_time.strftime(datetime_format)
    hourly_visualization_stats = hourly_visualization_stats.filter(hourly_visualization_stats.start_time>F.lit(start_time))
    visualization_stats = hourly_visualization_stats.toPandas()
    visualization_stats = visualization_stats[visualization_stats.total_encounters>=threshold]
    if visualization_stats.shape[0]>0:
        html_file = create_geojson_features(visualization_stats,day=end_time.split(' ')[0],
                                            time_column_name='start_time',
                                            latitude_columns_name='latitude',
                                            longitude_column_name='longitude',
                                            visualize_column_name='total_encounters')
        if sdir[-1] != '/':
            sdir+='/'
        html_file.save(sdir+end_time)



