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
from cerebralcortex.algorithms.bluetooth.encounter import bluetooth_encounter,remove_duplicate_encounters
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from pyspark.sql import functions as F

def compute_encounters(data,start_time,end_time):
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
        DataDescriptor().set_name("mean_distance").set_type("double").set_attribute("description", \
                                                                                    "Mean distance between participants in encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("average_count").set_type("double").set_attribute("description", \
                                                                                    "Average count of values received in phone per minute - average across the encounter")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_latitude").set_type("double").set_attribute("description", \
                                                                                        "Latitude of the centroid in which encounter occurred")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_longitude").set_type("double").set_attribute("description", \
                                                                                         "Longitude of the centroid in which encounter occurred")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("centroid_id").set_type("integer").set_attribute("description", \
                                                                                   "Unique id of the centroid in the time window it was computed")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("covid").set_type("integer").set_attribute("description", \
                                                                             "0, 1 or 2 indicating if this encounter contained a covid user -- 0 - no covid-19 affected, 1 - user is, 2 - participant identifier is "))
    stream_metadata.add_module(
        ModuleMetadata().set_name('Encounter computation after parsing raw bluetooth-gps data, clustering gps locations and removing double counting') \
            .set_attribute("url", "https://mcontain.md2k.org").set_author(
            "Md Azim Ullah", "mullah@memphis.edu"))
    data_encounter = bluetooth_encounter(data,start_time,end_time)
    data_clustered = cluster_gps(data_encounter,minimum_points_in_cluster=1,geo_fence_distance=50)
    data_result = remove_duplicate_encounters(data_clustered)
    data_result.metadata = stream_metadata
    return data_result

def assign_covid_user(data,covid_users):
    data = data.withColumn('covid',F.when(F.col('user').isin(covid_users), 1 ).when(F.col('participant_identifier').isin(covid_users), 2).otherwise(0))
    return data





