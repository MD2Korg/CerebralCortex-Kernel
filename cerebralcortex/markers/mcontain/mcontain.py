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




def compute_encounters(data,start_time,end_time):
    data_encounter = bluetooth_encounter(data,start_time,end_time)
    data_clustered = cluster_gps(data_encounter,minimum_points_in_cluster=1,geo_fence_distance=50)
    data_result = remove_duplicate_encounters(data_clustered)
    return data_result

def assign_covid_user(data,covid_users):
    if not isinstance(covid_users,list):
        covid_users = [covid_users]
    data = data.withColumn('covid',F.when(F.col('user').isin(covid_users), 1 ).when(F.col('participant_identifier').isin(covid_users), 2).otherwise(0))
    save_encounter_data(data)
    return True

def save_encounter_data(CC,data_result,):
    columns = ['centroid_id',
               'centroid_latitude',
               'centroid_longitude',
               'centroid_area']
    for c in columns:
        if c in data_result.columns:
            data_result = data_result.drop(*[c])
    ###save(data_result)  How do I get CC object?
    CC.save_stream(data_result)

def make_CC_object(study_name):
    return CC


def generate_visualization_hourly(stream_name,start_time,end_time):
    CC = make_CC_object('mcontain')
    data_all = CC.get_stream(stream_name)
    unique_encounters = compute_encounters(data_all,start_time=start_time,end_time=end_time) ## we need to save this datastream
    save_encounter_data(CC,unique_encounters)
    hourly_stats = count_encounters_per_cluster(unique_encounters,start_time,end_time)
    return hourly_stats

def generate_daily_stats(data,start_time,end_time):
    ### function yet to be written
    data = function(data,start_time,end_time) ## to be saved
    return data





