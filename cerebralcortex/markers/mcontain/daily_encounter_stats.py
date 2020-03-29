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


from cerebralcortex.algorithms.gps.clustering import cluster_gps
from pyspark.sql import functions as F
from cerebralcortex.kernel import Kernel, DataStream
import argparse


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
    number_of_app_users = data_all_users.select('user').distinct().count()
    max_concurrent_encounters = data.toPandas().max()['sum(total_encounters)']
    proximity_encounter_per_user = encounter_data.count()*2/number_of_app_users
