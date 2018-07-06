# Copyright (c) 2018, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.data_manager.raw.stream_handler import StreamHandler
from cerebralcortex.core.data_manager.time_series.data import TimeSeriesData
from cerebralcortex.core.data_manager.object.data import ObjectData

class RawData(StreamHandler):
    def __init__(self, CC):
        """

        :param CC: CerebralCortex object reference
        """
        self.config = CC.config
        self.sql_data = CC.SqlData

        self.time_zone = CC.timezone

        self.logging = CC.logging
        self.timeSeriesData = TimeSeriesData(CC)
        self.logtypes = LogTypes()
        
        self.ObjectData = ObjectData(CC)

        self.host_ip = self.config['cassandra']['host']
        self.host_port = self.config['cassandra']['port']
        self.keyspace_name = self.config['cassandra']['keyspace']
        self.datapoint_table = self.config['cassandra']['datapoint_table']
        self.batch_size = 64500
        self.sample_group_size = 0 # disabled batching in row

        self.hdfs_ip = self.config['hdfs']['host']
        self.hdfs_port = self.config['hdfs']['port']
        self.hdfs_user = self.config['hdfs']['hdfs_user']
        self.hdfs_kerb_ticket = self.config['hdfs']['hdfs_kerb_ticket']
        self.raw_files_dir = self.config['hdfs']['raw_files_dir']

        self.nosql_store = self.config['data_ingestion']['nosql_store']
        self.filesystem_path = self.config["data_ingestion"]["filesystem_path"]
        
        self.data_play_type = self.config["data_replay"]["replay_type"]
        
        self.minio_input_bucket = self.config['minio']['input_bucket_name']
        self.minio_output_bucket = self.config['minio']['output_bucket_name']
        self.minio_dir_prefix = self.config['minio']['dir_prefix']
