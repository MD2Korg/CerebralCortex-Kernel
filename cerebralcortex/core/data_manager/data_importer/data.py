# Copyright (c) 2019, MD2K Center of Excellence
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

import os

from cerebralcortex.core.data_manager.raw.storage_filesystem import FileSystemStorage
from cerebralcortex.core.data_manager.raw.storage_hdfs import HDFSStorage
from cerebralcortex.core.data_manager.raw.stream_handler import StreamHandler
from cerebralcortex.core.data_manager.time_series.data import TimeSeriesData
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


class DataImporter:
    def __init__(self, CC, data_dir_path, with_metadata=False, headers=True, file_type="csv"):

        self.config = CC.config
        self.sql_data = CC.SqlData

        self.logging = CC.logging
        self.nosql_store = self.config['nosql_storage']

        self.sparkSession = CC.sparkSession

        self.metadata = Metadata()
        
        self.data_dir_path = data_dir_path
        self.with_metadata = with_metadata
        self.headers = headers
        self.file_type = file_type

        # pseudo factory pattern
        if self.nosql_store == "hdfs":
            self.nosql = HDFSStorage(self)
            self.hdfs_ip = self.config['hdfs']['host']
            self.hdfs_port = self.config['hdfs']['port']
            self.hdfs_spark_url = "hdfs://"+str(self.hdfs_ip)+":"+str(self.hdfs_port)+"/"
            self.raw_files_dir = self.config['hdfs']['raw_files_dir']
        elif self.nosql_store=="filesystem":
            self.nosql = FileSystemStorage(self)
            self.filesystem_path = self.config["filesystem"]["filesystem_path"]
            if not os.access(self.filesystem_path, os.W_OK):
                raise Exception(self.filesystem_path+" path is not writable. Please check your cerebralcortex.yml configurations.")
        else:
            raise ValueError(self.nosql_store + " is not supported.")

        if self.config["visualization_storage"]!="none":
            self.timeSeriesData = TimeSeriesData(CC)

        self.logtypes = LogTypes()
