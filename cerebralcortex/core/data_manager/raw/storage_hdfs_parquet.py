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

import gzip
import pickle
import traceback
import uuid
from datetime import datetime, timedelta
from typing import List

import pyarrow

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.util.data_types import deserialize_obj

class HDFSStorage():

    def __init__(self, obj):
        self.obj = obj

    def read_file(self, stream_name:str):

        hdfs_url = self.get_hdf_url(stream_name)
        df = self.obj.sparkSession.read.load(hdfs_url)
        return df


    def write_file(self, stream_name, data) -> bool:


        # Using libhdfs
        stream_name = "stream="+stream_name
        hdfs_url = self.obj.hdfs_spark_url+self.obj.raw_files_dir+stream_name+"/"
        success = False
        try:
            data.write.save(hdfs_url, format='parquet')
            success = True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))
        return success

    def get_hdf_url(self, stream_name:str):
        hdfs_url = self.obj.hdfs_spark_url+self.obj.raw_files_dir

        if stream_name is None or stream_name=="":
            raise ValueError("Stream name cannot be empty.")
        else:
            hdfs_url = hdfs_url+"stream="+stream_name+"/"

        # if stream_version is not None or stream_version!="":
        #     hdfs_url = hdfs_url+stream_version+"/"
        #
        # if owner_id is not None and owner_id!="":
        #     hdfs_url = hdfs_url+owner_id+"/"
        #
        # if day is not None or day!="":
        #     hdfs_url = hdfs_url+day+"_*"

        return hdfs_url
