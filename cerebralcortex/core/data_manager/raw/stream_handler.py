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


import traceback
from enum import Enum

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.metadata import Metadata


class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():
    
    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################
    def get_stream(self, stream_name:str, version:str, data_type=DataSet.COMPLETE) -> DataStream:

        if stream_name is None or stream_name=="":
            raise ValueError("stream_name cannot be None or empty")



        if version is not None and version!="":
            all_versions = self.sql_data.get_stream_versions(stream_name=stream_name)

        version = str(version).strip().replace(" ", "")

        if version!="all" and version!="latest":
            if int(version) not in all_versions:
                raise Exception("Version "+str(version)+" is not available for stream: "+str(stream_name))

        if version=="latest":
            version = max(all_versions)

        version = str(version)

        stream_metadata = self.sql_data.get_stream_metadata_by_name(stream_name=stream_name, version=version)
        metadata_obj = Metadata.from_json(stream_metadata)

        if len(stream_metadata) > 0:
            if data_type == DataSet.COMPLETE:
                df = self.nosql.read_file(stream_name=stream_name, version=version)
                stream = DataStream(data=df,metadata=metadata_obj)
            elif data_type == DataSet.ONLY_DATA:
                df = self.nosql.read_file(stream_name=stream_name, version=version)
                stream = DataStream(data=df)
            elif data_type == DataSet.ONLY_METADATA:
                stream = DataStream(metadata=metadata_obj)
            else:
                raise ValueError("Invalid type parameter: data_type "+str(data_type))
            return stream
        else:
            return DataStream()

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################
    def save_stream(self, datastream, ingestInfluxDB=False):

        metadata = datastream.metadata
        data = datastream.data
        if len(metadata)>0:
            stream_name = metadata[0].name # only supports one data-stream storage at a time
            try:

                # get start and end time of a stream
                if datastream:
                    status = self.nosql.write_file(stream_name, data)

                    if status:
                        # save metadata in SQL store
                        for md in metadata:
                            self.sql_data.save_stream_metadata(md)
                    else:
                        print(
                            "Something went wrong in saving data points.")
            except Exception as e:
                self.logging.log(
                    error_message="STREAM ID:  - Cannot save stream. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
        else:
            raise Exception("Metadata cannot be empty.")
