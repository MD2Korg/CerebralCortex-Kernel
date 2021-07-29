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

import pandas as pd
from pyspark.sql.functions import lit

from cerebralcortex.algorithms.utils.mprov_helper import write_metadata_to_mprov
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata


class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():
    
    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################
    def get_stream(self, stream_name:str, version:str="latest", user_id:str=None, data_type=DataSet.COMPLETE) -> DataStream:
        """
        Retrieve a data-stream with it's metadata.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are latest, or a specific version of a stream (e.g., 2)
            user_id (str): id of a user
            data_type (DataSet):  DataSet.COMPLETE returns both Data and Metadata. DataSet.ONLY_DATA returns only Data. DataSet.ONLY_METADATA returns only metadata of a stream. (Default=DataSet.COMPLETE)

        Returns:
            DataStream: contains Data and/or metadata
        Raises:
            ValueError: if stream name is empty or None
        Note:
            Please specify a version if you know the exact version of a stream. Getting all the stream data and then filtering versions won't be efficient.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = CC.get_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ds.data # an object of a dataframe
            >>> ds.metadata # an object of MetaData class
            >>> ds.get_metadata(version=1) # get the specific version metadata of a stream
        """
        if stream_name is None or stream_name=="":
            raise ValueError("stream_name cannot be None or empty")

        stream_name = stream_name.lower()

        if not self.sql_data.is_stream(stream_name):
            print(stream_name, " does not exist.")
            return DataStream(data=None, metadata=None)

        if version is not None and version!="":
            all_versions = self.sql_data.get_stream_versions(stream_name=stream_name)

        version = str(version).strip().replace(" ", "")

        if version!="latest" and version!="all":
            if int(version) not in all_versions:
                raise Exception("Version "+str(version)+" is not available for stream: "+str(stream_name))

        if version=="latest":
            version = max(all_versions)

        if version!="all":
            version = int(version)

        stream_metadata = self.sql_data.get_stream_metadata_by_name(stream_name=stream_name, version=version)

        if stream_metadata:
            if data_type == DataSet.COMPLETE:
                df = self.read_file(stream_name=stream_name, version=version, user_id=user_id)
                #df = df.dropDuplicates(subset=['timestamp'])
                stream = DataStream(data=df,metadata=stream_metadata)
            elif data_type == DataSet.ONLY_DATA:
                df = self.read_file(stream_name=stream_name, version=version, user_id=user_id)
                #df = df.dropDuplicates(subset=['timestamp'])
                stream = DataStream(data=df)
            elif data_type == DataSet.ONLY_METADATA:
                stream = DataStream(metadata=stream_metadata)
            else:
                raise ValueError("Invalid type parameter: data_type "+str(data_type))
            return stream
        else:
            return DataStream()

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################
    def save_stream(self, datastream, overwrite=False)->bool:
        """
        Saves datastream raw data in selected NoSQL storage and metadata in MySQL.

        Args:
            datastream (DataStream): a DataStream object
            overwrite (bool): if set to true, whole existing datastream data will be overwritten by new data
        Returns:
            bool: True if stream is successfully stored or throws an exception
        Todo:
            Add functionality to store data in influxdb.
        Raises:
            Exception: log or throws exception if stream is not stored
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_stream(ds)
        """
        if overwrite:
            file_mode="overwrite"
            if not self.sql_data._delete_stream(datastream.metadata.name):
                raise Exception("Cannot remove MySQL records during overwrite operation.")
        else:
            file_mode = "append"
            
        metadata = datastream.metadata
        data = datastream.data
        if metadata:
            stream_name = metadata.name # only supports one data-stream storage at a time
            stream_name = stream_name.lower()
            metadata.set_study_name(self.study_name)
            if not stream_name:
                raise ValueError("Stream name cannot be empty/None. Check metadata.")
            #metadata = self.__update_data_desciptor(data=data, metadata=metadata)
            try:
                if datastream:
                    if isinstance(data, pd.DataFrame):
                        column_names = data.columns
                    else:
                        column_names = data.schema.names

                    if 'user' not in column_names:
                        raise Exception("user column is missing in data schema")

                    data = self._drop_column(data, column_names)

                    result = self.sql_data.save_stream_metadata(metadata)
                    
                    if result["status"]==True:
                        write_metadata_to_mprov(metadata=metadata)

                        version = result.get("version")

                        if isinstance(data, pd.DataFrame):
                            data["version"] = version
                        else:
                            data = data.withColumn('version', lit(version))

                        status = self.write_file(stream_name, data, file_mode)
                        return status
                    else:
                        print("Something went wrong in saving data points in SQL store.")
                        return False
            except Exception as e:
                self.logging.log(
                    error_message="STREAM ID:  - Cannot save stream. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
        else:
            raise Exception("Metadata cannot be empty.")

    def _drop_column(selfd, data, column_names):
        if 'version' in column_names:
            if isinstance(data, pd.DataFrame):
                del data["version"]
            else:
                data = data.drop('version')
        return data

    def __update_data_desciptor(self, data, metadata):
        """
        Read pyspark dataframe clumns and add each column name and type to datadescriptor field

        Args:
            data (pyspark dataframe): pyspark dataframe
            metadata (Metadata): stream metadata
        Notes:
            this is a private method and should only be used internally
        Returns:
            metadata (MetaData): updated metadata with name/type added in data descriptors
        Raises:
            Exception: if number of datadescriptors columns in metadata and number of pyspark dataframe columns have different length

        """
        tmp = []
        if isinstance(data, pd.DataFrame):
            for field_name, field_type in zip(data.dtypes.index, data.dtypes):
                #if field_name not in ["timestamp", "localtime", "user", "version"]:
                basic_dd = {}
                basic_dd["name"] = field_name
                basic_dd["type"]= str(field_type)
                tmp.append(basic_dd)
        else:
            for field in data.schema.fields:
                #if field.name not in ["timestamp", "localtime", "user", "version"]:
                basic_dd = {}
                basic_dd["name"] = field.name
                basic_dd["type"]= str(field.dataType)
                tmp.append(basic_dd)

        new_dd = []
        for dd in metadata.data_descriptor:
            dd.name = ""
            dd.type = ""
            dd.attributes = dd.attributes
            new_dd.append(dd)

        if len(tmp)!=len(new_dd) and (len(tmp)-4)!=len(new_dd):
            raise Exception("Data descriptor number of columns does not match with the actual number of dataframe columns. Add data description for each of dataframe column.")

        updated_data_descriptors = []
        for (datadescipt,column_names) in zip(new_dd, tmp):
            datadescipt.name = column_names["name"]
            datadescipt.type = column_names["type"]
            updated_data_descriptors.append(datadescipt)

        metadata.data_descriptor = updated_data_descriptors
        return metadata
