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
from typing import List
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from cerebralcortex.core.datatypes import DataStream


class FileBasedStorage():

    def __init__(self):
        """
        Constructor

        Args:
            obj (object): Object of Data class
        """

        if self.new_study or self.study_name=="default":
            self._create_dir()

    def _get_storage_path(self, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Construct storage path for data

        Args:
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            str: path where data shall be stored/searched
        """

        dirpath = self.data_path+"study="+self.study_name+"/"

        if stream_name:
            dirpath += "stream={0}/".format(stream_name)

        if version:
            if "stream=" not in dirpath:
                raise ValueError("stream_name argument is missing.")
            else:
                dirpath += "version={0}/".format(str(version))

        if user_id:
            if "stream=" not in dirpath or "version=" not in dirpath:
                raise ValueError("stream_name and/or version arguments are missing.")
            else:
                dirpath += "user={0}/".format(user_id)

        return dirpath

    def _path_exist(self, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Checks if a path exist

        Args:
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            bool: true if path exist, false otherwise
        """
        storage_path = self._get_storage_path(stream_name=stream_name, version=version, user_id=user_id)
        if self.nosql_store == "hdfs":
            status = self.fs.exists(storage_path)
        elif self.nosql_store=="filesystem":
            status = self.fs.path.exists(storage_path)
        else:
            raise Exception("Not supported File system")

        if status:
            return True
        else:
            return False

    def _ls_dir(self, stream_name:str=None, version:int=None, user_id:str=None):
        """
        List the contents of a directory

        Args:
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            list[str]: list of file and/or dir names
        """
        storage_path = self._get_storage_path(stream_name=stream_name, version=version, user_id=user_id)
        if self.nosql_store == "hdfs":
            return self.fs.ls(storage_path)
        elif self.nosql_store=="filesystem":
            return self.fs.listdir(storage_path)
        else:
            raise Exception("Not supported File system")

    def _create_dir(self, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Creates a directory if it does not exist.

        Args:
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        """
        storage_path = self._get_storage_path(stream_name=stream_name, version=version, user_id=user_id)
        if self.nosql_store == "hdfs":
            if not self.fs.exists(storage_path):
                self.fs.mkdir(storage_path)
            return storage_path
        elif self.nosql_store=="filesystem":
            if not os.path.exists(storage_path):
                self.fs.makedirs(storage_path)
            return storage_path
        return None


    def read_file(self, stream_name:str, version:str="latest", user_id:str=None)->object:
        """
        Get stream data from storage system. Data would be return as pyspark DataFrame object
        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")
            user_id (str): id of a user
        Note:
            Please specify a version if you know the exact version of a stream. Getting all the stream data and then filtering versions won't be efficient.

        Returns:
            object: pyspark DataFrame object
        Raises:
            Exception: if stream name does not exist.
        """

        if user_id is not None and version=="all":
            raise Exception("Not supported, yet. You can only request only one version of a stream associated with a user.")

        if stream_name!="" and stream_name is not None:
            if not self._path_exist(stream_name=stream_name):
                raise Exception(stream_name+" does not exist.")

        if version is not None and version!="all":
            data_path = self._get_storage_path(stream_name=stream_name, version=version, user_id=user_id)
        else:
            data_path = self._get_storage_path(stream_name=stream_name)

        df = self.sparkSession.read.load(data_path)

        if version is not None and version!="all":
            df = df.withColumn('version', F.lit(int(version)))

        if user_id is not None:
            df = df.withColumn('user', F.lit(str(user_id)))

        return df

    def write_file(self, stream_name:str, data:DataStream.data, file_mode:str) -> bool:
        """
        Write pyspark DataFrame to a file storage system

        Args:
            stream_name (str): name of the stream
            data (object): pyspark DataFrame object
            file_mode (str): write mode, append is currently supportes

        Returns:
            bool: True if data is stored successfully or throws an Exception.
        Raises:
            Exception: if DataFrame write operation fails
        """
        data_path = self._get_storage_path(stream_name=stream_name)
        if isinstance(data, pd.DataFrame):
            try:
                table = pa.Table.from_pandas(data, preserve_index=False)
                pq.write_to_dataset(table, root_path=data_path, partition_cols=["version", "user"])
                return True
            except Exception as e:
                raise Exception("Cannot store pandas dataframe: "+str(e))
        else:
            try:
                data.write.partitionBy(["version","user"]).format('parquet').mode(file_mode).save(data_path)
                return True
            except Exception as e:
                raise Exception("Cannot store spark dataframe: "+str(e))

    def write_pandas_to_parquet_file(self, df: pd, user_id: str, stream_name: str, stream_version:str) -> str:
        """
        Convert pandas dataframe into pyarrow parquet format and store

        Args:
            df (pandas): pandas dataframe
            user_id (str): user id
            stream_name (str): name of a stream

        Returns:
            str: file_name of newly create parquet file
        """

        table = pa.Table.from_pandas(df, preserve_index=False)
        file_id = str(uuid4().hex) + ".parquet"

        data_path = self._create_dir(stream_name=stream_name, version=stream_version, user_id=user_id)
        data_path = data_path+file_id
        pq.write_table(table, data_path)

        return data_path

    #################################################################################################################

    def is_study(self) -> bool:
        """
        Returns true if study_name exists.

        Returns:
            bool: True if study_name exist False otherwise
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.is_study()
            >>> True
        """
        return self._path_exist()


    def is_stream(self, stream_name: str) -> bool:
        """
        Returns true if provided stream exists.

        Args:
            stream_name (str): name of a stream
        Returns:
            bool: True if stream_name exist False otherwise
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.is_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> True
        """
        return self._path_exist(stream_name=stream_name)

    def get_stream_versions(self, stream_name: str) -> list:
        """
        Returns a list of versions available for a stream

        Args:
            stream_name (str): name of a stream
        Returns:
            list: list of int
        Raises:
            ValueError: if stream_name is empty or None
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_stream_versions("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> [1, 2, 4]
        """
        stream_path = self._get_storage_path(stream_name=stream_name)
        stream_versions = []
        if self.is_stream(stream_name):
            all_streams = self._ls_dir(stream_name=stream_name)
            for strm in all_streams:
                stream_versions.append(strm.replace(stream_path,"").replace("version=",""))
            return stream_versions
        else:
            raise Exception(stream_name+" does not exist")

    def list_streams(self)->List[str]:
        """
        Get all the available stream names

        Returns:
            List[str]: list of available streams names

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_streams()
        """
        stream_path = self._get_storage_path()
        stream_names = []
        all_streams = self._ls_dir()
        for strm in all_streams:
            stream_names.append(strm.replace(stream_path,"").replace("stream=","").replace("study="+self.study_name, ""))
        return stream_names

    def list_users(self, stream_name:str, version:int=1)->List[str]:
        """
        Get all the available stream names with metadata

        stream_name (str): name of a stream
        version (int): version of a stream

        Returns:
            List[str]: list of available user-ids for a giving stream version

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_users()
        """
        stream_path = self._get_storage_path(stream_name=stream_name, version=version)
        all_users = self._ls_dir(stream_name=stream_name, version=version)
        user_ids = []
        for usr in all_users:
            user_ids.append(usr.replace(stream_path,"").replace("user=","").replace("study="+self.study_name, ""))
        return user_ids

    def search_stream(self, stream_name)->List[str]:
        """
        Find all the stream names similar to stream_name arg. For example, passing "location"
        argument will return all stream names that contain the word location

        Returns:
            List[str]: list of stream names similar to stream_name arg

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.search_stream("battery")
            >>> ["BATTERY--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST", "BATTERY--org.md2k.phonesensor--PHONE".....]
        """
        stream_path = self._get_storage_path()
        all_streams = self._ls_dir()
        stream_names = []
        for strm in all_streams:
            if stream_name in strm:
                stream_names.append(strm.replace(stream_path,"").replace("stream=",""))
        return stream_names
