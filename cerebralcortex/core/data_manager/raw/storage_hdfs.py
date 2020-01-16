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

from pyspark.sql.functions import lit
import pandas as pd
import uuid
import os
from uuid import uuid4
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from cerebralcortex.core.datatypes import DataStream


class HDFSStorage:

    def __init__(self, obj):
        """
        Constructor

        Args:
            obj (object): Object of Data class
        """
        self.obj = obj

    def read_file(self, stream_name:str, version:str="all", user_id:str=None)->object:
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
            if not self.obj.sql_data.is_stream(stream_name=stream_name):
                raise Exception("stream_name does not exist.")

        if user_id is not None:
            if not self.obj.sql_data.is_user(user_id=user_id):
                raise Exception("user_id does not exist.")

        hdfs_url = self._get_storage_path(stream_name)

        if version is not None and version!="all":
            hdfs_url = hdfs_url+"version="+str(version)+"/"
            
        if user_id is not None:
            hdfs_url = hdfs_url+"user="+str(user_id)+"/"

        df = self.obj.sparkSession.read.load(hdfs_url)

        if version is not None and version!="all":
            df = df.withColumn('version', lit(int(version)))

        if user_id is not None:
            df = df.withColumn('user', lit(str(user_id)))

        return df

        # if version=="all":
        #     hdfs_url = self._get_storage_path(stream_name)
        #     df = self.obj.sparkSession.read.load(hdfs_url)
        #     return df
        # else:
        #     hdfs_url = self._get_storage_path(stream_name)
        #     hdfs_url = hdfs_url+"version="+str(version)
        #     df = self.obj.sparkSession.read.load(hdfs_url)
        #     df = df.withColumn('version', lit(int(version)))
        #     return df

    def write_file(self, stream_name:str, data:DataStream.data, file_mode:str) -> bool:
        """
        Write pyspark DataFrame to HDFS

        Args:
            stream_name (str): name of the stream
            data (object): pyspark DataFrame object
            file_mode (str): write mode, append is currently supportes

        Returns:
            bool: True if data is stored successfully or throws an Exception.
        Raises:
            Exception: if DataFrame write operation fails
        """
        if isinstance(data, pd.DataFrame):
            return self.write_pandas_dataframe(stream_name, data)
        else:
            return self.write_spark_dataframe(stream_name, data, file_mode)

        # hdfs_url = self._get_storage_path(stream_name)
        # try:
        #     #data.write.save(hdfs_url, format='parquet', mode='append')
        #     data.write.partitionBy(["version","user"]).format('parquet').mode('overwrite').save(hdfs_url)
        #     return True
        # except Exception as e:
        #     raise Exception("Cannot store dataframe: "+str(e))

    def write_spark_dataframe(self, stream_name, data, file_mode:str):
        hdfs_url = self._get_storage_path(stream_name)
        try:
            data.write.partitionBy(["version","user"]).format('parquet').mode(file_mode).save(hdfs_url)
            return True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))

    def write_pandas_dataframe(self, stream_name, data):
        try:
            hdfs_url = self._get_storage_path(stream_name, no_spark=True)
            table = pa.Table.from_pandas(data, preserve_index=False)
            fs = pa.hdfs.connect(self.hdfs_ip, self.hdfs_port)
            pq.write_to_dataset(table, root_path=hdfs_url, partition_cols=["version", "user"], filesystem=fs)
            return True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))

    def write_pandas_to_parquet_file(self, df: pd, user_id: str, stream_name: str) -> str:
        """
        Convert pandas dataframe into pyarrow parquet format and store

        Args:
            df (pandas): pandas dataframe
            user_id (str): user id
            stream_name (str): name of a stream

        Returns:
            str: file_name of newly create parquet file

        Raises:
             Exception: if data cannot be stored

        """
        base_dir_path = self._get_storage_path(stream_name)
        table = pa.Table.from_pandas(df, preserve_index=False)
        file_id = str(uuid4().hex) + ".parquet"
        data_file_url = os.path.join(base_dir_path, "version=1", "user=" + user_id)
        file_name = os.path.join(data_file_url, file_id)
        if not self.obj.fs.exists(data_file_url):
            self.obj.fs.mkdir(data_file_url)
        with self.obj.fs.open(file_name, "wb") as fp:
            pq.write_table(table, fp)

        return file_name

    ###########################################################################################################

    def is_study(self) -> bool:
        """
        Returns true if study_name exists.

        Returns:
            bool: True if study_name exist False otherwise
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.is_study("default")
            >>> True
        """
        stream_path = self._get_storage_path()
        if self.obj.fs.exists(stream_path):
            return True
        else:
            return False

    def list_studies(self)->List[str]:
        """
        Get all the available study names

        Returns:
            List[str]: list of available study names

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_studies()
        """
        study_path = self._get_storage_path()
        study_names = []
        all_studies = self.obj.fs.ls(study_path)
        for strm in all_studies:
            study_names.append(strm.replace(study_path,"").replace("study=",""))
        return study_names

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
        stream_path = self._get_storage_path(stream_name=stream_name)
        if self.obj.fs.exists(stream_path):
            return True
        else:
            return False

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
            all_streams = self.obj.fs.ls(stream_path)
            for strm in all_streams:
                stream_versions.append(strm.replace(stream_path,"").replace("version=",""))
            return stream_versions
        else:
            raise Exception(stream_name+" does not exist")

    def list_streams(self)->List[str]:
        """
        Get all the available stream names

        Returns:
            List[str]: list of available streams metadata

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_streams()
        """
        stream_path = self._get_storage_path()
        stream_names = []
        all_streams = self.obj.fs.ls(stream_path)
        for strm in all_streams:
            stream_names.append(strm.replace(stream_path,"").replace("stream=","").replace("study="+self.obj.study_name, "").replace(self.obj.raw_files_dir,""))
        return stream_names

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
        all_streams = self.obj.fs.ls(stream_path)
        stream_names = []
        for strm in all_streams:
            if stream_name in strm:
                stream_names.append(strm.replace(stream_path,"").replace("stream=",""))
        return stream_names

    def get_stream_name(self, metadata_hash: uuid) -> str:
        """
        metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

        Args:
            metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
        Returns:
            str: name of a stream
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
            >>> ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST
        """
        stream_name = self.obj.sql_data.get_stream_name(metadata_hash)
        stream_path = self._get_storage_path(stream_name=stream_name)
        if self.is_stream(stream_path):
            return stream_name
        else:
            raise Exception(metadata_hash+" stream does not exist.")

    def get_stream_metadata_hash(self, stream_name: str) -> list:
        """
        Get all the metadata_hash associated with a stream name.

        Args:
            stream_name (str): name of a stream
        Returns:
            list[str]: list of all the metadata hashes
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_stream_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ["00ab666c-afb8-476e-9872-6472b4e66b68", "15cc444c-dfb8-676e-3872-8472b4e66b12"]
        """

        stream_path = self._get_storage_path(stream_name=stream_name)
        if self.is_stream(stream_path):
            return self.obj.sql_data.get_stream_metadata_hash(stream_name)
        else:
            raise Exception(stream_name+" stream does not exist.")

    def _get_storage_path(self, stream_name:str=None, no_spark=False)->str:
        """
        Build path of storage location

        Args:
            stream_name (str): name of a stream
        Returns:
            str: storage location path

        """
        if no_spark:
            storage_url = self.obj.raw_files_dir
        else:
            storage_url = self.obj.hdfs_spark_url + self.obj.raw_files_dir

        if stream_name is None or stream_name=="":
            storage_path = storage_url + "study="+self.obj.study_name+"/"
        else:
            storage_path = storage_url + "study="+self.obj.study_name+"/stream=" + stream_name + "/"

        if (self.obj.new_study or self.obj.study_name=="default") and not self.obj.fs.exists(storage_url + "study="+self.obj.study_name+"/"):
            self.obj.fs.mkdir(storage_url + "study="+self.obj.study_name+"/")

        return storage_path
