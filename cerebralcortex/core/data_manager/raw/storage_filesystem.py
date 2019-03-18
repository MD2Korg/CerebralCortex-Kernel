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
import pyarrow as pa
import pyarrow.parquet as pq
from cerebralcortex.core.datatypes import DataStream


class FileSystemStorage:

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

    def write_file(self, stream_name:str, data:DataStream.data) -> bool:
        """
        Write pyspark DataFrame to a file storage system

        Args:
            stream_name (str): name of the stream
            data (object): pyspark DataFrame object

        Returns:
            bool: True if data is stored successfully or throws an Exception.
        Raises:
            Exception: if DataFrame write operation fails
        """
        if isinstance(data, pd.DataFrame):
            return self.write_pandas_dataframe(stream_name, data)
        else:
            return self.write_spark_dataframe(stream_name, data)

        # hdfs_url = self._get_storage_path(stream_name)
        # try:
        #     data.write.partitionBy(["version","user"]).format('parquet').mode('overwrite').save(hdfs_url)
        #     return True
        # except Exception as e:
        #     raise Exception("Cannot store dataframe: "+str(e))

    def write_spark_dataframe(self, stream_name, data):
        hdfs_url = self._get_storage_path(stream_name)
        try:
            data.write.partitionBy(["version","user"]).format('parquet').mode('overwrite').save(hdfs_url)
            return True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))

    def write_pandas_dataframe(self, stream_name, data):
        try:
            hdfs_url = self._get_storage_path(stream_name)
            table = pa.Table.from_pandas(data)
            pq.write_to_dataset(table, root_path=hdfs_url, partition_cols=["version", "user"], preserve_index=False)
            return True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))

    def _get_storage_path(self, stream_name:str)->str:
        """
        Build path of storage location

        Args:
            stream_name (str): name of a stream
        Returns:
            str: storage location path

        """
        storage_url = self.obj.filesystem_path

        if stream_name is None or stream_name=="":
            return storage_url
        else:
            return storage_url + "stream=" + stream_name + "/"
