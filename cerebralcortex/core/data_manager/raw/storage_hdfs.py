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

from cerebralcortex.core.datatypes import DataStream


class HDFSStorage:

    def __init__(self, obj):
        """
        Constructor

        Args:
            obj (object): Object of Data class
        """
        self.obj = obj

    def read_file(self, stream_name:str, version:str="all"):
        """
        Get stream data from storage system. Data would be return as pyspark DataFrame object
        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")
        Note:
            Please specify a version if you know the exact version of a stream. Getting all the stream data and then filtering versions won't be efficient.

        Returns:
            object: pyspark DataFrame object
        Raises:
            Exception: if stream name does not exist.
        """
        if version=="all":
            hdfs_url = self._get_storage_path(stream_name)
            df = self.obj.sparkSession.read.load(hdfs_url)
            return df
        else:
            hdfs_url = self._get_storage_path(stream_name)
            hdfs_url = hdfs_url+"version="+str(version)
            df = self.obj.sparkSession.read.load(hdfs_url)
            df = df.withColumn('version', lit(int(version)))
            return df

    def write_file(self, stream_name:str, data:DataStream.data) -> bool:
        """
        Write pyspark DataFrame to HDFS

        Args:
            stream_name (str): name of the stream
            data (object): pyspark DataFrame object

        Returns:
            bool: True if data is stored successfully or throws an Exception.
        Raises:
            Exception: if DataFrame write operation fails
        """
        hdfs_url = self._get_storage_path(stream_name)
        try:
            #data.write.save(hdfs_url, format='parquet', mode='append')
            data.write.partitionBy(["version","user"]).format('parquet').mode('overwrite').save(hdfs_url)
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
        storage_url = self.obj.hdfs_spark_url + self.obj.raw_files_dir

        if stream_name is None or stream_name=="":
            return storage_url
        else:
            return storage_url + "stream=" + stream_name + "/"
