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


from pyspark.sql.functions import lit

class FileSystemStorage():

    def __init__(self, obj):
        self.obj = obj

    def read_file(self, stream_name:str, version:str="all"):

        if version=="all":
            hdfs_url = self.get_storage_path(stream_name)
            df = self.obj.sparkSession.read.load(hdfs_url)
            return df
        else:
            hdfs_url = self.get_storage_path(stream_name)
            hdfs_url = hdfs_url+"ver="+str(version)
            df = self.obj.sparkSession.read.load(hdfs_url)
            df = df.withColumn('ver', lit(int(version)))
            return df

    def write_file(self, stream_name, data) -> bool:

        hdfs_url = self.get_storage_path(stream_name)
        try:
            data.write.partitionBy(["ver","user"]).format('parquet').mode('overwrite').save(hdfs_url)
            return True
        except Exception as e:
            raise Exception("Cannot store dataframe: "+str(e))

    def get_storage_path(self, stream_name:str):
        storage_url = self.obj.filesystem_path

        if stream_name is None or stream_name=="":
            raise ValueError("Stream name cannot be empty.")
        else:
            storage_url = storage_url + "stream=" + stream_name + "/"
        return storage_url
