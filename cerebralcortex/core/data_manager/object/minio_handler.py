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

import json
import os
from typing import List
import traceback

from minio.error import ResponseError


class MinioHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_buckets(self) -> List:
        """
        returns all available buckets in Minio storage
        :return: [{bucket-name: str, last_modified: str}], in case of an error [{"error": str}]
        :rtype: List
        """
        bucket_list = []
        try:
            temp = []
            bucket_list = {}
            buckets = self.minioClient.list_buckets()
            for bucket in buckets:
                temp.append({"bucket-name":bucket.name, "last_modified": str(bucket.creation_date)})
            bucket_list["buckets-list"] = temp
            return bucket_list
        except Exception as e:
            return [{"error": str(e)}]

    def get_bucket_objects(self, bucket_name: str) -> dict:
        """
        returns a list of all objects stored in the specified Minio bucket
        :param bucket_name:
        :return:{object-name:{stat1:str, stat2, str}},  in case of an error [{"error": str}]
        :rtype: dict
        """
        objects_in_bucket = {}
        try:
            objects = self.minioClient.list_objects(bucket_name, recursive=True)
            temp = []
            bucket_objects = {}
            for obj in objects:
                object_stat = self.minioClient.stat_object(obj.bucket_name, obj.object_name)
                object_stat = json.dumps(object_stat, default=lambda o: o.__dict__)
                object_stat = json.loads(object_stat)
                temp.append(object_stat)
                objects_in_bucket[obj.object_name] = object_stat
                object_stat.pop('metadata', None)
            bucket_objects["bucket-objects"] = temp
            return bucket_objects
        except Exception as e:
            objects_in_bucket["error"] = str(e)+" \n - Trace: "+str(traceback.format_exc())
            return objects_in_bucket

    def get_object_stats(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns properties (e.g., object type, last modified etc.) of an object stored in a specified bucket
        :param bucket_name:
        :param object_name:
        :return: {stat1:str, stat2, str},  in case of an error {"error": str}
        :rtype: dict
        """
        try:
            if self.is_bucket(bucket_name):
                object_stat = self.minioClient.stat_object(bucket_name, object_name)
                object_stat = json.dumps(object_stat, default=lambda o: o.__dict__)
                object_stat = json.loads(object_stat)
                return object_stat
            else:
                return [{"error": "Bucket does not exist"}]

        except Exception as e:
            return {"error": str(e)}

    def get_object(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns stored object (HttpResponse)
        :param bucket_name:
        :param object_name:
        :return: object (HttpResponse), in case of an error {"error": str}
        :rtype: dict
        """
        try:
            if self.is_bucket(bucket_name):
                return self.minioClient.get_object(bucket_name, object_name)
            else:
                return {"error": "Bucket does not exist"}

        except Exception as e:
            return {"error": str(e)}

    def is_bucket(self, bucket_name: str) -> bool:
        """

        :param bucket_name:
        :return: True/False
        :rtype: bool
        """
        try:
            return self.minioClient.bucket_exists(bucket_name)
        except Exception as e:
            raise e

    def is_object(self, bucket_name: str, object_name: str) -> dict:
        """
        Return True if object exists in a bucket, false otherwise
        :param bucket_name:
        :param object_name:
        :return: {stat1:str, stat2, str},  in case of an error {"error": str}
        :rtype: dict
        """        
        try:
            if self.is_bucket(bucket_name):
                self.minioClient.stat_object(bucket_name, object_name)
                return True
            else:
                return False
        except Exception as e:
            raise e

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def create_bucket(self, bucket_name: str) -> bool:
        """
        creates a new bucket
        :param bucket_name:
        :return: True/False
        :rtype: bool
        """
        if not bucket_name:
            raise ValueError("Bucket name cannot be empty")
        try:
            self.minioClient.make_bucket(bucket_name, location=self.CC.timezone)
            return True
        except Exception as e:
            raise e

    def upload_object(self, bucket_name: str, object_name: str, object_filepath: object) -> bool:
        """
        Uploads an object to Minio storage
        :param bucket_name:
        :param object_name:
        :param object_filepath: it shall contain full path of a file with file name (e.g., /home/nasir/obj.zip)
        :return: True/False
        :rtype: bool
        """
        if not object_filepath:
            raise ValueError("File name cannot be empty")
        try:
            file_stat = os.stat(object_filepath)
            file_data = open(object_filepath, 'rb')
            self.minioClient.put_object(bucket_name, object_name, file_data,
                                        file_stat.st_size, content_type='application/zip')
            return True
        except Exception as e:
            raise e

    def upload_object_to_s3(self, bucket_name: str, object_name: str, file_data: object, obj_length:int) -> bool:
        """
        Uploads an object to AWS-S3 storage
        :param bucket_name: 
        :param object_name: 
        :param file_data: 
        :param obj_length: 
        :return: 
        """
        try:
            self.minioClient.put_object(bucket_name, object_name, file_data,
                                        obj_length, content_type='application/zip')
            return True
        except Exception as e:
            raise e
            return False
