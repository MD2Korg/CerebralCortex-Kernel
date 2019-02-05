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

import json
import os
import traceback
from typing import List


class MinioHandler:
    """
    Todo:
        For now, Minio is disabled as CC config doesn't provide an option to use mutliple object-storage
    """
    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_buckets(self) -> List:
        """
        returns all available buckets in an object storage

        Returns:
            dict: {bucket-name: str, [{"key":"value"}]}, in case of an error {"error": str}

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

        Args:
            bucket_name (str): name of the bucket aka folder
        Returns:
            dict: {bucket-objects: [{"object_name":"", "metadata": {}}...],  in case of an error {"error": str}
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

        Args:
            bucket_name (str): name of a bucket aka folder
            object_name (str): name of an object
        Returns:
            dict: information of an object (e.g., creation_date, object_size etc.). In case of an error {"error": str}
        Raises:
            ValueError: Missing bucket_name and object_name params.
            Exception: {"error": "error-message"}
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

        Args:
            bucket_name (str): name of a bucket aka folder
            object_name (str): name of an object that needs to be downloaded
        Returns:
            file-object: object that needs to be downloaded. If file does not exists then it returns an error {"error": "File does not exist."}
        Raises:
            ValueError: Missing bucket_name and object_name params.
            Exception: {"error": "error-message"}
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
        checks whether a bucket exist
        Args:
            bucket_name (str): name of the bucket aka folder
        Returns:
            bool: True if bucket exist or False otherwise. In case an error {"error": str}
        Raises:
            ValueError: bucket_name cannot be None or empty.
        """
        try:
            return self.minioClient.bucket_exists(bucket_name)
        except Exception as e:
            raise e

    def is_object(self, bucket_name: str, object_name: str) -> dict:
        """
        checks whether an object exist in a bucket
        Args:
            bucket_name (str): name of the bucket aka folder
            object_name (str): name of the object
        Returns:
            bool: True if object exist or False otherwise. In case an error {"error": str}
        Raises:
            Excecption: if bucket_name and object_name are empty or None
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
        creates a bucket aka folder in object storage system.

        Args:
            bucket_name (str): name of the bucket
        Returns:
            bool: True if bucket was successfully created. On failure, returns an error with dict {"error":"error-message"}
        Raises:
            ValueError: Bucket name cannot be empty/None.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.create_bucket("live_data_folder")
            >>> True
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
        Upload an object in a bucket aka folder of object storage system.

        Args:
            bucket_name (str): name of the bucket
            object_name (str): name of the object to be uploaded
            object_filepath (str): it shall contain full path of a file with file name (e.g., /home/nasir/obj.zip)
        Returns:
            bool: True if object  successfully uploaded. On failure, returns an error with dict {"error":"error-message"}
        Raises:
            ValueError: Bucket name cannot be empty/None.
            Exception: if upload fails
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
        Upload an object in a bucket aka folder of object storage system.

        Args:
            bucket_name (str): name of the bucket
            object_name (str): name of the object to be uploaded
            file_data (object): object of a file
            obj_length (int): size of an object
        Returns:
            bool: True if object  successfully uploaded. On failure, throws an exception
        Raises:
            Exception: if upload fails
        """
        try:
            self.minioClient.put_object(bucket_name, object_name, file_data,
                                        obj_length, content_type='application/zip')
            return True
        except Exception as e:
            raise e
