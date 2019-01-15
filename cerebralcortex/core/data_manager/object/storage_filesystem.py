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
import glob


class FileSystemStorage():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_buckets(self) -> dict:
        """
        returns all available buckets in storage system
        :return: [{bucket-name: str, last_modified: str}], in case of an error [{"error": str}]
        :rtype: List
        """
        try:
            temp = []
            bucket_list = {}
            with os.scandir(self.filesystem_path) as dir_entries:
                for bucket in dir_entries:
                    info = self.get_file_info(bucket)
                    temp.append(info)
                bucket_list["buckets-list"] = temp
            return bucket_list
        except Exception as e:
            return {"error": str(e)}

    def get_bucket_objects(self, bucket_name: str) -> dict:
        """
        returns a list of all objects stored in the specified bucket
        :param bucket_name:
        :return:{object-name:{stat1:str, stat2, str}},  in case of an error [{"error": str}]
        :rtype: dict
        """
        objects_in_bucket = {}
        try:
            temp = []
            bucket_list = {}
            with os.scandir(self.filesystem_path) as dir_entries:
                for bucket in dir_entries:
                    if os.path.isfile(bucket.path) and bucket.name[-3:]=="json":
                        with open(bucket.path) as metadata_file:
                            metadata = metadata_file.read()
                            metadata = json.loads(metadata)

                        base_file_name = os.path.splitext(bucket.path)[0]
                        for infile in glob.glob( base_file_name+'.*' ):
                            if infile[-3:]!="json":
                                object_file = infile
                                break
                        object_stats = self.get_file_info(object, only_file_name=True)
                        metadata.update(object_stats)
                        temp.append({"object_name":object_file, "metadata": json.dumps(metadata)})

                bucket_list["bucket-objects"] = temp
            return bucket_list

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
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            if os.path.isfile(object_path):
                metadata_file_path = os.path.splitext(object_path)[0]
                metadata_file_path = metadata_file_path+".json"

                with open(str(metadata_file_path).replace()) as metadata_file:
                    metadata = metadata_file.read()
                    metadata = json.loads(metadata)
                object_stats = self.get_file_info(object_path, only_file_name=True)
                metadata.update(object_stats)

                return metadata
            else:
                return {"error": "Bucket does not exist"}

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
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            if os.path.isfile(object_path):
                with open(object_path, "rb") as obj:
                    object_file = obj.read()
                return object_file
            else:
                return {"error": "File does not exist."}

        except Exception as e:
            return {"error": str(e)}

    def is_bucket(self, bucket_name: str) -> bool:
        """

        :param bucket_name:
        :return: True/False
        :rtype: bool
        """
        try:
            object_path = os.path.join(self.filesystem_path,bucket_name)
            if os.path.isdir(object_path):
                return True
            else:
                return False
        except Exception as e:
            raise {"error": str(e)}

    def is_object(self, bucket_name: str, object_name: str) -> dict:
        """
        Return True if object exists in a bucket, false otherwise
        :param bucket_name:
        :param object_name:
        :return: {stat1:str, stat2, str},  in case of an error {"error": str}
        :rtype: dict
        """        
        try:
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            if os.path.isfile(object_path):
                return True
            else:
                return False
        except Exception as e:
            raise {"error": str(e)}

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
            new_bucket_path = os.path.join(self.filesystem_path,bucket_name)
            os.mkdir(new_bucket_path)
            return True
        except Exception as e:
            raise {"error": str(e)}

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
            object_path = os.path.join(self.filesystem_path,bucket_name)
            if os.path.isdir(object_path):
                file_data = open(object_filepath, 'rb')
                with open(object_path, "wb") as obj:
                    obj.write(file_data)

            return True
        except Exception as e:
            raise {"error": str(e)}

    def get_file_info(self, dir_entry, only_file_name=False):
        if only_file_name:
            info = os.stat(dir_entry.path)
        else:
            info = dir_entry.stat()
        isdir = os.path.isdir(dir_entry.path)
        if isdir:
            entry_type = "directory"
        else:
            entry_type = "file"
        return {"file_name":dir_entry.name,"creation_time":info.st_ctime, "modified_file": info.st_mtime, "access_time": info.st_atime, "file_size":info.st_size, "type":entry_type}
