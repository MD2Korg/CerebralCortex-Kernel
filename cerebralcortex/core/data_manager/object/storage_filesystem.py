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

import glob
import json
import os
import traceback


class FileSystemStorage():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_buckets(self) -> dict:
        """
        returns all available buckets in an object storage

        Returns:
            dict: {bucket-name: str, [{"key":"value"}]}, in case of an error {"error": str}

        """
        try:
            temp = []
            bucket_list = {}
            with os.scandir(self.filesystem_path) as dir_entries:
                for bucket in dir_entries:
                    info = self._get_file_info(bucket)
                    temp.append(info)
                bucket_list["buckets-list"] = temp
            return bucket_list
        except Exception as e:
            return {"error": str(e)}

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
            temp = []
            bucket_list = {}
            bucket_path = os.path.join(self.filesystem_path,bucket_name)
            with os.scandir(bucket_path) as dir_entries:
                for bucket in dir_entries:
                    if os.path.isfile(bucket.path) and bucket.name[-4:]=="json":
                        with open(bucket.path) as metadata_file:
                            metadata = metadata_file.read()
                            metadata = json.loads(metadata)

                        base_file_name = os.path.splitext(bucket.path)[0]
                        for infile in glob.glob( base_file_name+'.*' ):
                            if infile[-4:]!="json":
                                object_file = infile
                                break
                        object_stats = self._get_file_info(object_file, only_file_name=True)
                        metadata.update(object_stats)
                        temp.append({"object_name":object_file, "metadata": json.dumps(metadata)})

                bucket_list["bucket-objects"] = temp
            return bucket_list

        except Exception as e:
            objects_in_bucket["error"] = str(e)+" \n - Trace: "+str(traceback.format_exc())
            return {"error":objects_in_bucket}

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
            if not bucket_name or not object_name:
                raise ValueError("Missing bucket_name and object_name params.")
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            if os.path.isfile(object_path):
                metadata_file_path = os.path.splitext(object_path)[0]
                metadata_file_path = metadata_file_path+".json"

                with open(str(metadata_file_path)) as metadata_file:
                    metadata = metadata_file.read()
                    metadata = json.loads(metadata)
                object_stats = self._get_file_info(object_path, only_file_name=True)
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
            if not bucket_name or not object_name:
                raise ValueError("Missing bucket_name and object_name params.")
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
        checks whether a bucket exist
        Args:
            bucket_name (str): name of the bucket aka folder
        Returns:
            bool: True if bucket exist or False otherwise. In case an error {"error": str}
        Raises:
            ValueError: bucket_name cannot be None or empty.
        """
        try:
            if not bucket_name:
                raise ValueError("bucket_name cannot be None or empty.")
            object_path = os.path.join(self.filesystem_path,bucket_name)
            if os.path.isdir(object_path):
                return True
            else:
                return False
        except Exception as e:
            return {"error": str(e)}

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
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            if os.path.isfile(object_path):
                return True
            else:
                return False
        except Exception as e:
            return {"error": str(e)}

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
            raise ValueError("Bucket name cannot be empty/None.")
        try:
            new_bucket_path = os.path.join(self.filesystem_path,bucket_name)
            os.mkdir(new_bucket_path)
            return True
        except Exception as e:
            return {"error": str(e)}

    def upload_object(self, bucket_name: str, object_name: str, object_filepath: str) -> bool:
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
        """
        if not bucket_name or not object_name or not object_filepath:
            raise ValueError("All parameters are required.")
        try:
            object_path = os.path.join(self.filesystem_path,bucket_name, object_name)
            file_data = open(object_filepath, 'rb').read()
            with open(object_path, "wb+") as obj:
                obj.write(file_data)

            return True
        except Exception as e:
            return {"error": str(e)}

    def _get_file_info(self, dir_entry, only_file_name:bool=False)->dict:
        """
        Returns statistics of a file
        Args:
            dir_entry (DirEntry): object entry in a directory-scanner
            only_file_name (bool): if only file is passed

        Returns:
            dict: stats of a file

        """
        if only_file_name:
            info = os.stat(dir_entry)
            isdir = os.path.isdir(dir_entry)
            file_name = os.path.split(dir_entry)[1]
        else:
            info = dir_entry.stat()
            isdir = os.path.isdir(dir_entry.path)
            file_name = dir_entry.name
        if isdir:
            entry_type = "directory"
        else:
            entry_type = "file"
        return {"name":file_name,"creation_time":info.st_ctime, "modified_file": info.st_mtime, "access_time": info.st_atime, "file_size":info.st_size, "type":entry_type}
