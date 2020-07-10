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

class tmp:
    def get_storage_path(self, dirpath, stream_name, version, user_id):

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

class filesystem_helper:
    def path_exist(self, dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Checks if a path exist

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            bool: true if path exist, false otherwise
        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        if os.path.exists(storage_path):
            return True
        else:
            return False
        
    def ls_dir(self, dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        List the contents of a directory

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            list[str]: list of file and/or dir names
        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        return os.listdir(storage_path)
    
    def create_dir(self,dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Creates a directory if it does not exist.

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        if not os.path.exists(storage_path):
            os.mkdir(storage_path)

class hdfs_helper:
    hdfs_conn = ""
    def path_exist(self, dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Checks if a path exist

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            bool: true if path exist, false otherwise
        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        if self.hdfs_conn.fs.exists(storage_path):
            return True
        else:
            return False

    def ls_dir(self, dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        List the contents of a directory

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        Returns:
            list[str]: list of file and/or dir names
        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        return self.hdfs_conn.ls(storage_path)

    def create_dir(self,dirpath:str, stream_name:str=None, version:int=None, user_id:str=None):
        """
        Creates a directory if it does not exist.

        Args:
            dirpath (str): base storage dir path
            stream_name (str): name of a stream
            version (int): version number of stream data
            user_id (str): uuid of a user

        """
        storage_path = self.get_storage_path(dirpath=dirpath, stream_name=stream_name, version=version, user_id=user_id)
        if not os.path.exists(storage_path):
            self.hdfs_conn.mkdir(storage_path)