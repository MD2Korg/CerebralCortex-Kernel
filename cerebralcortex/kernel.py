# Copyright (c) 2020, MD2K Center of Excellence
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
import uuid
import warnings
from datetime import datetime
from typing import List
from pyspark.sql import types as T
from cerebralcortex.core.config_manager.config import Configuration
from cerebralcortex.core.data_manager.raw.data import RawData
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.core.data_manager.time_series.data import TimeSeriesData
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.log_manager.logging import CCLogging
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.version import __version__

class Kernel:

    def __init__(self, configs_dir_path: str="", cc_configs:dict=None, study_name:str="default", new_study:bool=False, enable_spark:bool=True, enable_spark_ui=False):
        """
        CerebralCortex constructor

        Args:
            configs_dir_path (str): Directory path of cerebralcortex configurations.
            cc_configs (dict or str): if sets to cc_configs="default" all defaults configs would be loaded. Or you can provide a dict of all available cc_configs as a param
            study_name (str): name of the study. If there is no study, you can pass study name as study_name="default"
            new_study (bool): create a new study with study_name if it does not exist
            enable_spark (bool): enable spark
            enable_spark_ui (bool): enable spark ui
        Raises:
            ValueError: If configuration_filepath is None or empty.
        Examples:
            >>> CC = Kernel(cc_configs="default", study_name="default")
            >>> # if you want to change any of the configs, pass cc_configs as dict with new configurations
            >>> updated_cc_configs = {"nosql_storage": "filesystem", "filesystem_path": "/path/to/store/data/"}
            >>> CC = Kernel(cc_configs=updated_cc_configs, study_name="default")
            >>> # for complete configs, have a look at default configs at: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/3.3/cerebralcortex/core/config_manager/default.yml
        """
        try:
            if not os.getenv("PYSPARK_PYTHON"):
                os.environ["PYSPARK_PYTHON"] = os.environ['_']
            if not os.getenv("PYSPARK_DRIVER_PYTHON"):
                os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ['_']
        except:
            raise Exception("Please set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON environment variable. For example, export PYSPARK_DRIVER_PYTHON=/path/to/python/dir")

        if not configs_dir_path and not cc_configs:
            raise ValueError("Please provide configs_dir_path or cc_configs.")
        elif configs_dir_path and cc_configs:
            raise ValueError("Provide only configs_dir_path OR cc_configs.")

        self.version = __version__
        self.config_filepath = configs_dir_path
        self.study_name = study_name
        os.environ["STUDY_NAME"] = study_name
        self.config = Configuration(configs_dir_path, cc_configs).config

        if enable_spark:
            self.sparkContext = get_or_create_sc(enable_spark_ui=enable_spark_ui)
            self.sqlContext = get_or_create_sc(type="sqlContext", enable_spark_ui=enable_spark_ui)
            self.sparkSession = get_or_create_sc(type="sparkSession", enable_spark_ui=enable_spark_ui)
        else:
            self.sparkContext = None
            self.sqlContext = None
            self.sparkSession = None

        if self.config["mprov"]=="pennprov":
            os.environ["MPROV_HOST"] = self.config["pennprov"]["host"]
            os.environ["MPROV_USER"] = self.config["pennprov"]["user"]
            os.environ["MPROV_PASSWORD"] = self.config["pennprov"]["password"]
            os.environ["ENABLE_MPROV"] = "True"
        elif self.config["mprov"]=="none":
            os.environ["ENABLE_MPROV"] = "False"
        else:
            raise ValueError("Please check cerebralcortex.yml file. mprov is not properly configured.")

        self.new_study = new_study

        if not study_name:
            raise Exception("Study name cannot be None.")

        self.debug = self.config["cc"]["debug"]
        self.logging = CCLogging(self)
        self.logtypes = LogTypes()
        self.SqlData = SqlData(self)
        self.RawData = RawData(self)
        self.TimeSeriesData = None


        warnings.simplefilter('always', DeprecationWarning)


        if not new_study and not self.RawData.is_study():
            raise Exception("Study name does not exist. If this is a new study set new_study param to True")

        if self.config["visualization_storage"] != "none":
            self.TimeSeriesData = TimeSeriesData(self)

    ###########################################################################
    #                     RAW DATA MANAGER METHODS                            #
    ###########################################################################
    def save_stream(self, datastream: DataStream, overwrite=False)->bool:
        """
        Saves datastream raw data in selected NoSQL storage and metadata in MySQL.

        Args:
            datastream (DataStream): a DataStream object
            overwrite (bool): if set to true, whole existing datastream data will be overwritten by new data
        Returns:
            bool: True if stream is successfully stored or throws an exception
        Raises:
            Exception: log or throws exception if stream is not stored
        Todo:
            Add functionality to store data in influxdb.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_stream(ds)
        """
        return self.RawData.save_stream(datastream=datastream, overwrite=overwrite)

    
    def get_stream(self, stream_name: str, version: str = "latest", user_id:str=None, data_type=DataSet.COMPLETE) -> DataStream:
        """
        Retrieve a data-stream with it's metadata.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")
            data_type (DataSet):  DataSet.COMPLETE returns both Data and Metadata. DataSet.ONLY_DATA returns only Data. DataSet.ONLY_METADATA returns only metadata of a stream. (Default=DataSet.COMPLETE)

        Returns:
            DataStream: contains Data and/or metadata
        Raises:
            ValueError: if stream name is empty or None
        Note:
            Please specify a version if you know the exact version of a stream. Getting all the stream data and then filtering versions won't be efficient.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> ds = CC.get_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ds.data # an object of a dataframe
            >>> ds.metadata # an object of MetaData class
            >>> ds.get_metadata(version=1) # get the specific version metadata of a stream
        """

        return self.RawData.get_stream(stream_name=stream_name, version=version, user_id=user_id, data_type=data_type)

    ###########################################################################
    #                     TIME SERIES DATA MANAGER METHODS                    #
    ###########################################################################

    # def save_data_to_influxdb(self, datastream: DataStream):
    #     """
    #     Save data stream to influxdb only for visualization purposes.
    #
    #     Args:
    #         datastream (DataStream): a DataStream object
    #     Returns:
    #         bool: True if data is ingested successfully or False otherwise
    #     Todo:
    #         This needs to be updated with the new structure. Should metadata be stored or not?
    #     Example:
    #         >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
    #         >>> ds = DataStream(dataframe, MetaData)
    #         >>> CC.save_data_to_influxdb(ds)
    #     """
    #     self.TimeSeriesData.save_data_to_influxdb(datastream)

    ###########################################################################
    #               SQL DATA MANAGER METHODS                                  #
    ###########################################################################

    ################### STREAM RELATED METHODS ################################

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
        return self.RawData.is_stream(stream_name)

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
        return self.RawData.get_stream_versions(stream_name)

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
        return self.SqlData.get_stream_name(metadata_hash)

    def get_stream_metadata_hash(self, stream_name: str) -> list:
        """
        Get all the metadata_hash associated with a stream name.

        Args:
            stream_name (str): name of a stream
        Returns:
            list: list of all the metadata hashes with name and versions
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_stream_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> [["stream_name", "version", "metadata_hash"]]
        """
        return self.SqlData.get_stream_metadata_hash(stream_name)

    def get_stream_metadata_by_name(self, stream_name: str, version:str=1) -> List[Metadata]:
        """
        Get a list of metadata for all versions available for a stream.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")

        Returns:
            Metadata: Returns an empty list if no metadata is available for a stream_name or a list of metadata otherwise.
        Raises:
            ValueError: stream_name cannot be None or empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_stream_metadata_by_name("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST", version=1)
            >>> Metadata # list of MetaData class objects
        """
        return self.SqlData.get_stream_metadata_by_name(stream_name, version)

    def get_stream_metadata_by_hash(self, metadata_hash: uuid) -> str:
        """
           metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

           Args:
               metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
           Returns:
               List: [stream_name, metadata]
           Examples:
               >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
               >>> CC.get_stream_metadata_by_hash("00ab666c-afb8-476e-9872-6472b4e66b68")
               >>> ["name" .....] # stream metadata and other information
       """
        return self.SqlData.get_stream_metadata_by_hash(metadata_hash=metadata_hash)

    def list_streams(self)->List[str]:
        """
        Get all the available stream names with metadata

        Returns:
            List[str]: list of available streams metadata

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_streams()
        """
        return self.RawData.list_streams()

    def search_stream(self, stream_name):
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

        return self.RawData.search_stream(stream_name=stream_name)

    ################### USER RELATED METHODS ##################################

    def create_user(self, username:str, user_password:str, user_role:str, user_metadata:dict, user_settings:dict, encrypt_password:bool=False)->bool:
        """
        Create a user in SQL storage if it doesn't exist

        Args:
            username (str): Only alphanumeric usernames are allowed with the max length of 25 chars.
            user_password (str): no size limit on password
            user_role (str): role of a user
            user_metadata (dict): metadata of a user
            user_settings (dict): user settings, mCerebrum configurations of a user
            encrypt_password (bool): encrypt password if set to true
        Returns:
            bool: True if user is successfully registered or throws any error in case of failure
        Raises:
            ValueError: if selected username is not available
            Exception: if sql query fails
        """
        return self.SqlData.create_user(username, user_password, user_role, user_metadata, user_settings, encrypt_password)

    def is_user(self, user_id: str = None, user_name: str = None) -> bool:
        """
        Checks whether a user exists in the system. One of both parameters could be set to verify whether user exist.

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            bool: True if a user exists in the system or False otherwise.
        Raises:
            ValueError: Both user_id and user_name cannot be None or empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.is_user(user_id="76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> True
        """
        return self.SqlData.is_user(user_id, user_name)

    def get_user_id(self, user_name: str) -> str:
        """
        Get the user id linked to user_name.

        Args:
            user_name (str): username of a user
        Returns:
            str: user id associated to user_name
        Raises:
            ValueError: User name is a required field.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_user_id("nasir_ali")
            >>> '76cc444c-4fb8-776e-2872-9472b4e66b16'
        """
        return self.SqlData.get_user_id(user_name)

    def get_username(self, user_id: str) -> str:
        """
        Get the user name linked to a user id.

        Args:
            user_name (str): username of a user
        Returns:
            bool: user_id associated to username
        Raises:
            ValueError: User ID is a required field.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_username("76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> 'nasir_ali'
        """
        return self.SqlData.get_username(user_id)

    def list_users(self) -> List[dict]:
        """
        Get a list of all users part of a study.

        Args:
            study_name (str): name of a study. If no study_name is provided then all users' list will be returned
        Raises:
            ValueError: Study name is a requied field.
        Returns:
            list[dict]: Returns empty list if there is no user associated to the study_name and/or study_name does not exist.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.list_users()
            >>> [{"76cc444c-4fb8-776e-2872-9472b4e66b16": "nasir_ali"}] # [{user_id, user_name}]
        """
        return self.SqlData.list_users()

    def get_user_metadata(self, user_id: str = None, username: str = None)  -> dict:
        """
        Get user metadata by user_id or by username

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            dict: user metadata
        Todo:
            Return list of User class object
        Raises:
            ValueError: User ID/name cannot be empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_user_metadata(username="nasir_ali")
            >>> {"study_name":"mperf"........}
        """
        return self.SqlData.get_user_metadata(user_id, username)

    def get_user_settings(self, username: str=None, auth_token: str = None) -> dict:
        """
        Get user settings by auth-token or by username. These are user's mCerebrum settings

        Args:
            username (str): username of a user
            auth_token (str): auth-token
        Returns:
            list[dict]: List of dictionaries of user metadata
        Todo:
            Return list of User class object
        Raises:
            ValueError: User ID/name cannot be empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.get_user_settings(username="nasir_ali")
            >>> [{"mcerebrum":"some-conf"........}]
        """
        return self.SqlData.get_user_settings(username, auth_token)

    def connect(self, username: str, password: str, encrypt_password:bool=False) -> dict:
        """
        Authenticate a user based on username and password and return an auth token

        Args:
            username (str):  username of a user
            password (str): password of a user
            encrypt_password (str): is password encrypted or not. mCerebrum sends encrypted passwords
        Raises:
            ValueError: User name and password cannot be empty/None.
        Returns:
            dict: return eturn {"status":bool, "auth_token": str, "msg": str}
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/", study_name="default")
            >>> CC.connect("nasir_ali", "2ksdfhoi2r2ljndf823hlkf8234hohwef0234hlkjwer98u234", True)
            >>> True
        """
        return self.SqlData.login_user(username, password, encrypt_password)

    def is_auth_token_valid(self, username: str, auth_token: str, checktime:bool=False) -> bool:
        """
        Validate whether a token is valid or expired based on the token expiry datetime stored in SQL

        Args:
            username (str): username of a user
            auth_token (str): token generated by API-Server
            checktime (bool): setting this to False will only check if the token is available in system. Setting this to true will check if the token is expired based on the token expiry date.
        Raises:
            ValueError: Auth token and auth-token expiry time cannot be null/empty.
        Returns:
            bool: returns True if token is valid or False otherwise.
        """
        return self.SqlData.is_auth_token_valid(username, auth_token, checktime)

    def update_auth_token(self, username: str, auth_token: str, auth_token_issued_time: datetime,
                          auth_token_expiry_time: datetime) -> bool:
        """
        Update an auth token in SQL database to keep user stay logged in. Auth token valid duration can be changed in configuration files.

        Notes:
            This method is used by API-server to store newly created auth-token
        Args:
            username (str): username of a user
            auth_token (str): issued new auth token
            auth_token_issued_time (datetime): datetime when the old auth token was issue
            auth_token_expiry_time (datetime): datetime when the token will get expired
        Raises:
            ValueError: Auth token and auth-token issue/expiry time cannot be None/empty.
        Returns:
            bool: Returns True if the new auth token is set or False otherwise.

        """
        return self.SqlData.update_auth_token(username, auth_token, auth_token_issued_time, auth_token_expiry_time)

    def gen_random_pass(self, string_type: str="varchar", size: int = 8) -> str:
        """
        Generate a random password

        Args:
            string_type: Accepted parameters are "varchar" and "char". (Default="varchar")
            size: password length (default=8)

        Returns:
            str: random password

        """
        return self.SqlData.gen_random_pass(string_type, size)

    def encrypt_user_password(self, user_password: str) -> str:
        """
        Encrypt password

        Args:
            user_password (str): unencrypted password
        Raises:
             ValueError: password cannot be None or empty.
        Returns:
            str: encrypted password
        """
        return self.SqlData.encrypt_user_password(user_password)

    # ~~~~~~~~~~~~~~~~~~~~      Data Import ~~~~~~~~~~~~~~~~~~~~~~~ #

    def read_csv(self, file_path, stream_name:str, header:bool=False, delimiter:str=',', column_names:list=[], timestamp_column_index:int=0, timein:str="milliseconds", metadata:Metadata=None)->DataStream:
        """
        Reads a csv file (compressed or uncompressed), parse it, convert it into CC DataStream object format and returns it

        Args:
            file_path (str): path of the file
            stream_name (str): name of the stream
            header (bool): set it to True if csv contains header column
            delimiter (str): seprator used in csv file. Default is comma
            column_names (list[str]): list of column names
            timestamp_column_index (int): index of the timestamp column name
            timein (str): if timestamp is epoch time, provide whether it is in milliseconds or seconds
            metadata (Metadata): metadata object for the csv file

        Returns:
            DataStream object
        """
        if timein not in ["milliseconds", "seconds"]:
            raise Exception("timestamp can only be in milliseconds or seconds.")

        if column_names:
            df = self.sparkSession.read.options(inferschema='true', quote="'", delimiter=delimiter, header=header).csv(file_path).toDF(*column_names)

        else:
            df = self.sparkSession.read.options(inferschema='true', quote="'", delimiter=delimiter, header=header).csv(file_path)

        timestamp_column_name = df.schema[timestamp_column_index].name

        if timein=="milliseconds" and str(df.schema[timestamp_column_index].dataType)!="TimestampType":
            df = df.withColumn(timestamp_column_name, df[timestamp_column_name]/1000)

        parsed_df  = df.withColumn(timestamp_column_name, df[timestamp_column_name].cast(dataType=T.TimestampType()))

        if isinstance(metadata, Metadata) and metadata:
            mtadata = metadata
        else:
            mtadata = Metadata()

        ds = DataStream(data=parsed_df, metadata=mtadata)
        ds.metadata.set_name(stream_name)
        return ds