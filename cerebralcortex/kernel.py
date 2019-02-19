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

import uuid
import warnings
from datetime import datetime
from typing import List

from mprov.metadata.stream_metadata import BasicTuple
from pyspark.sql import SparkSession

from cerebralcortex.core.config_manager.config import Configuration
from cerebralcortex.core.data_manager.object.data import ObjectData
from cerebralcortex.core.data_manager.raw.data import RawData
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.core.data_manager.time_series.data import TimeSeriesData
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.log_manager.logging import CCLogging
from cerebralcortex.core.messaging_manager.messaging_queue import MessagingQueue
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.modules.mprov.connection.mprov_connection import MProvConnection


class Kernel:

    def __init__(self, configs_dir_path: str = None, auto_offset_reset: str = "largest", enable_spark:bool=True):
        """
        CerebralCortex constructor

        Args:
            configuration_filepath (str): Directory path of cerebralcortex configurations.
            auto_offset_reset (str): Kafka offset. Acceptable parameters are smallest or largest (default=largest)
        Raises:
            ValueError: If configuration_filepath is None or empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
        """
        if configs_dir_path is None or configs_dir_path == "":
            raise ValueError("config_file path cannot be None or blank.")

        if enable_spark:
            self.sparkSession = SparkSession.builder.appName("CerebralCortex").getOrCreate()
        else:
            self.sparkSession = None

        self.config_filepath = configs_dir_path
        self.config = Configuration(configs_dir_path).config

        self.debug = self.config["cc"]["debug"]
        self.logging = CCLogging(self)
        self.logtypes = LogTypes()
        self.SqlData = SqlData(self)
        self.RawData = RawData(self)
        self.ObjectData = ObjectData(self)
        self.MessagingQueue = None
        self.TimeSeriesData = None

        warnings.simplefilter('always', DeprecationWarning)



        if self.config["visualization_storage"] != "none":
            self.TimeSeriesData = TimeSeriesData(self)

        if self.config["messaging_service"] != "none":
            self.MessagingQueue = MessagingQueue(self, auto_offset_reset)

        if self.config["provenance"] != "none":
            self.MProvConnection = MProvConnection(self)

    ###########################################################################
    #                     RAW DATA MANAGER METHODS                            #
    ###########################################################################
    def save_stream(self, datastream: DataStream, ingestInfluxDB: bool = False)->bool:
        """
        Saves datastream raw data in selected NoSQL storage and metadata in MySQL.

        Args:
            datastream (DataStream): a DataStream object
            ingestInfluxDB (bool): Setting this to True will ingest the raw data in InfluxDB as well that could be used to visualize data in Grafana
        Returns:
            bool: True if stream is successfully stored or throws an exception
        Raises:
            Exception: log or throws exception if stream is not stored
        Todo:
            Add functionality to store data in influxdb.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_stream(ds)
        """
        return self.RawData.save_stream(datastream=datastream, ingestInfluxDB=ingestInfluxDB)

    def get_stream(self, stream_name: str, version: str = "all", data_type=DataSet.COMPLETE) -> DataStream:
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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> ds = CC.get_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ds.data # an object of a dataframe
            >>> ds.metadata # an object of MetaData class
            >>> ds.get_metadata(version=1) # get the specific version metadata of a stream
        """

        return self.RawData.get_stream(stream_name=stream_name, version=version, data_type=data_type)

    ###########################################################################
    #                     TIME SERIES DATA MANAGER METHODS                    #
    ###########################################################################

    def save_data_to_influxdb(self, datastream: DataStream):
        """
        Save data stream to influxdb only for visualization purposes.

        Args:
            datastream (DataStream): a DataStream object
        Returns:
            bool: True if data is ingested successfully or False otherwise
        Todo:
            This needs to be updated with the new structure. Should metadata be stored or not?
        Example:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_data_to_influxdb(ds)
        """
        self.TimeSeriesData.save_data_to_influxdb(datastream)

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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.is_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> True
        """
        return self.SqlData.is_stream(stream_name)

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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_stream_versions("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> [1, 2, 4]
        """
        return self.SqlData.get_stream_versions(stream_name)

    def get_stream_name(self, metadata_hash: uuid) -> str:
        """
        metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

        Args:
            metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
        Returns:
            str: name of a stream
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
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
            list[str]: list of all the metadata hashes
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ["00ab666c-afb8-476e-9872-6472b4e66b68", "15cc444c-dfb8-676e-3872-8472b4e66b12"]
        """
        return self.SqlData.get_stream_metadata_hash(stream_name)

    def get_stream_metadata(self, stream_name: str, version:str="all") -> List[Metadata]:
        """
        Get a list of metadata for all versions available for a stream.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")

        Returns:
            list[Metadata]: Returns an empty list if no metadata is available for a stream_name or a list of metadata otherwise.
        Raises:
            ValueError: stream_name cannot be None or empty.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [Metadata] # list of MetaData class objects
        """
        return self.SqlData.get_stream_metadata(stream_name, version)

    def get_stream_info_by_hash(self, metadata_hash: uuid) -> str:
        """
           metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.

           Args:
               metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
           Returns:
               dict: stream metadata and other info related to a stream
           Examples:
               >>> CC = Kernel("/directory/path/of/configs/")
               >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
               >>> {"name": .....} # stream metadata and other information
       """
        return self.SqlData.get_stream_info_by_hash(metadata_hash=metadata_hash)

    def list_streams(self)->List[Metadata]:
        """
        Get all the available stream names with metadata

        Returns:
            List[Metadata]: list of available streams metadata

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.list_streams()
        """
        return self.SqlData.list_streams()

    def search_stream(self, stream_name):
        """
        Find all the stream names similar to stream_name arg. For example, passing "location"
        argument will return all stream names that contain the word location

        Returns:
            List[str]: list of stream names similar to stream_name arg

        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.search_stream("battery")
            >>> ["BATTERY--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST", "BATTERY--org.md2k.phonesensor--PHONE".....]
        """

        return self.SqlData.search_stream(stream_name=stream_name)

    ################### USER RELATED METHODS ##################################

    def create_user(self, username:str, user_password:str, user_role:str, user_metadata:dict, user_settings:dict)->bool:
        """
        Create a user in SQL storage if it doesn't exist

        Args:
            username (str): Only alphanumeric usernames are allowed with the max length of 25 chars.
            user_password (str): no size limit on password
            user_role (str): role of a user
            user_metadata (dict): metadata of a user
            user_settings (dict): user settings, mCerebrum configurations of a user
        Returns:
            bool: True if user is successfully registered or throws any error in case of failure
        Raises:
            ValueError: if selected username is not available
            Exception: if sql query fails
        """
        return self.SqlData.create_user( username, user_password, user_role, user_metadata, user_settings)

    def delete_user(self, username:str)->bool:
        """
        Delete a user record in SQL table

        Args:
            username: username of a user that needs to be deleted
        Returns:
            bool: if user is successfully removed
        Raises:
            ValueError: if username param is empty or None
            Exception: if sql query fails
        """
        return self.SqlData.delete_user(username)

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
            >>> CC = Kernel("/directory/path/of/configs/")
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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_user_id("nasir_ali")
            >>> '76cc444c-4fb8-776e-2872-9472b4e66b16'
        """
        return self.SqlData.get_user_id(user_name)

    def get_user_name(self, user_id: str) -> str:
        """
        Get the user name linked to a user id.

        Args:
            user_name (str): username of a user
        Returns:
            bool: user_id associated to username
        Raises:
            ValueError: User ID is a required field.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_user_name("76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> 'nasir_ali'
        """
        return self.SqlData.get_user_name(user_id)

    def get_all_users(self, study_name: str) -> List[dict]:
        """
        Get a list of all users part of a study.

        Args:
            study_name (str): name of a study
        Raises:
            ValueError: Study name is a requied field.
        Returns:
            list[dict]: Returns empty list if there is no user associated to the study_name and/or study_name does not exist.
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [{"76cc444c-4fb8-776e-2872-9472b4e66b16": "nasir_ali"}] # [{user_id, user_name}]
        """
        return self.SqlData.get_all_users(study_name)

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
            >>> CC = Kernel("/directory/path/of/configs/")
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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_user_settings(username="nasir_ali")
            >>> [{"mcerebrum":"some-conf"........}]
        """
        return self.SqlData.get_user_settings(username, auth_token)

    def connect(self, username: str, password: str, encrypted_password:bool=False) -> dict:
        """
        Authenticate a user based on username and password and return an auth token

        Args:
            username (str):  username of a user
            password (str): password of a user
            encrypted_password (str): is password encrypted or not. mCerebrum sends encrypted passwords
        Raises:
            ValueError: User name and password cannot be empty/None.
        Returns:
            dict: return eturn {"status":bool, "auth_token": str, "msg": str}
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.connect("nasir_ali", "2ksdfhoi2r2ljndf823hlkf8234hohwef0234hlkjwer98u234", True)
            >>> True
        """
        return self.SqlData.login_user(username, password, encrypted_password)

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

    ################### KAFKA RELATED METHODS ##################################

    def store_or_update_Kafka_offset(self, topic: str, topic_partition: str, offset_start: str, offset_until: str)->bool:
        """
        Store or Update kafka topic offsets. Offsets are used to track what messages have been processed.

        Args:
            topic (str): name of the kafka topic
            topic_partition (str): partition number
            offset_start (str): starting of offset
            offset_until (str): last processed offset
        Raises:
            ValueError: All params are required.
            Exception: Cannot add/update kafka offsets because ERROR-MESSAGE
        Returns:
            bool: returns True if offsets are add/updated or throws an exception.

        """
        self.SqlData.store_or_update_Kafka_offset(topic, topic_partition, offset_start, offset_until)

    def get_kafka_offsets(self, topic: str) -> dict:
        """
        Get last stored kafka offsets

        Args:
            topic (str): kafka topic name

        Returns:
            list[dict]: list of kafka offsets. This method will return empty list if topic does not exist and/or no offset is stored for the topic.
        Raises:
            ValueError: Topic name cannot be empty/None
        Examples:
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.get_kafka_offsets("live-data")
            >>> [{"id","topic", "topic_partition", "offset_start", "offset_until", "offset_update_time"}]
        """
        return self.SqlData.get_kafka_offsets(topic)

    ###########################################################################
    #                      OBJECTS DATA MANAGER METHODS                       #
    ###########################################################################

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
            >>> CC = Kernel("/directory/path/of/configs/")
            >>> CC.create_bucket("live_data_folder")
            >>> True
        """
        return self.ObjectData.create_bucket(bucket_name)

    def upload_object(self, bucket_name: str, object_name: str, object_filepath:str) -> bool:
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

        return self.ObjectData.upload_object(bucket_name, object_name, object_filepath)

    def get_buckets(self) -> dict:
        """
        returns all available buckets in an object storage

        Returns:
            dict: {bucket-name: str, [{"key":"value"}]}, in case of an error {"error": str}

        """
        return self.ObjectData.get_buckets()

    def get_bucket_objects(self, bucket_name: str) -> dict:
        """
        returns a list of all objects stored in the specified Minio bucket

        Args:
            bucket_name (str): name of the bucket aka folder
        Returns:
            dict: {bucket-objects: [{"object_name":"", "metadata": {}}...],  in case of an error {"error": str}
        """
        return self.ObjectData.get_bucket_objects(bucket_name)

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
        return self.ObjectData.get_object_stats(bucket_name, object_name)

    def get_object(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns stored object (HttpResponse)

        Args:
            bucket_name (str): name of a bucket aka folder
            object_name (str): name of an object that needs to be downloaded
        Returns:
            file-object: object that needs to be downloaded. If file does not exists then it returns an error {"error": "File does not exist."}
        Raises:
            ValueError: Missing bucket_name and object_name params.
            Exception: {"error": "error-message"}
        """
        return self.ObjectData.get_object(bucket_name, object_name)

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
        return self.ObjectData.is_bucket(bucket_name)

    def is_object(self, bucket_name: str, object_name: str) -> bool:
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
        return self.ObjectData.is_object(bucket_name, object_name)


    ###########################################################################
    #                      Kafka consumer producer                            #
    ###########################################################################

    def kafka_produce_message(self, topic: str, msg: dict):
        """
        Publish a message on kafka message queue

        Args:
            topic (str): name of the kafka topic
            msg (dict): message that needs to published on kafka
        Returns:
            bool: True if successful. In case of failure, it returns an Exception message.
        Raises:
            ValueError: topic and message parameters cannot be empty or None.
            Exception: Error publishing message. Topic: topic_name - error-message

        """
        self.MessagingQueue.produce_message(topic, msg)

    def kafka_subscribe_to_topic(self, topic: str):
        """
        Subscribe to kafka topic as a consumer

        Args:
            topic (str): name of the kafka topic
        Yields:
             dict: kafka message
        Raises:
            ValueError: Topic parameter is missing.
        """
        return self.MessagingQueue.subscribe_to_topic(topic)

    ################### CACHE RELATED METHODS ##################################

    def set_cache_value(self, key: str, value: str) -> bool:
        """
        Creates a new cache entry in the cache. Values are overwritten for existing keys.

        Args:
            key: key in the cache
            value: value associated with the key
        Returns:
            bool: True on successful insert or False otherwise.
        Raises:
            ValueError: if key is None or empty
        """
        return self.SqlData.set_cache_value(key, value)

    def get_cache_value(self, key: str) -> str:
        """
        Retrieves value from the cache for the given key.

        Args:
            key: key in the cache
        Returns:
            str: The value in the cache
        Raises:
            ValueError: if key is None or empty
        """
        return self.SqlData.get_cache_value(key)


    ###########################################################################
    #                      mProve Data Provenance                             #
    ###########################################################################

    def store_activity(self, activity: str, start: int, end: int, location: int):
        """
        Create an entity node for an activity (a stream operator computation)

        Args:
            activity: Name of the operation
            start: Start time
            end: End time
            location: Index position etc
        """
        self.MProvConnection.store_activity(activity=activity, start=start, end=end, location=location)

    def store_stream_tuple(self, stream_name: str, stream_index: int, input_tuple: BasicTuple):
        """
        Create an entity node for a stream tuple

        Args:
            stream_name: The name of the stream itself
            stream_index: The index position (count) or timestamp (if unique)
            input_tuple: The actual stream value
        Returns:
            token for the new node
        """
        self.MProvConnection.store_stream_tuple(stream_name=stream_name, stream_index=stream_index, input_tuple=input_tuple)

    def store_annotation(self, stream_name: str, stream_index: int, annotation_name: str, annotation_value):
        """
        Create a node for an annotation to an entity / tuple

        Args:
            stream_name: The name of the stream itself
            stream_index: The index position (count) or timestamp (if unique) of the stream element we are annotating
            annotation_name: The name of the annotation
            annotation_value: The value of the annotation
        """
        self.MProvConnection.store_annotation(stream_name=stream_name, stream_index=stream_index, annotation_name=annotation_name, annotation_value=annotation_value)

    def store_window_and_inputs(self, output_stream_name: str, output_stream_index: int, input_tokens_list: list ):
        """
        Store a mapping between an operator window, from
        which a stream is to be derived, and the input
        nodes

        Args:
            output_stream_name:
            output_stream_index:
            input_tokens_list:
        """
        self.MProvConnection.store_window_and_inputs(output_stream_name=output_stream_name, output_stream_index=output_stream_index, input_tokens_list=input_tokens_list)

    def store_windowed_result(self, output_stream_name: str, output_stream_index: int, output_tuple: BasicTuple, input_tokens_list: list, activity: str, start: int, end: int ):
        """
        When we have a windowed computation, this creates a complex derivation subgraph
        in one operation.

        Args:
            output_stream_name: The name of the stream our operator produces
            output_stream_index: The position of the outgoing tuple in the stream
            output_tuple: The tuple itself
            input_tokens_list: IDs of the inputs to the computation
            activity: The computation name
            start: Start time
            end: End time
        """
        self.MProvConnection.store_windowed_result(output_stream_name=output_stream_name, output_stream_index=output_stream_index, output_tuple=output_tuple, input_tokens_list=input_tokens_list, activity=activity, start=start, end=end)
