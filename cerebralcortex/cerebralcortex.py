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

from pyspark.sql import SparkSession

from cerebralcortex.core.config_manager.config import Configuration
from cerebralcortex.core.data_manager.object.data import ObjectData
from cerebralcortex.core.data_manager.raw.data import RawData
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.core.data_manager.time_series.data import TimeSeriesData
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.file_manager.file_io import FileIO
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.log_manager.logging import CCLogging
from cerebralcortex.core.messaging_manager.messaging_queue import MessagingQueue
from cerebralcortex.core.metadata_manager.metadata import Metadata


class CerebralCortex:

    def __init__(self, configuration_filepath: str = None, auto_offset_reset: str = "largest"):
        """
        CerebralCortex constructor
        Args:
            configuration_filepath (str): Directory path of cerebralcortex configurations.
            auto_offset_reset (str): Kafka offset. Acceptable parameters are smallest or largest (default=largest)
        Raises:
            ValueError: If configuration_filepath is None or empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
        """
        if configuration_filepath is None or configuration_filepath == "":
            raise ValueError("config_file path cannot be None or blank.")

        self.config_filepath = configuration_filepath
        self.config = Configuration(configuration_filepath).config
        self.sparkSession = SparkSession.builder.appName("CerebralCortex").getOrCreate()
        self.debug = self.config["cc"]["debug"]
        self.logging = CCLogging(self)
        self.logtypes = LogTypes()
        self.SqlData = SqlData(self)
        self.RawData = RawData(self)
        self.MessagingQueue = None
        self.TimeSeriesData = None
        self.FileIO = FileIO(self)

        warnings.simplefilter('always', DeprecationWarning)

        if self.config["visualization_storage"] != "none":
            self.TimeSeriesData = TimeSeriesData(self)

        if self.config["messaging_service"] != "none":
            self.MessagingQueue = MessagingQueue(self, auto_offset_reset)

        if "minio" in self.config:
            self.ObjectData = ObjectData(self)

    ###########################################################################
    #                     RAW DATA MANAGER METHODS                            #
    ###########################################################################
    def save_stream(self, datastream: DataStream, ingestInfluxDB: bool = False):
        """
        Saves datastream raw data in selected NoSQL storage and metadata in MySQL.

        Args:
            datastream (DataStream): a DataStream object
            ingestInfluxDB (bool): Setting this to True will ingest the raw data in InfluxDB as well that could be used to visualize data in Grafana
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_stream(ds)
        """
        self.RawData.save_stream(datastream=datastream, ingestInfluxDB=ingestInfluxDB)

    def get_stream(self, stream_name: str, version: str = "all", data_type=DataSet.COMPLETE) -> DataStream:
        """
        Retrieve a data-stream with it's metadata.

        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")
            data_type (DataSet):  DataSet.COMPLETE returns both Data and Metadata. DataSet.ONLY_DATA returns only Data. DataSet.ONLY_METADATA returns only metadata of a stream. (Default=DataSet.COMPLETE)

        Returns:
            DataStream: contains Data and/or metadata
        Notes:
            Please specify a version if you know the exact version of a stream. Getting all the stream data and then filtering versions won't be efficient.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = CC.get_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ds.data # an object of a dataframe
            >>> ds.metadata # an object of MetaData class
            >>> ds.get_metadata(version=1) # get the specific version metadata of a stream
        """

        return self.RawData.get_stream(stream_name=stream_name, version=version, data_type=data_type)

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
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.is_stream("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> True
        """
        return self.SqlData.is_stream(stream_name)

    def get_stream_name(self, metadata_hash: uuid) -> str:
        """
        metadata_hash are unique to each stream version. This reverse look can return the stream name of a metadata_hash.
        Args:
            metadata_hash (uuid): This could be an actual uuid object or a string form of uuid.
        Returns:
            str: name of a stream
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_stream_name("00ab666c-afb8-476e-9872-6472b4e66b68")
            >>> ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST
        """
        return self.SqlData.get_stream_name(metadata_hash)

    def get_metadata_hash(self, stream_name: str) -> list:
        """
        Get all the metadata_hash associated with a stream name.
        Args:
            stream_name (str): name of a stream
        Returns:
            list(str): list of all the metadata hashes
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_metadata_hash("ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST")
            >>> ["00ab666c-afb8-476e-9872-6472b4e66b68", "15cc444c-dfb8-676e-3872-8472b4e66b12"]
        """
        return self.SqlData.get_stream_metadata_hash(stream_name)

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
            >>> CC = CerebralCortex("/directory/path/of/configs/")
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
            >>> CC = CerebralCortex("/directory/path/of/configs/")
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
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_name("76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> 'nasir_ali'
        """
        return self.SqlData.get_user_name(user_id)

    def get_all_users(self, study_name: str) -> list[dict]:
        """
        Get a list of all users part of a study.
        Args:
            study_name (str): name of a study
        Raises:
            ValueError: Study name is a requied field.
        Returns:
            list(dict): Returns empty list if there is no user associated to the study_name and/or study_name does not exist.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [{"76cc444c-4fb8-776e-2872-9472b4e66b16": "nasir_ali"}] # [{user_id, user_name}]
        """
        return self.SqlData.get_all_users(study_name)

    def get_stream_metadata(self, stream_name: str, version:str="all") -> list(Metadata):
        """
        Get a list of metadata for all versions available for a stream.
        Args:
            stream_name (str): name of a stream
            version (str): version of a stream. Acceptable parameters are all, latest, or a specific version of a stream (e.g., 2.0) (Default="all")

        Returns:
            list(Metadata): Returns an empty list if no metadata is available for a stream_name or a list of metadata otherwise.
        Raises:
            ValueError: stream_name cannot be None or empty.
        Todo:
            this shall return a list of Metadata objects
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_all_users("mperf")
            >>> [Metadata] # list of MetaData class objects
        """
        return self.SqlData.get_stream_metadata_by_name(stream_name, version)

    ################### USER RELATED METHODS ##################################

    def get_user_metadata(self, user_id: uuid = None, username: str = None) -> List:
        """

        :param user_id:
        :param username:
        :return:
        """
        return self.SqlData.get_user_metadata(user_id, username)

    def connect(self, username: str, password: str) -> bool:
        """

        :param username:
        :param password:
        :return:
        """
        return self.SqlData.login_user(username, password)

    def is_auth_token_valid(self, token_owner: str, auth_token: str, auth_token_expiry_time: datetime) -> bool:
        """

        :param token_owner:
        :param auth_token:
        :param auth_token_expiry_time:
        :return:
        """
        return self.SqlData.is_auth_token_valid(token_owner, auth_token, auth_token_expiry_time)

    def update_auth_token(self, username: str, auth_token: str, auth_token_issued_time: datetime,
                          auth_token_expiry_time: datetime) -> str:
        """

        :param username:
        :param auth_token:
        :param auth_token_issued_time:
        :param auth_token_expiry_time:
        :return uuid of the current user
        """
        return self.SqlData.update_auth_token(username, auth_token, auth_token_issued_time, auth_token_expiry_time)

    def gen_random_pass(self, string_type: str, size: int = 8) -> str:
        """
        :param string_type:
        :param size:
        :return:
        """
        return self.SqlData.gen_random_pass(string_type, size)

    def encrypt_user_password(self, user_password: str) -> str:
        """
        :param user_password:
        :return:
        """
        self.SqlData.encrypt_user_password(user_password)

    ################### KAFKA RELATED METHODS ##################################

    def store_or_update_Kafka_offset(self, topic: str, topic_partition: str, offset_start: str, offset_until: str):
        """

        :param topic:
        :param topic_partition:
        :param offset_start:
        :param offset_until:
        """
        self.SqlData.store_or_update_Kafka_offset(topic, topic_partition, offset_start, offset_until)

    def get_kafka_offsets(self, topic: str) -> dict:
        """

        :param topic:
        :return:
        """
        return self.SqlData.get_kafka_offsets(topic)

    ###########################################################################
    ############### OBJECTS DATA MANAGER METHODS ##############################
    ###########################################################################

    def create_bucket(self, bucket_name: str) -> bool:
        """
        creates a bucket
        :param bucket_name:
        """
        return self.ObjectData.create_bucket(bucket_name)

    def upload_object(self, bucket_name: str, object_name: str, object_filepath: object) -> bool:
        """
        Uploads an object to Minio storage
        :param bucket_name:
        :param object_name:
        :param object_filepath: it shall contain full path of a file with file name (e.g., /home/nasir/obj.zip)
        :return: True/False, in case of an error {"error": str}
        """
        return self.ObjectData.upload_object(bucket_name, object_name, object_filepath)

    def upload_object_s3(self, bucket_name: str, object_name: str, object_: object, obj_size) -> bool:
        """
        Uploads an object to Minio storage
        :param bucket_name:
        :param object_name:
        :param object_: object that needs to be stored
        :param obj_size object size
        :return: True/False, in case of an error {"error": str}
        """
        return self.ObjectData.upload_object_to_s3(bucket_name, object_name, object_, obj_size)

    def get_buckets(self) -> List:
        """
        returns all available buckets in Minio storage
        :return: [{bucket-name: str, last_modified: str}], in case of an error [{"error": str}]
        """
        return self.ObjectData.get_buckets()

    def get_bucket_objects(self, bucket_name: str) -> List:
        """
        returns a list of all objects stored in the specified Minio bucket
        :param bucket_name:
        :return:{object-name:{stat1:str, stat2, str}},  in case of an error [{"error": str}]
        """
        return self.ObjectData.get_bucket_objects(bucket_name)

    def get_object_stats(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns properties (e.g., object type, last modified etc.) of an object stored in a specified bucket
        :param bucket_name:
        :param object_name:
        :return: {stat1:str, stat2, str},  in case of an error {"error": str}
        """
        return self.ObjectData.get_object_stats(bucket_name, object_name)

    def get_object(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns stored object (HttpResponse)
        :param bucket_name:
        :param object_name:
        :return: object (HttpResponse), in case of an error {"error": str}
        """
        return self.ObjectData.get_object(bucket_name, object_name)

    def is_bucket(self, bucket_name: str) -> bool:
        """

        :param bucket_name:
        :return: True/False, in case of an error {"error": str}
        """
        return self.ObjectData.is_bucket(bucket_name)

    ###########################################################################
    ############### TIME SERIES DATA MANAGER METHODS ##########################
    ###########################################################################

    def store_data_to_influxdb(self, datastream: DataStream):
        """
        :param datastream:
        """
        self.TimeSeriesData.store_data_to_influxdb(datastream)

    ###########################################################################
    ############### IO MANAGER METHODS ########################################
    ###########################################################################

    def read_file(self, filepath: str) -> str:
        """

        :param filepath:
        :return:
        """
        return self.FileIO.read_file(filepath)

    def file_processor(self, msg: dict, zip_filepath: str) -> DataStream:
        """
        :param msg:
        :param zip_filepath:
        :return:
        """
        return self.FileIO.file_processor(msg, zip_filepath)

    def get_gzip_file_contents(self, filepath: str) -> str:
        """
        Read and return gzip compressed file contents
        :param filepath:
        :return:
        """
        self.FileIO.get_gzip_file_contents(filepath)

    #################################################
    #   Kafka consumer producer
    #################################################

    def kafka_produce_message(self, topic: str, msg: str):
        """

        :param topic:
        :param msg:
        """
        try:
            self.MessagingQueue.produce_message(topic, msg)
        except Exception as e:
            raise Exception("Error publishing message. Topic: " + str(topic) + " - " + str(e))

    def kafka_subscribe_to_topic(self, topic: str):
        """

        :param topic:
        :param auto_offset_reset:
        """
        return self.MessagingQueue.subscribe_to_topic(topic)

    ################### CACHE RELATED METHODS ##################################

    def set_cache_value(self, key: str, value: str) -> bool:
        """
        Creates a new cache entry in the cache. Values are overwritten for
        existing keys.
        :param key: key in the cache
        :param value: value associated with the key
        :return True on successful insert
        :rtype bool
        """
        return self.SqlData.set_cache_value(key, value)

    def get_cache_value(self, key: str) -> str:
        """
        Retrieves value from the cache for the given key.
        :param key: key in the cache
        :return The value in the cache
        :rtype str
        """
        return self.SqlData.get_cache_value(key)
