# Copyright (c) 2017, MD2K Center of Excellence
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
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import List

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from pytz import timezone

from core.data_manager.sql.data import SqlData
from core.datatypes.datapoint import DataPoint
from core.datatypes.datastream import DataStream
from core.util.data_types import convert_sample
from core.util.debuging_decorators import log_execution_time
from core.config_manager.config import Configuration
from core.data_manager.raw.data import RawData
from core.data_manager.raw.stream_handler import DataSet
from core.data_manager.sql.data import SqlData
from core.data_manager.time_series.data import TimeSeriesData


class CerebralCortex:
    def __init__(self, configuration_file, timezone='UTC'):
        self.config_filepath = configuration_file
        self.config = Configuration(configuration_file).config
        self.timezone = timezone

        self.RawData = RawData(self.config)
        self.SqlData = SqlData(self.config)
        self.TimeSeriesData = TimeSeriesData(self.config)

    ###########################################################################
    ############### RAW DATA MANAGER METHODS ##################################
    ###########################################################################
    def save_stream(self, datastream: DataStream):

        """
        Saves datastream raw data in Cassandra and metadata in MySQL.
        :param datastream:
        """
        self.RawData.save_stream(datastream)

    def get_stream(self, stream_id: uuid, day, start_time: datetime = None, end_time: datetime = None,
                   data_type=DataSet.COMPLETE) -> DataStream:

        """

        :param stream_id:
        :param day:
        :param start_time:
        :param end_time:
        :param data_type:
        :return:
        """
        self.RawData.get_stream(stream_id, day, start_time, end_time, data_type)

    def get_stream_samples(self, stream_id, day, start_time=None, end_time=None) -> List[DataPoint]:
        """
        returns list of DataPoint objects
        :param stream_id:
        :param day:
        :param start_time:
        :param end_time:
        :return:
        """
        return self.RawData.get_stream_samples(stream_id, day, start_time, end_time)


    ###########################################################################
    ############### SQL DATA MANAGER METHODS ##################################
    ###########################################################################

    ################### STREAM RELATED METHODS ################################

    def is_stream(self, stream_id: uuid)->bool:

        """

        :param stream_id:
        :return:
        """
        return self.SqlData.is_stream(stream_id)

    def get_stream_name(self, stream_id: uuid) -> str:
        """

        :param stream_id:
        :return:
        """
        return self.SqlData.get_stream_name(stream_id)

    def get_stream_id(self, stream_name: str) -> str:
        """

        :param stream_name:
        :return:
        """
        return self.SqlData.get_stream_id(stream_name)

    def get_user_id(self, user_name: str) -> str:
        """

        :param user_name:
        :return:
        """
        return self.SqlData.get_user_id(user_name)

    def get_user_name(self, user_id: uuid) -> str:
        """

        :param user_id:
        :return:
        """
        return self.SqlData.get_user_name(user_id)

    def get_user_streams_metadata(self, user_id: str) -> uuid:
        """

        :param user_id:
        :return:
        """
        return self.SqlData.get_user_streams_metadata(user_id)

    def get_user_streams(self, user_id: uuid) -> dict:

        """

        :param user_id:
        :return:
        """
        return self.SqlData.get_user_streams(user_id)

    def get_all_users(self, study_name: str) -> dict:

        """

        :param study_name:
        :return:
        """
        return self.get_all_users(study_name)

    def get_stream_duration(self, stream_id: uuid) -> dict:
        """

        :param stream_id:
        :param time_type: acceptable parameters are start_time OR end_time
        :return:
        """
        return self.SqlData.get_stream_duration(stream_id)

    def get_stream_names_ids_by_user(self, user_id: uuid, stream_name: str = None, start_time: datetime = None,
                                     end_time: datetime = None) -> List:
        """
        Returns all the stream ids and name that belongs to an owner-id
        :param user_id:
        :return:
        """
        return self.SqlData.get_stream_names_ids_by_user(user_id, stream_name, start_time, end_time)

    def get_stream_metadata(self, stream_id: uuid) -> dict:
        """

        :param stream_id:
        :return:
        """
        return self.SqlData.get_stream_metadata(stream_id)

    ################### USER RELATED METHODS ##################################

    def get_user_metadata(self, user_id, username: str = None) -> List:
        """

        :param user_id:
        :param username:
        :return:
        """
        return self.SqlData.get_user_metadata(user_id, username)

    def login_user(self, username: str, password: str) -> bool:
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

    ###########################################################################
    ############### TIME SERIES DATA MANAGER METHODS ##########################
    ###########################################################################

    ###########################################################################
    ############### IO MANAGER METHODS ########################################
    ###########################################################################

