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


import gzip
import json
import os
import pickle
import traceback
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import List

import pytz
from pytz import timezone as pytimezone

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.util.data_types import deserialize_obj
from cerebralcortex.core.util.datetime_helper_methods import get_timezone


class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():
    
    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################
    def get_stream(self, stream_id: uuid = None, owner_id: uuid = None, day: str = None, start_time: datetime = None,
                   end_time: datetime = None, localtime: bool = False,
                   data_type=DataSet.COMPLETE) -> DataStream:
        """
        Returns a DataStream object with metadata of a stream and data for a given day.
        :param stream_id: UUID of a stream
        :param owner_id: UUID of a stream
        :param day: day format (YYYYMMDD)
        :param start_time: start time of @day
        :param end_time: end time of @day
        :param localtime: get data in participant's local time
        :param data_type: get metadata, data both or just metadata or data
        :return: DataStream object
        :rtype: DataStream
        """
        if stream_id is None or day is None:
            return DataStream()

        # query datastream(mysql) for metadata
        datastream_metadata = self.sql_data.get_stream_metadata(stream_id)

        if len(datastream_metadata) > 0:
            owner_id = datastream_metadata[0]["owner"]
            if data_type == DataSet.COMPLETE:
                dps = self.nosql.read_file(owner_id, stream_id, day, start_time, end_time, localtime)
                stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, dps, localtime)
            elif data_type == DataSet.ONLY_DATA:
                stream = self.nosql.read_file(owner_id, stream_id, day, start_time, end_time, localtime)                
            elif data_type == DataSet.ONLY_METADATA:
                stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, None)
            else:
                raise ValueError("STREAM ID: " + str(stream_id) + "Failed to get data stream. Invalid type parameter.")
            return stream
        else:
            return DataStream()

    def get_stream_by_name(self, stream_name: uuid, user_id: uuid = None, start_time: datetime = None,
                           end_time: datetime = None, localtime: bool = False,
                           data_type=DataSet.COMPLETE) -> DataStream:
        """
        TODO: Return stream data for all stream-ids related to a stream-name
        :param stream_name:
        :param user_id:
        :param start_time:
        :param end_time:
        :param localtime:
        :param data_type:
        :return:

        """
        if stream_name is None or user_id is None:
            return []
        days = []
        all_stream_days = {}
        # get all stream ids for a stream name
        stream_ids = self.sql_data.get_stream_id(user_id, stream_name)
        for sid in stream_ids:
            all_stream_days[sid] = self.get_stream_days(sid)

        #     days.extend(self.get_stream_days(sid))
        # all_stream_days = list(set(days))

        if start_time is not None and end_time is not None:
            time_diff = end_time - start_time
            for day in range(time_diff.days + 1):
                days.append((start_time + timedelta(days=day)).strftime('%Y%m%d'))
        elif start_time is not None and end_time is None:
            # make day out of start-time, just one day data
            days = [start_time.strftime("%Y%m%d")]
        elif start_time is None and end_time is not None:
            raise ValueError("Start time cannot be None if end time is provided.")
            return []
        else:
            # get all days data
            for sid in stream_ids:
                days.extend(self.get_stream_days(sid))
            days = list(set(days))

        # query datastream(mysql) for metadata
        # datastream_metadata = self.sql_data.get_stream_metadata(stream_name)
        # if len(datastream_metadata)>0:
        #     owner_id = datastream_metadata[0]["owner"]
        #     if data_type == DataSet.COMPLETE:
        #         if self.nosql_store=="hdfs":
        #             dps = self.read_hdfs_day_file(owner_id, stream_name, day, start_time, end_time, localtime)
        #         elif self.nosql_store=="filesystem":
        #             dps =  self.read_filesystem_day_file(owner_id, stream_name, day, start_time, end_time, localtime)
        #         else:
        #             dps = self.load_cassandra_data(stream_name, day, start_time, end_time)
        #         stream = self.map_datapoint_and_metadata_to_datastream(stream_name, datastream_metadata, dps, localtime)
        #     elif data_type == DataSet.ONLY_DATA:
        #         if self.nosql_store=="hdfs":
        #             return self.read_hdfs_day_file(owner_id, stream_name, day, start_time, end_time, localtime)
        #         elif self.nosql_store=="filesystem":
        #             return self.read_filesystem_day_file(owner_id, stream_name, day, start_time, end_time, localtime)
        #         else:
        #             return self.load_cassandra_data(stream_name, day, start_time, end_time)
        #     elif data_type == DataSet.ONLY_METADATA:
        #         stream = self.map_datapoint_and_metadata_to_datastream(stream_name, datastream_metadata, None)
        #     else:
        #         self.logging.log(
        #             error_message="STREAM ID: " + stream_name + "Failed to get data stream. Invalid type parameter.",
        #             error_type=self.logtypes.DEBUG)
        #         return None
        #     return stream
        # else:
        #     return DataStream()

    def map_datapoint_and_metadata_to_datastream(self, stream_id: uuid, metadata: dict,
                                                 data: List[DataPoint], localtime: bool = True) -> DataStream:
        """
        This method will map the datapoint and metadata to a DataStream object
        :param stream_id:
        :param metadata:
        :param data:
        :param localtime:
        :return: DataStream Object containing metadata and data
        :rtype: DataStream

        """
        try:
            ownerID = metadata[0]["owner"]
            name = metadata[0]["name"]
            data_descriptor = json.loads(metadata[0]["data_descriptor"])
            execution_context = json.loads(metadata[0]["execution_context"])
            annotations = json.loads(metadata[0]["annotations"])
            stream_type = metadata[0]["type"]
            start_time = metadata[0]["start_time"]
            end_time = metadata[0]["end_time"]
            if localtime:
                stream_timezone = "local"
            else:
                stream_timezone = "utc"
            return DataStream(stream_id, ownerID, name, data_descriptor, execution_context, annotations,
                              stream_type, start_time, end_time, data, stream_timezone)
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - Error in mapping datapoints and metadata to datastream. " + str(
                    traceback.format_exc()), error_type=self.logtypes.CRITICAL)

    def compress_store_pickle(self, filename: str, data: pickle, hdfs: object = None):
        """
        Compress (using gzip compression) pickled binary object and store it to HDFS or File System
        :param filename: pickle file name
        :param data: pickled data (binary object)
        :param hdfs: hdfs connection object, store in a file system if object is None
        """
        gz_filename = filename.replace(".pickle", ".gz")
        if len(data) > 0:

            if hdfs is None:
                try:
                    if not os.path.exists(gz_filename):
                        # moved inside if condition so if exist do not even pickle/compress
                        data = pickle.dumps(data)
                        compressed_data = gzip.compress(data)
                        gzwrite = open(gz_filename, "wb")
                        gzwrite.write(compressed_data)
                        gzwrite.close()
                        if os.path.exists(filename):
                            if os.path.getsize(gz_filename) > 0:
                                os.remove(filename)
                except:
                    if os.path.exists(gz_filename):
                        if os.path.getsize(gz_filename) == 0:
                            os.remove(gz_filename)
            else:
                try:
                    if not hdfs.exists(gz_filename):
                        # moved inside if condition so if exist do not even pickle/compress
                        data = pickle.dumps(data)
                        compressed_data = gzip.compress(data)
                        gzwrite = hdfs.open(gz_filename, "wb")
                        gzwrite.write(compressed_data)
                        gzwrite.close()
                        if hdfs.exists(filename):
                            if hdfs.info(gz_filename)["size"] > 0:
                                hdfs.delete(filename)
                except:
                    print("Error in generating gz file.")
                    # delete file if file was opened and no data was written to it
                    if hdfs.exists(gz_filename):
                        if hdfs.info(gz_filename)["size"] == 0:
                            hdfs.delete(gz_filename)

    def subset_data(self, data: List[DataPoint], start_time: datetime = None, end_time: datetime = None) -> List[
        DataPoint]:
        """
        It accepts list of DataPoints and subset it based on start/end time
        :param data: List of DataPoint
        :param start_time:
        :param end_time:
        :return: data
        :rtype: List[DataPoint]
        """
        if len(data)>0:
            subset_data = []
            if start_time.tzinfo is None or start_time.tzinfo == "":
                start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
            if end_time.tzinfo is None or end_time.tzinfo == "":
                end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)

            if start_time is not None and end_time is not None:
                for dp in data:
                    if dp.start_time >= start_time and dp.start_time <= end_time:
                        subset_data.append(dp)
                return subset_data
            elif start_time is not None and end_time is None:
                for dp in data:
                    if dp.start_time >= start_time:
                        subset_data.append(dp)
                return subset_data
            elif start_time is None and end_time is not None:
                for dp in data:
                    if dp.start_time <= end_time:
                        subset_data.append(dp)
                return subset_data
            else:
                return data
        else:
            return data

    def filter_sort_datapoints(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Remove duplicate datapoints and sort them based on the start time
        :param data:
        :return: contains unique and sorted list of Datapoints
        :rtype: List[DataPoint]
        """
        if len(data) > 0:
            if not isinstance(data, list):
                data = deserialize_obj(data)
            clean_data = self.dedup(sorted(data))
            return clean_data
        else:
            return data

    def dedup(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Remove duplicate datapoints based on the start time
        :param data:
        :return: contains list of unique Datapoints
        :rtype: List[DataPoint]
        """
        result = [data[0]]
        for dp in data[1:]:
            if dp.start_time == result[-1].start_time:
                continue
            result.append(dp)

        return result

    def convert_to_localtime(self, data: List[DataPoint], localtime) -> List[DataPoint]:
        """
        Adds timezone to time. If locatime is false then it adds UTC timezone to start/end time
        :param data: List[DataPoint]
        :return: List of DataPoints with timezone embedded to start/end time
        :rtype: List[DataPoint]
        """

        if len(data) > 0:
            if localtime:
                if localtime:
                    possible_tz = pytimezone(get_timezone(data[0].offset))
                    for dp in data:

                        if dp.end_time is not None:
                            dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                            dp.end_time = datetime.fromtimestamp(dp.end_time.timestamp(),
                                                                 possible_tz)
                        dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)
                        dp.start_time = datetime.fromtimestamp(dp.start_time.timestamp(),
                                                               possible_tz)
                # TODO: for now, disabled it. Storing stream method already store all the data in UTC.
                # else:
                #     if dp.end_time is not None:
                #         dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                #     dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)

        return data

    def convert_to_UTCtime(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        convert start/end time of a DataPoint to UTC time
        :param data: List[DataPoint]
        :return: List of DataPoints with timezone embedded to start/end time
        :rtype: List[DataPoint]
        """
        local_tz_data = []
        if len(data) > 0:
            possible_tz = pytimezone(get_timezone(0000))
            for dp in data:
                if dp.end_time is not None:
                    dp.end_time = datetime.fromtimestamp(dp.end_time.timestamp(), possible_tz)
                dp.start_time = datetime.fromtimestamp(dp.start_time.timestamp(), possible_tz)
                local_tz_data.append(dp)
        return local_tz_data

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################
    def save_stream(self, datastream: DataStream, localtime=False, ingestInfluxDB=False):

        """
        Stores metadata in MySQL and raw data in HDFS, file system, Cassandra, OR ScyllaDB (nosql data store could be changed in CC yaml file)
        :param datastream:
        :param localtime: if localtime is True then all DataPoints in a DataStream would be converted to UTC from localtime
        """
        owner_id = datastream.owner
        stream_name = datastream.name
        stream_id = ""
        data_descriptor = datastream.data_descriptor
        execution_context = datastream.execution_context
        annotations = datastream.annotations
        stream_type = datastream.datastream_type
        data = datastream.data
        if isinstance(data, list) and len(data) > 0 and (data[0].offset == "" or data[0].offset is None):
            raise ValueError(
                "Offset cannot be None and/or empty. Please set the same time offsets you received using get_stream.")

        data = self.filter_sort_datapoints(data)
        if localtime:
            data = self.convert_to_UTCtime(data)
        try:

            # get start and end time of a stream
            if data:
                if isinstance(data, list):
                    total_dp = len(data) - 1
                    if not datastream.start_time:
                        new_start_time = data[0].start_time
                    else:
                        new_start_time = datastream.start_time
                    if not datastream.end_time:
                        new_end_time = data[total_dp].start_time
                    else:
                        new_end_time = datastream.end_time
                else:
                    if not datastream.start_time:
                        new_start_time = data.start_time
                    else:
                        new_start_time = datastream.start_time
                    if not datastream.end_time:
                        new_end_time = data.start_time
                    else:
                        new_end_time = datastream.end_time

                stream_id = datastream.identifier
                
                status = self.nosql.write_file(owner_id, stream_id, data)

                if status:
                    # save metadata in SQL store
                    self.sql_data.save_stream_metadata(stream_id, stream_name, owner_id,
                                                       data_descriptor, execution_context,
                                                       annotations,
                                                       stream_type, new_start_time, new_end_time)
                    try:
                        if ingestInfluxDB:
                            self.timeSeriesData.store_data_to_influxdb(datastream=datastream)
                    except:
                        raise Exception
                else:
                    print(
                        "Something went wrong in saving data points. Please check error logs /var/log/cerebralcortex.log")
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - Cannot save stream. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)
