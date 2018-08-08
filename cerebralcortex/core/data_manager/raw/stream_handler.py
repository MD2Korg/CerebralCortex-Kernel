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
import sys
import pickle
import traceback
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import List
from io import BytesIO

import pyarrow
import pytz
from pytz import timezone as pytimezone

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.util.data_types import deserialize_obj
from cerebralcortex.core.util.data_types import serialize_obj
from cerebralcortex.core.util.datetime_helper_methods import get_timezone

try:
    from cassandra.cluster import Cluster
    from cassandra.query import BatchStatement, BatchType
except ImportError:
    pass


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
                if self.nosql_store == "hdfs":
                    dps = self.read_hdfs_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
                elif self.nosql_store == "filesystem":
                    dps = self.read_filesystem_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
                elif self.nosql_store == "aws_s3":
                    dps = self.read_aws_s3_file(owner_id, stream_id, day, start_time, end_time, localtime)
                else:
                    dps = self.load_cassandra_data(stream_id, day, start_time, end_time)
                stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, dps, localtime)
            elif data_type == DataSet.ONLY_DATA:
                if self.nosql_store == "hdfs":
                    return self.read_hdfs_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
                elif self.nosql_store == "filesystem":
                    return self.read_filesystem_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
                elif self.nosql_store == "aws_s3":
                    return self.read_aws_s3_file(owner_id, stream_id, day, start_time, end_time, localtime)
                else:
                    return self.load_cassandra_data(stream_id, day, start_time, end_time)
            elif data_type == DataSet.ONLY_METADATA:
                stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, None)
            else:
                self.logging.log(
                    error_message="STREAM ID: " + stream_id + "Failed to get data stream. Invalid type parameter.",
                    error_type=self.logtypes.DEBUG)
                return DataStream()
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

    def read_hdfs_day_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                           end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from HDFS
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        # Using libhdfs,
        hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)

        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:

                filename = self.raw_files_dir + str(owner_id) + "/" + str(stream_id) + "/" + str(d) + ".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                data = None
                if hdfs.exists(filename):
                    curfile = hdfs.open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif hdfs.exists(gz_filename):

                    curfile = hdfs.open(gz_filename, "rb")
                    data = curfile.read()
                    try:
                        data = gzip.decompress(data)
                    except:
                        curfile.close()
                        hdfs.delete(gz_filename)

                if data is not None and data != b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data, hdfs)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    # day_start_time = datetime.fromtimestamp(day_start_time.timestamp(), clean_data[0].start_time.tzinfo)
                    # day_end_time = datetime.fromtimestamp(day_end_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))
            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.raw_files_dir + str(owner_id) + "/" + str(stream_id) + "/" + str(day) + ".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None
            try:
                if hdfs.exists(filename):
                    curfile = hdfs.open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif hdfs.exists(gz_filename):
                    curfile = hdfs.open(gz_filename, "rb")
                    data = curfile.read()
                    try:
                        data = gzip.decompress(data)
                    except:
                        curfile.close()
                        hdfs.delete(gz_filename)
                else:
                    return []
                if data is not None and data != b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data, hdfs)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(
                    error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
                return []

    def read_aws_s3_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                           end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from AWS-S3
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        # TODO: this method only works for .gz files. Update it to handle .pickle uncompressed if required

        bucket_name = self.minio_input_bucket

        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                try:
                    object_name = self.minio_dir_prefix+str(owner_id) + "/" + str(stream_id)+"/"+str(d)+".gz"

                    if self.ObjectData.is_object(bucket_name, object_name):
                        data = self.ObjectData.get_object(bucket_name, object_name)
                        if data.status==200:
                            data = gzip.decompress(data.data)
                            if data is not None and data != b'':
                                clean_data = self.filter_sort_datapoints(data)
                                clean_data = self.convert_to_localtime(clean_data, localtime)
                                day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))
                        else:
                            self.logging.log(
                                error_message="HTTP-STATUS: "+data.status+" - Cannot get "+str(object_name)+" from AWS-S3. " + str(traceback.format_exc()),
                                error_type=self.logtypes.CRITICAL)
                except:
                    self.logging.log(
                        error_message="Error loading from AWS-S3: Cannot parse row. " + str(traceback.format_exc()),
                        error_type=self.logtypes.CRITICAL)

            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            object_name = self.minio_dir_prefix+str(owner_id) + "/" + str(stream_id)+"/"+str(day)+".gz"
            try:
                if self.ObjectData.is_object(bucket_name, object_name):
                    http_resp = self.ObjectData.get_object(bucket_name, object_name)
                    if http_resp.status==200:
                        data = gzip.decompress(http_resp.data)
                    else:
                        self.logging.log(
                            error_message=http_resp.status+" HTTP-STATUS, Cannot get data from AWS-S3. " + str(traceback.format_exc()),
                            error_type=self.logtypes.CRITICAL)
                        return []
                else:
                    return []
                if data is not None and data != b'':
                    clean_data = self.filter_sort_datapoints(data)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(
                    error_message="Error loading from AWS-S3: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
                return []

    def read_filesystem_day_file(self, owner_id: uuid, stream_id: uuid, day: str, start_time: datetime = None,
                                 end_time: datetime = None, localtime: bool = True) -> List[DataPoint]:
        """
        Read and Process (read, unzip, unpickle, remove duplicates) data from a file system
        :param owner_id:
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :param localtime:
        :return: returns unique (based on start time) list of DataPoints
        :rtype: DataPoint
        """
        if localtime:
            days = [datetime.strftime(datetime.strptime(day, '%Y%m%d') - timedelta(hours=24), "%Y%m%d"), day,
                    datetime.strftime(datetime.strptime(day, '%Y%m%d') + timedelta(hours=24), "%Y%m%d")]
            day_start_time = datetime.strptime(day, '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                data = None
                filename = self.filesystem_path + str(owner_id) + "/" + str(stream_id) + "/" + str(d) + ".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                if os.path.exists(filename):
                    curfile = open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif os.path.exists(gz_filename):
                    curfile = open(gz_filename, "rb")
                    data = curfile.read()
                    if data is not None and data != b'':
                        try:
                            data = gzip.decompress(data)
                        except:
                            os.remove(gz_filename)
                    curfile.close()
                if data is not None and data != b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    # day_start_time = datetime.fromtimestamp(day_start_time.timestamp(), clean_data[0].start_time.tzinfo)
                    # day_end_time = datetime.fromtimestamp(day_end_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))

            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.filesystem_path + str(owner_id) + "/" + str(stream_id) + "/" + str(day) + ".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None

            try:
                if os.path.exists(filename):
                    curfile = open(filename, "rb")
                    data = curfile.read()
                    curfile.close()
                elif os.path.exists(gz_filename):
                    curfile = open(gz_filename, "rb")
                    data = curfile.read()
                    try:
                        data = gzip.decompress(data)
                    except:
                        os.remove(gz_filename)
                    curfile.close()
                else:
                    return []
                if data is not None and data != b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(
                    error_message="Error loading from FileSystem: Cannot parse row. " + str(traceback.format_exc()),
                    error_type=self.logtypes.CRITICAL)
                return []

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
                possible_tz = pytimezone(get_timezone(data[0].offset))
            for dp in data:
                if localtime:
                    if dp.end_time is not None:
                        dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                        dp.end_time = datetime.fromtimestamp(dp.end_time.timestamp(),
                                                             possible_tz)  # possible_tz.localize(dp.end_time)
                    dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)
                    dp.start_time = datetime.fromtimestamp(dp.start_time.timestamp(),
                                                           possible_tz)  # possible_tz.localize(dp.start_time)
                else:
                    if dp.end_time is not None:
                        dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                    dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)

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

    def load_cassandra_data(self, stream_id: uuid, day: str, start_time: datetime = None, end_time: datetime = None) -> \
    List[DataPoint]:
        """
        Get data from Cassandra/SyllaDB and convert it into list of DataPoint
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :return: list of datapoints
        :rtype: List[DataPoint]
        """
        if isinstance(stream_id, str):
            stream_id = uuid.UUID(stream_id)
        where_clause = ""
        vals = {"identifier": stream_id, "day": day}
        try:
            cluster = Cluster([self.host_ip], port=self.host_port, protocol_version=4)

            session = cluster.connect(self.keyspace_name)

            if start_time is not None and end_time is not None:
                where_clause = " and start_time>=? and end_time<=?"
                vals["start_time"] = start_time
                vals["end_time"] = end_time

            elif start_time is not None and end_time is None:
                where_clause = " and start_time>=? "
                vals["start_time"] = start_time
            elif start_time is None and end_time is not None:
                where_clause = " and end_time<=?"
                vals["end_time"] = end_time

            query = session.prepare(
                "SELECT start_time,end_time, blob_obj FROM " + self.datapoint_table + " where identifier=? and day=? " + where_clause + " ALLOW FILTERING").bind(
                vals)

            data = []
            rows = session.execute(query, timeout=None)
            for row in rows:
                data += self.parse_row(row)

            session.shutdown()
            cluster.shutdown()
            if not data:
                return []
            return data
        except Exception as e:
            self.logging.log(
                error_message="WHERE CLAUSE:" + where_clause + " - Error in loading cassandra data. " + str(
                    traceback.format_exc()), error_type=self.logtypes.CRITICAL)
            return []

    def parse_row(self, row: str) -> List:
        """
        Unpickle an object of List[DataPoint]
        :param row:
        :return: list of DataPoint
        :rtype: List[DataPoint]
        """
        try:
            return deserialize_obj(row[2])
        except Exception as e:
            self.logging.log(error_message="Row: " + row + " - Cannot parse row. " + str(traceback.format_exc()),
                             error_type=self.logtypes.CRITICAL)
            return []

    # def parse_row_raw_sample(self, row: str, stream_name:str) -> List[DataPoint]:
    #     """
    #
    #     :param row:
    #     :param stream_name:
    #     :return:
    #     """
    #     updated_rows = []
    #     try:
    #         rows = json.loads(row[2])
    #         for r in rows:
    #             # Caasandra timezone is already in UTC. Adding timezone again would double the timezone value
    #             if self.time_zone != 'UTC':
    #                 localtz = pytimezone(self.time_zone)
    #                 start_time = localtz.localize(datetime.fromtimestamp(r[0]))
    #             else:
    #                 sample_timezone = timezone(timedelta(milliseconds=r[1]))
    #                 start_time = datetime.fromtimestamp(r[0], sample_timezone)
    #             sample = convert_sample(r[2], stream_name)
    #             updated_rows.append(DataPoint(start_time=start_time, sample=sample))
    #         return updated_rows
    #     except Exception as e:
    #         self.logging.log(error_message="Row: " + row + " - Cannot parse row. " + str(traceback.format_exc()),
    #                          error_type=self.logtypes.CRITICAL)
    #         return []

    def get_stream_samples(self, stream_id: uuid, day: str, start_time: datetime = None, end_time: datetime = None) -> \
    List[DataPoint]:
        """
        Get data from Cassandra/ScyllaDB and convert it into a list of DataPoint format
        :param stream_id:
        :param day: format (YYYYMMDD)
        :param start_time:
        :param end_time:
        :return: list of DataPoint objects
        :rtype: List[DataPoint]
        """
        try:
            cluster = Cluster([self.hostIP], port=self.hostPort)
            session = cluster.connect(self.keyspaceName)

            if start_time and end_time:
                qry = "SELECT sample from " + self.datapointTable + " where identifier=" + stream_id + " and day='" + day + "' and start_time>='" + str(
                    start_time) + "' and start_time<='" + str(end_time) + "' ALLOW FILTERING"
            elif start_time and not end_time:
                qry = "SELECT sample from " + self.datapointTable + " where identifier=" + stream_id + " and day='" + day + "' and start_time>='" + str(
                    start_time) + "' ALLOW FILTERING"
            elif not start_time and end_time:
                qry = "SELECT sample from " + self.datapointTable + " where identifier=" + stream_id + " and day='" + day + "' and start_time<='" + str(
                    end_time) + "' ALLOW FILTERING"
            else:
                qry = "SELECT sample from " + self.datapointTable + " where identifier=" + stream_id + " and day='" + day + "'"

            rows = session.execute(qry)
            dps = self.row_to_datapoints(rows)

            session.shutdown()
            cluster.shutdown()
            return dps
        except Exception as e:
            self.logging.log(
                error_message="ID: " + stream_id + " - Cannot get stream samples. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)
            return []

    def row_to_datapoints(self, rows: object) -> List[DataPoint]:
        """
        Convert Cassandra rows into DataPoint list
        :param rows: Cassandra row object
        :return: list of DataPoint objects
        :rtype: List[DataPoint]
        """
        dps = []
        try:
            if rows:
                for row in rows:
                    sample = row[2]
                    # Caasandra timezone is already in UTC. Adding timezone again would double the timezone value
                    if self.time_zone != 'UTC':
                        localtz = pytimezone(self.time_zone)
                        if row[0]:
                            start_time = localtz.localize(row[0])
                        if row[1]:
                            end_time = localtz.localize(row[1])
                    else:
                        if row[0]:
                            start_time = row[0]
                        if row[1]:
                            end_time = row[1]

                    dps.append(DataPoint(start_time, end_time, sample))
            return dps
        except Exception as e:
            self.logging.log(
                error_message="ROW: " + row + " - Cannot convert row to datapoints. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)
            return []

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

                if self.nosql_store == "hdfs":
                    status = self.write_hdfs_day_file(owner_id, stream_id, data)
                elif self.nosql_store == "filesystem":
                    status = self.write_filesystem_day_file(owner_id, stream_id, data)
                elif self.nosql_store == "aws_s3":
                    status = self.write_aws_s3_file(owner_id, stream_id, data)
                elif self.nosql_store=="cassandra" or self.nosql_store=="scylladb":
                    # save raw sensor data in Cassandra
                    status = self.save_raw_data(stream_id, data)
                else:
                    raise Exception("Acceptable parameters are: cassandra, scylladb, hdfs, filesystem. Please check CC config (data_ingestion->nosql_store)")

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

    def write_hdfs_day_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in HDFS. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """

        # Using libhdfs
        hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
        outputdata = {}
        success = False

        # Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)

        # Data Write loop
        for day, dps in outputdata.items():
            existing_data = None
            filename = self.raw_files_dir + str(participant_id) + "/" + str(stream_id) + "/" + str(day) + ".gz"
            if len(dps) > 0:
                try:
                    if hdfs.exists(filename):
                        curfile = hdfs.open(filename, "rb")
                        existing_data = curfile.read()
                        curfile.close()
                    if existing_data is not None and existing_data != b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        # dps = existing_data


                    dps = self.filter_sort_datapoints(dps)
                    f = hdfs.open(filename, "wb")
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    f.write(dps)
                    f.close()

                    if hdfs.exists(filename.replace(".gz", ".pickle")):
                        hdfs.delete(filename.replace(".gz", ".pickle"))
                    success = True
                except Exception as ex:
                    success = False
                    # delete file if file was opened and no data was written to it
                    if hdfs.exists(filename):
                        if hdfs.info(filename)["size"] == 0:
                            hdfs.delete(filename)
                    self.logging.log(
                        error_message="Error in writing data to HDFS. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            filename) + " - Exception: " + str(ex), error_type=self.logtypes.DEBUG)
        return success

    def write_aws_s3_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in AWS-S3. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """

        bucket_name = self.minio_output_bucket
        outputdata = {}
        success = False

        # Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)

        # Data Write loop
        for day, dps in outputdata.items():
            existing_data = None
            object_name = self.minio_dir_prefix + str(participant_id) + "/" + str(stream_id) + "/" + str(day) + ".gz"

            if len(dps) > 0:
                try:
                    try:
                        self.ObjectData.is_object(bucket_name, object_name)
                        existing_data = self.ObjectData.get_object(bucket_name, object_name)
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                    except:
                        pass
                    dps = self.filter_sort_datapoints(dps)
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    dps = BytesIO(dps)
                    obj_size = dps.seek(0, os.SEEK_END)
                    dps.seek(0) # set file pointer back to start, otherwise minio would complain as size 0
                    success = self.ObjectData.upload_object_to_s3(bucket_name, object_name, dps, obj_size)
                except Exception as ex:
                    success = False
                    self.logging.log(
                        error_message="Error in writing data to AWS-S3. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            object_name) + " - Exception: " + str(ex), error_type=self.logtypes.DEBUG)
        return success

    def write_filesystem_day_file(self, participant_id: uuid, stream_id: uuid, data: List[DataPoint]) -> bool:
        """
        Stores data in file system. If data contains multiple days then one file will be created for each day
        :param participant_id:
        :param stream_id:
        :param data:
        :return True if data is successfully stored
        :rtype bool
        """
        existing_data = None
        outputdata = {}
        success = False

        # Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []
            outputdata[day].append(row)

        # Data Write loop
        for day, dps in outputdata.items():
            existing_data = None
            filename = self.filesystem_path + str(participant_id) + "/" + str(stream_id)
            if not os.path.exists(filename):
                os.makedirs(filename, exist_ok=True)
            filename = filename + "/" + str(day) + ".gz"
            if len(dps) > 0:
                try:
                    if os.path.exists(filename):
                        curfile = open(filename, "rb")
                        existing_data = curfile.read()
                        curfile.close()
                    if existing_data is not None and existing_data != b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        # dps = existing_data
                    dps = self.filter_sort_datapoints(dps)
                    f = open(filename, "wb")
                    dps = pickle.dumps(dps)
                    dps = gzip.compress(dps)
                    f.write(dps)
                    f.close()
                    if os.path.exists(filename.replace(".gz", ".pickle")):
                        os.remove(filename.replace(".gz", ".pickle"))
                    success = True
                except Exception as ex:
                    # delete file if file was opened and no data was written to it
                    if os.path.exists(filename):
                        if os.path.getsize(filename) == 0:
                            os.remove(filename)
                    self.logging.log(
                        error_message="Error in writing data to FileSystem. STREAM ID: " + str(
                            stream_id) + "Owner ID: " + str(participant_id) + "Files: " + str(
                            filename) + " - Exception: " + str(ex), error_type=self.logtypes.DEBUG)
        return success

    def save_raw_data(self, stream_id: uuid, datapoints: List[DataPoint]) -> bool:

        """
        Pickle list of DataPoint objects and store it in Cassandra/ScyllaDB
        :param stream_id:
        :param datapoints:
        :return True if data is successfully ingested
        :rtype bool
        """
        datapoints = self.serialize_datapoints_batch(datapoints)
        success = False
        try:
            cluster = Cluster([self.host_ip], port=self.host_port, protocol_version=4)

            session = cluster.connect(self.keyspace_name)

            qry_without_endtime = session.prepare(
                "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, blob_obj) VALUES (?, ?, ?, ?)")
            qry_with_endtime = session.prepare(
                "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, end_time, blob_obj) VALUES (?, ?, ?, ?, ?)")

            if isinstance(stream_id, str):
                stream_id = uuid.UUID(stream_id)

            for data_block in self.datapoints_to_cassandra_sql_batch(stream_id, datapoints, qry_without_endtime,
                                                                     qry_with_endtime):
                session.execute(data_block)
                data_block.clear()

            session.shutdown()
            cluster.shutdown()
            success = True
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + str(stream_id) + " - Cannot save raw data. " + str(
                    traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)
        return success

    def datapoints_to_cassandra_sql_batch(self, stream_id: uuid, datapoints: DataPoint, qry_without_endtime: str,
                                          qry_with_endtime: str) -> object: #don't put return type as BatchStatement because it can cause errors when Cassandra is selected as NoSQL store
        """
        Convert List of DataPoint objects to Cassandra/ScyllaDB batch
        :param stream_id:
        :param datapoints:
        :param qry_without_endtime:
        :param qry_with_endtime:
        :return Cassandra/ScyllaDB batch
        :rtype BatchStatement(batch_type=BatchType.UNLOGGED)
        """
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.clear()
        dp_number = 1
        for dp in datapoints:
            day = dp[0].strftime("%Y%m%d")
            sample = dp[3]
            if dp[1]:
                insert_qry = qry_with_endtime
            else:
                insert_qry = qry_without_endtime

            if dp_number > self.batch_size:
                yield batch
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                # just to make sure batch does not have any existing entries.
                batch.clear()
                dp_number = 1
            else:
                if dp[1]:
                    batch.add(insert_qry, (stream_id, day, dp[0], dp[1], sample))
                else:
                    batch.add(insert_qry, (stream_id, day, dp[0], sample))
                dp_number += 1
        yield batch

    def serialize_datapoints_batch(self, data: List[DataPoint]):

        """
        Converts list of datapoints into batches and pickle it
        :param data:
        :return picked format of List of DataPoint objects
        :rtype pickle
        """

        grouped_samples = []
        line_number = 1
        current_day = None  # used to check boundry condition. For example, if half of the sample belong to next day
        last_start_time = None
        datapoints = []

        for dp in data:

            start_time = dp.start_time
            end_time = dp.end_time

            if line_number == 1:
                datapoints = []
                first_start_time = start_time
                # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
                start_day = first_start_time.strftime("%Y%m%d")

                current_day = int(start_time.timestamp() / 86400)
            if line_number > self.sample_group_size:
                last_start_time = start_time
                datapoints.append(DataPoint(start_time, end_time, dp.offset, dp.sample))
                grouped_samples.append([first_start_time, last_start_time, start_day, serialize_obj(datapoints)])
                line_number = 1
            else:
                if (int(start_time.timestamp() / 86400)) > current_day:
                    start_day = start_time.strftime("%Y%m%d")
                datapoints.append(DataPoint(start_time, end_time, dp.offset, dp.sample))
                line_number += 1

        if len(datapoints) > 0:
            if not last_start_time:
                last_start_time = start_time
            grouped_samples.append([first_start_time, last_start_time, start_day, serialize_obj(datapoints)])

        return grouped_samples
