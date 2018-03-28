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
import pytz
import pyarrow
import pickle
import gzip
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List
import traceback
from cerebralcortex.core.util.datetime_helper_methods import get_timezone
import os
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cerebralcortex.core.util.data_types import serialize_obj
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.util.data_types import convert_sample, deserialize_obj
from pytz import timezone as pytimezone
from cerebralcortex.core.util.debuging_decorators import log_execution_time

class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():


    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################
    def get_stream(self, stream_id: uuid=None, owner_id: uuid=None, day:str=None, start_time: datetime = None, end_time: datetime = None, localtime:bool=True,
                   data_type=DataSet.COMPLETE) -> DataStream:

        """

        :param stream_id:
        :param day:
        :param start_time:
        :param end_time:
        :param data_type:
        :return:
        """
        if stream_id is None or day is None or owner_id is None:
            return []
        
        # query datastream(mysql) for metadata
        datastream_metadata = self.sql_data.get_stream_metadata(stream_id)

        if data_type == DataSet.COMPLETE:
            if self.nosql_store=="hdfs":
                dps = self.read_hdfs_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
            elif self.nosql_store=="filesystem":
                dps =  self.read_filesystem_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
            else:
                dps = self.load_cassandra_data(stream_id, day, start_time, end_time)
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, dps, localtime)
        elif data_type == DataSet.ONLY_DATA:
            if self.nosql_store=="hdfs":
                return self.read_hdfs_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
            elif self.nosql_store=="filesystem":
                return self.read_filesystem_day_file(owner_id, stream_id, day, start_time, end_time, localtime)
            else:
                return self.load_cassandra_data(stream_id,day, start_time, end_time)
        elif data_type == DataSet.ONLY_METADATA:
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, None)
        else:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + "Failed to get data stream. Invalid type parameter.",
                error_type=self.logtypes.DEBUG)
            return None
        return stream

    def map_datapoint_and_metadata_to_datastream(self, stream_id: int, datastream_info: dict,
                                                 data: object, localtime:bool=True) -> DataStream:
        """
        This method will map the datapoint and metadata to datastream object
        :param stream_id:
        :param data: list
        :return: datastream object
        """

        try:
            ownerID = datastream_info[0]["owner"]
            name = datastream_info[0]["name"]
            data_descriptor = json.loads(datastream_info[0]["data_descriptor"])
            execution_context = json.loads(datastream_info[0]["execution_context"])
            annotations = json.loads(datastream_info[0]["annotations"])
            stream_type = datastream_info[0]["type"]
            start_time = datastream_info[0]["start_time"]
            end_time = datastream_info[0]["end_time"]
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

    # def read_hdfs_day_file(self, owner_id:uuid, stream_id:uuid, day:str, start_time:datetime=None, end_time:datetime=None, localtime:bool=True):
    #     # Using libhdfs,
    #     hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
    #     data = None
    #     filename = self.raw_files_dir+str(owner_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #     if not hdfs.exists(filename):
    #         print(filename, "does not exist.")
    #         return []
    #
    #     try:
    #         with hdfs.open(filename, "rb") as curfile:
    #             data = curfile.read()
    #         if data is not None and data!=b'':
    #             clean_data = self.filter_sort_datapoints(data)
    #             clean_data = self.convert_to_localtime(clean_data, localtime)
    #             if start_time is not None or end_time is not None:
    #                 clean_data = self.subset_data(clean_data, start_time, end_time)
    #             return clean_data
    #         else:
    #             return []
    #     except Exception as e:
    #         self.logging.log(error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
    #                          error_type=self.logtypes.CRITICAL)
    #         return []

    def read_hdfs_day_file(self, owner_id:uuid, stream_id:uuid, day:str, start_time:datetime=None, end_time:datetime=None, localtime:bool=True):
        # Using libhdfs,
        hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
        data = None

        if localtime:
            days = [datetime.strftime(datetime.strptime(day,  '%Y%m%d')-timedelta(hours=24),"%Y%m%d"), day, datetime.strftime(datetime.strptime(day,  '%Y%m%d')+timedelta(hours=24),"%Y%m%d")]
            day_start_time = datetime.strptime(day,  '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                filename = self.raw_files_dir+str(owner_id)+"/"+str(stream_id)+"/"+str(d)+".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                data = None
                if hdfs.exists(filename):
                    with hdfs.open(filename, "rb") as curfile:
                        data = curfile.read()
                elif hdfs.exists(gz_filename):
                    with hdfs.open(gz_filename, "rb") as curfile:
                        data = curfile.read()
                        data = gzip.decompress(data)

                if data is not None and data!=b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data,hdfs)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    day_start_time = datetime.fromtimestamp(day_start_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_end_time = datetime.fromtimestamp(day_end_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))
            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.raw_files_dir+str(owner_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None
            try:
                if hdfs.exists(filename):
                    with hdfs.open(filename, "rb") as curfile:
                        data = curfile.read()
                elif hdfs.exists(gz_filename):
                    with hdfs.open(gz_filename, "rb") as curfile:
                        data = curfile.read()
                        data = gzip.decompress(data)
                else:
                    print(filename.replace("pickle",""), "does not exist.")
                    return []
                if data is not None and data!=b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data,hdfs)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
                                 error_type=self.logtypes.CRITICAL)
                return []


    def read_filesystem_day_file(self, owner_id:uuid, stream_id:uuid, day:str, start_time:datetime=None, end_time:datetime=None, localtime:bool=True):
        data = None
        if localtime:
            days = [datetime.strftime(datetime.strptime(day,  '%Y%m%d')-timedelta(hours=24),"%Y%m%d"), day, datetime.strftime(datetime.strptime(day,  '%Y%m%d')+timedelta(hours=24),"%Y%m%d")]
            day_start_time = datetime.strptime(day,  '%Y%m%d')
            day_end_time = day_start_time + timedelta(hours=24)
            day_block = []
            for d in days:
                data = None
                filename = self.filesystem_path+str(owner_id)+"/"+str(stream_id)+"/"+str(d)+".pickle"
                gz_filename = filename.replace(".pickle", ".gz")
                if os.path.exists(filename):
                    with open(filename, "rb") as curfile:
                        data = curfile.read()
                elif os.path.exists(gz_filename):
                    with open(gz_filename, "rb") as curfile:
                        data = curfile.read()
                        data = gzip.decompress(data)
                if data is not None and data!=b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    day_start_time = datetime.fromtimestamp(day_start_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_end_time = datetime.fromtimestamp(day_end_time.timestamp(), clean_data[0].start_time.tzinfo)
                    day_block.extend(self.subset_data(clean_data, day_start_time, day_end_time))

            day_block = self.filter_sort_datapoints(day_block)
            if start_time is not None or end_time is not None:
                day_block = self.subset_data(day_block, start_time, end_time)
            return day_block
        else:
            filename = self.filesystem_path+str(owner_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
            gz_filename = filename.replace(".pickle", ".gz")
            data = None

            try:
                if os.path.exists(filename):
                    with open(filename, "rb") as curfile:
                        data = curfile.read()
                elif os.path.exists(gz_filename):
                    with open(gz_filename, "rb") as curfile:
                        data = curfile.read()
                        data = gzip.decompress(data)
                else:
                    print(filename.replace("pickle",""), "does not exist.")
                    return []
                if data is not None and data!=b'':
                    clean_data = self.filter_sort_datapoints(data)
                    self.compress_store_pickle(filename, clean_data)
                    clean_data = self.convert_to_localtime(clean_data, localtime)
                    if start_time is not None or end_time is not None:
                        clean_data = self.subset_data(clean_data, start_time, end_time)
                    return clean_data
                else:
                    return []
            except Exception as e:
                self.logging.log(error_message="Error loading from FileSystem: Cannot parse row. " + str(traceback.format_exc()),
                                 error_type=self.logtypes.CRITICAL)
                return []

    def compress_store_pickle(self, filename: str, data: pickle, hdfs: object=None):
        """

        :param filename: pickle file name
        :param data: pickled data
        :param hdfs: hdfs connection object
        """
        gz_filename = filename.replace(".pickle", ".gz")
        if len(data)>0:
            data = pickle.dumps(data)
            compressed_data = gzip.compress(data)
            if hdfs is None:
                try:
                    if not os.path.exists(gz_filename):
                        with open(gz_filename, "wb") as gzwrite:
                            gzwrite.write(compressed_data)
                    if os.path.exists(filename):
                        if os.path.getsize(gz_filename)>0:
                            os.remove(filename)
                except:
                    if os.path.exists(gz_filename):
                        if os.path.getsize(gz_filename)==0:
                            os.remove(gz_filename)
            else:
                try:
                    if not hdfs.exists(gz_filename):
                        with hdfs.open(gz_filename, "wb") as gzwrite:
                            gzwrite.write(compressed_data)
                    if hdfs.exists(filename):
                        print("exists")
                        if hdfs.info(gz_filename)["size"]>0:
                            try:
                                print("gonna delete", filename)
                                hdfs.delete(filename)
                            except:
                                print("error in deleting")
                except:
                    print("Error in generating gz file.")
                    # delete file if file was opened and no data was written to it
                    if hdfs.info(gz_filename)["size"]==0:
                        hdfs.delete(gz_filename)

    def subset_data(self, data, start_time:datetime=None, end_time:datetime=None):
        subset_data = []
        if start_time.tzinfo is None or start_time.tzinfo =="":
            start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        if end_time.tzinfo is None or end_time.tzinfo =="":
            end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)

        if start_time is not None and end_time is not None:
            for dp in data:
                if dp.start_time>=start_time and dp.start_time<=end_time:
                    subset_data.append(dp)
            return subset_data
        elif start_time is not None and end_time is None:
            for dp in data:
                if dp.start_time>=start_time:
                    subset_data.append(dp)
            return subset_data
        elif start_time is None and end_time is not None:
            for dp in data:
                if dp.start_time<=end_time:
                    subset_data.append(dp)
            return subset_data
        else:
            return data

    def filter_sort_datapoints(self, data):
        if len(data)>0:
            if not isinstance(data, list):
                data = deserialize_obj(data)
            clean_data = self.dedup(sorted(data))
            return clean_data
        else:
            return data

    def dedup(self, data):
        result = [data[0]]
        for dp in data[1:]:
            if dp.start_time == result[-1].start_time:
                continue
            result.append(dp)

        return result

    def convert_to_localtime(self, data: List[DataPoint], localtime) -> List[DataPoint]:
        """
        convert UTC time to local time
        :param data:
        :return:
        """

        if len(data)>0:
            possible_tz = pytimezone(get_timezone(data[0].offset))
            for dp in data:
                if localtime:
                    if dp.end_time is not None:
                        dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                        dp.end_time = datetime.fromtimestamp(dp.end_time.timestamp(),possible_tz)#possible_tz.localize(dp.end_time)
                    dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)
                    dp.start_time = datetime.fromtimestamp(dp.start_time.timestamp(),possible_tz)#possible_tz.localize(dp.start_time)
                else:
                    if dp.end_time is not None:
                        dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
                    dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)

        return data

    def convert_to_UTCtime(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        convert UTC time to local time
        :param data:
        :return:
        """
        local_tz_data = []
        if len(data)>0:
            possible_tz = pytimezone(get_timezone(0000))
            for dp in data:
                if dp.end_time is not None:
                    dp.end_time = datetime.fromtimestamp(dp.end_time.timestamp(),possible_tz)
                dp.start_time = datetime.fromtimestamp(dp.start_time.timestamp(),possible_tz)
                local_tz_data.append(dp)
        return local_tz_data

    # def convert_to_localtime(self, data: List[DataPoint], localtime) -> List[DataPoint]:
    #     """
    #     convert UTC time to local time
    #     :param data:
    #     :return:
    #     """
    #     #local_tz_data = []
    #     if len(data)>0:
    #         for dp in data:
    #             if localtime:
    #                 if dp.end_time is not None:
    #                     dp.end_time += timedelta(milliseconds=dp.offset)
    #                 dp.start_time += timedelta(milliseconds=dp.offset)
    #             else:
    #                 if dp.end_time is not None:
    #                     dp.end_time = dp.end_time.replace(tzinfo=pytz.utc)
    #                 dp.start_time = dp.start_time.replace(tzinfo=pytz.utc)
    #     return data

    # def convert_to_UTCtime(self, data: List[DataPoint]) -> List[DataPoint]:
    #     """
    #     convert local time to UTC time
    #     :param data:
    #     :return:
    #     """
    #     if data[0].offset==0 or data[0].offset is None or data[0].offset=="":
    #         raise ValueError("Offset cannot be None, 0, and/or empty. Please set the same time offsets you received using get_stream.")
    #
    #     if len(data)>0:
    #         for dp in data:
    #             if dp.end_time is not None:
    #                 dp.end_time -= timedelta(milliseconds=dp.offset)
    #             dp.start_time -= timedelta(milliseconds=dp.offset)
    #     return data

    def load_cassandra_data(self, stream_id, day, start_time=None, end_time=None) -> List:
        """

        :param stream_id:
        :param datapoints:
        :param batch_size:
        """
        if isinstance(stream_id, str):
            stream_id = uuid.UUID(stream_id)
        where_clause = ""
        vals = {"identifier":stream_id, "day":day}
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


            query = session.prepare("SELECT start_time,end_time, blob_obj FROM " + self.datapoint_table + " where identifier=? and day=? "+where_clause+" ALLOW FILTERING").bind(vals)

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

        :param row:
        :return:
        """
        updated_rows = []
        try:
            return deserialize_obj(row[2])
        except Exception as e:
            self.logging.log(error_message="Row: " + row + " - Cannot parse row. " + str(traceback.format_exc()),
                             error_type=self.logtypes.CRITICAL)
            return []

    def parse_row_raw_sample(self, row: str, stream_name) -> List:
        """

        :param row:
        :return:
        """
        updated_rows = []
        try:
            rows = json.loads(row[2])
            for r in rows:
                # Caasandra timezone is already in UTC. Adding timezone again would double the timezone value
                if self.time_zone != 'UTC':
                    localtz = pytimezone(self.time_zone)
                    start_time = localtz.localize(datetime.fromtimestamp(r[0]))
                else:
                    sample_timezone = timezone(timedelta(milliseconds=r[1]))
                    start_time = datetime.fromtimestamp(r[0], sample_timezone)
                sample = convert_sample(r[2], stream_name)
                updated_rows.append(DataPoint(start_time=start_time, sample=sample))
            return updated_rows
        except Exception as e:
            self.logging.log(error_message="Row: " + row + " - Cannot parse row. " + str(traceback.format_exc()),
                             error_type=self.logtypes.CRITICAL)
            return []

    def get_stream_samples(self, stream_id, day, start_time=None, end_time=None) -> List[DataPoint]:
        """
        returns list of DataPoint objects
        :param stream_id:
        :param day:
        :param start_time:
        :param end_time:
        :return:
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

            # TODO: secure it
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
        :param rows:
        :return:
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
    def save_stream(self, datastream: DataStream, localtime=True):

        """
        Saves datastream raw data in Cassandra and metadata in MySQL.
        :param datastream:
        """
        owner_id = datastream.owner
        stream_name = datastream.name
        stream_id = ""
        data_descriptor = datastream.data_descriptor
        execution_context = datastream.execution_context
        annotations = datastream.annotations
        stream_type = datastream.datastream_type
        data = datastream.data
        if len(data)>0 and (data[0].offset=="" or data[0].offset is None):
            raise ValueError("Offset cannot be None and/or empty. Please set the same time offsets you received using get_stream.")

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


                if self.nosql_store=="hdfs":
                    status = self.write_hdfs_day_file(owner_id, stream_id, data)
                elif self.nosql_store=="filesystem":
                    status = self.write_filesystem_day_file(owner_id, stream_id, data)
                else:
                    # save raw sensor data in Cassandra
                    status = self.save_raw_data(stream_id, data)

                if status:
                    # save metadata in SQL store
                    self.sql_data.save_stream_metadata(stream_id, stream_name, owner_id,
                                                       data_descriptor, execution_context,
                                                       annotations,
                                                       stream_type, new_start_time, new_end_time)
                else:
                    print("Something went wrong in saving data points. Please check error logs /var/log/cerebralcortex.log")
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - Cannot save stream. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)

    def write_hdfs_day_file(self, participant_id: uuid, stream_id: uuid, data: DataPoint):
        """

        :param participant_id:
        :param stream_id:
        :param data:
        """
        # Using libhdfs
        hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
        existing_data = None
        outputdata = {}
        success = False

        #Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)

        #Data Write loop
        for day, dps in outputdata.items():
            filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".gz"
            if len(dps)>0:
                try:
                    if hdfs.exists(filename):
                        with hdfs.open(filename, "rb") as curfile:
                            existing_data = curfile.read()
                    if existing_data is not None and existing_data!=b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        dps = existing_data
                    dps = self.filter_sort_datapoints(dps)
                    with hdfs.open(filename, "wb") as f:
                        dps = pickle.dumps(dps)
                        dps = gzip.compress(dps)
                        f.write(dps)
                    success = True
                except Exception as ex:
                    # delete file if file was opened and no data was written to it
                    if hdfs.info(filename)["size"]==0:
                        hdfs.delete(filename)
                    self.logging.log(
                        error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
        return success

    def write_filesystem_day_file(self, participant_id, stream_id, data):
        existing_data = None
        outputdata = {}
        success = False

        #Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []
            outputdata[day].append(row)

        #Data Write loop
        for day, dps in outputdata.items():
            filename = self.filesystem_path+str(participant_id)+"/"+str(stream_id)
            if not os.path.exists(filename):
                os.makedirs(filename, exist_ok=True)
            filename = filename+"/"+str(day)+".gz"
            if len(dps)>0:
                try:
                    if os.path.exists(filename):
                        with open(filename, "rb") as curfile:
                            existing_data = curfile.read()
                    if existing_data is not None and existing_data!=b'':
                        existing_data = gzip.decompress(existing_data)
                        existing_data = pickle.loads(existing_data)
                        dps.extend(existing_data)
                        dps = existing_data
                    dps = self.filter_sort_datapoints(dps)
                    with open(filename, "wb") as f:
                        dps = pickle.dumps(dps)
                        dps = gzip.compress(dps)
                        f.write(dps)
                    success = True
                except Exception as ex:
                    # delete file if file was opened and no data was written to it
                    if os.path.exists(filename):
                        if os.path.getsize(filename)==0:
                           os.remove(filename)
                    self.logging.log(
                        error_message="Error in writing data to FileSystem. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
        return success

    def save_raw_data(self, stream_id: uuid, datapoints: DataPoint):

        """

        :param stream_id:
        :param datapoints:
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
                error_message="STREAM ID: " + str(stream_id) + " - Cannot save raw data. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)
        return success

    def datapoints_to_cassandra_sql_batch(self, stream_id: uuid, datapoints: DataPoint, qry_without_endtime: str,
                                          qry_with_endtime: str):

        """

        :param stream_id:
        :param datapoints:
        :param qry_without_endtime:
        :param qry_with_endtime:
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

    def serialize_datapoints_batch(self, data):

        """
        Converts list of datapoints into batches and pickle it
        :param data:
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
