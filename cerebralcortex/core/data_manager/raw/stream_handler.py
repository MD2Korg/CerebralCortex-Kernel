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
import pyarrow
import pickle
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List
import traceback
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cerebralcortex.core.util.data_types import serialize_obj
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.util.data_types import convert_sample, deserialize_obj
from pytz import timezone as pytimezone



class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():


    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_stream(self, stream_id: uuid=None, owner_id: uuid=None, day:str=None, start_time: datetime = None, end_time: datetime = None,
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
            return None
        
        where_clause = "where identifier=" + str(stream_id) + " and day='" + str(day) + "'"

        if start_time:
            where_clause += " and start_time>=cast('" + str(start_time) + "' as timestamp)"

        if end_time:
            where_clause += " and start_time<=cast('" + str(end_time) + "' as timestamp)"

        # query datastream(mysql) for metadata
        datastream_metadata = self.sql_data.get_stream_metadata(stream_id)

        if data_type == DataSet.COMPLETE:
            if self.nosql_store=="hdfs":
                dps = self.read_hdfs_day_file(owner_id, stream_id, day, start_time, end_time)
            else:
                dps = self.load_cassandra_data(where_clause)
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, dps)
        elif data_type == DataSet.ONLY_DATA:
            if self.nosql_store=="hdfs":
                return self.read_hdfs_day_file(owner_id, stream_id, day)
            else:
                return self.load_cassandra_data(where_clause)                
        elif data_type == DataSet.ONLY_METADATA:
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, None)
        else:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + "Failed to get data stream. Invalid type parameter.",
                error_type=self.logtypes.DEBUG)
            return None
        return stream

    def map_datapoint_and_metadata_to_datastream(self, stream_id: int, datastream_info: dict,
                                                 data: object) -> DataStream:
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
            return DataStream(stream_id, ownerID, name, data_descriptor, execution_context, annotations,
                              stream_type, start_time, end_time, data)
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + stream_id + " - Error in mapping datapoints and metadata to datastream. " + str(
                    traceback.format_exc()), error_type=self.logtypes.CRITICAL)
    
    def read_hdfs_day_file(self, owner_id:uuid, stream_id:uuid, day:str, start_time:datetime=None, end_time:datetime=None):
        # Using libhdfs
        hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
        subset_data = []
        filename = self.raw_files_dir+str(owner_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
        if not hdfs.exists(filename):
            print("File does not exist.")
            return []

        try:
            with hdfs.open(filename, "rb") as curfile:
                data = curfile.read()
            if data is not None:
                clean_data = self.filter_sort_datapoints(data)
                if start_time is not None or end_time is not None:
                    clean_data = self.subset_data(clean_data, start_time, end_time)
                return clean_data
        except Exception as e:
            self.logging.log(error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
                             error_type=self.logtypes.CRITICAL)
            return []

    def subset_data(self, data, start_time:datetime=None, end_time:datetime=None):
        subset_data = []
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
        if not isinstance(data, list):
            data = deserialize_obj(data)
        clean_data = set(data)
        clean_data = sorted(clean_data)
        return clean_data


    # def read_hdfs_day_file(self, owner_id:uuid, stream_id:uuid, day:str):
    #     # Using libhdfs
    #     hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
    #
    #     datapoints = []
    #     filename = self.raw_files_dir+str(owner_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #     if not hdfs.exists(filename):
    #         return []
    #     with hdfs.open(filename, "rb") as curfile:
    #         data = curfile.read()
    #     if data is not None:
    #         data = deserialize_obj(data)
    #     try:
    #         for dp in data:
    #             datapoints.extend(deserialize_obj(dp[3]))
    #         return datapoints
    #     except Exception as e:
    #         self.logging.log(error_message="Error loading from HDFS: Cannot parse row. " + str(traceback.format_exc()),
    #                          error_type=self.logtypes.CRITICAL)
    #         return []
        
    
    def load_cassandra_data(self, where_clause=None) -> List:
        """

        :param stream_id:
        :param datapoints:
        :param batch_size:
        """
        try:
            cluster = Cluster([self.host_ip], port=self.host_port, protocol_version=4)

            session = cluster.connect(self.keyspace_name)

            query = "SELECT start_time,end_time, blob_obj FROM " + self.datapoint_table + " " + where_clause
            statement = SimpleStatement(query, fetch_size=None)
            data = []
            rows = session.execute(statement, timeout=None)
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

    def parse_row_raw_sample(self, row: str) -> List:
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
                sample = convert_sample(r[2])
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
    def save_stream(self, datastream: DataStream):

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
        data = self.filter_sort_datapoints(data)
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

                # save metadata in SQL store
                self.sql_data.save_stream_metadata(stream_id, stream_name, owner_id,
                                                   data_descriptor, execution_context,
                                                   annotations,
                                                   stream_type, new_start_time, new_end_time)
                if self.nosql_store=="hdfs":
                    self.write_hdfs_day_file(owner_id, stream_id, data)
                else:
                    # save raw sensor data in Cassandra
                    self.save_raw_data(stream_id, data)
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

        #Data Processing loop
        for row in data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)

        #Data Write loop
        for day, dps in outputdata.items():
            filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
            try:
                if hdfs.exists(filename):
                    with hdfs.open(filename, "rb") as curfile:
                        existing_data = curfile.read()
                if existing_data is not None:
                    existing_data = pickle.loads(existing_data)
                    existing_data.extend(dps)
                    dps = existing_data
                dps = self.filter_sort_datapoints(dps)
                with hdfs.open(filename, "wb") as f:
                    pickle.dump(dps, f)
            except Exception as ex:
                self.logging.log(
                    error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)

    # def write_hdfs_day_file(self, participant_id: uuid, stream_id: uuid, data: DataPoint):
    #     """
    #
    #     :param participant_id:
    #     :param stream_id:
    #     :param data:
    #     """
    #     # Using libhdfs
    #     hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
    #     day = None
    #
    #     # if the data appeared in a different day then this shall put that day in correct day
    #     chunked_data = []
    #     existing_data = None
    #
    #     if len(data)==1:
    #         filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #         try:
    #             if hdfs.exists(filename):
    #                 with hdfs.open(filename, "rb") as curfile:
    #                     existing_data = curfile.read()
    #             if existing_data is not None:
    #                 existing_data = deserialize_obj(existing_data)
    #                 chunked_data.extend(existing_data)
    #                 #TODO: remove duplicate
    #             with hdfs.open(filename, "wb") as f:
    #                 pickle.dump(chunked_data, f)
    #         except Exception as ex:
    #             self.logging.log(error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
    #     else:
    #         current_day = None
    #         for row in data:
    #             current_day = row.start_time.strftime("%Y%m%d")
    #             if day is None:
    #                 day = row.start_time.strftime("%Y%m%d")
    #                 chunked_data.append(row)
    #             elif day!=current_day:
    #                 filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #                 # if file exist then, retrieve, deserialize, concatenate, serialize again, and store
    #                 if hdfs.exists(filename):
    #                     with hdfs.open(filename, "rb") as curfile:
    #                         existing_data = curfile.read()
    #                 if existing_data is not None:
    #                     existing_data = pickle.loads(existing_data)
    #                     chunked_data.extend(existing_data)
    #                 # TODO: remove duplicate
    #
    #                 try:
    #                     with hdfs.open(filename, "wb") as f:
    #                         pickle.dump(chunked_data, f)
    #                 except Exception as ex:
    #                     self.logging.log(
    #                         error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
    #
    #                 day = row.start_time.strftime("%Y%m%d")
    #                 chunked_data =[]
    #                 chunked_data.append(row)
    #             else:
    #                 day = row.start_time.strftime("%Y%m%d")
    #                 chunked_data.append(row)
    #             existing_data = None

    # def write_hdfs_day_file(self, participant_id: uuid, stream_id: uuid, data: DataPoint):
    #     """
    #
    #     :param participant_id:
    #     :param stream_id:
    #     :param data:
    #     """
    #     # Using libhdfs
    #     hdfs = pyarrow.hdfs.connect(self.hdfs_ip, self.hdfs_port)
    #
    #     data = self.serialize_datapoints_batch(data)
    #
    #     day = None
    #
    #     # if the data appeared in a different day then this shall put that day in correct day
    #     chunked_data = []
    #     existing_data = None
    #     for row in data:
    #         if day is None:
    #             day = row[2]
    #             chunked_data.append(row)
    #             if len(data)==1:
    #                 filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #                 try:
    #                     if hdfs.exists(filename):
    #                         with hdfs.open(filename, "rb") as curfile:
    #                             existing_data = curfile.read()
    #                     if existing_data is not None:
    #                         existing_data = deserialize_obj(existing_data)
    #                         chunked_data.extend(existing_data)
    #                     with hdfs.open(filename, "wb") as f:
    #                         pickle.dump(chunked_data, f)
    #                 except Exception as ex:
    #                     self.logging.log(error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
    #
    #         elif day!=row[2]:
    #             filename = self.raw_files_dir+str(participant_id)+"/"+str(stream_id)+"/"+str(day)+".pickle"
    #             # if file exist then, retrieve, deserialize, concatenate, serialize again, and store
    #             if hdfs.exists(filename):
    #                 with hdfs.open(filename, "rb") as curfile:
    #                     existing_data = curfile.read()
    #             if existing_data is not None:
    #                 existing_data = deserialize_obj(existing_data)
    #                 chunked_data.extend(existing_data)
    #             #chunked_data = list(set(chunked_data)) # remove duplicate
    #             try:
    #                 with hdfs.open(filename, "wb") as f:
    #                     pickle.dump(chunked_data, f)
    #             except Exception as ex:
    #                 self.logging.log(
    #                     error_message="Error in writing data to HDFS. STREAM ID: " + str(stream_id)+ "Owner ID: " + str(participant_id)+ "Files: " + str(filename)+" - Exception: "+str(ex), error_type=self.logtypes.DEBUG)
    #
    #             day = row[2]
    #             chunked_data =[]
    #             chunked_data.append(row)
    #         else:
    #             day = row[2]
    #             chunked_data.append(row)
    #         existing_data = None
    #
    def save_raw_data(self, stream_id: uuid, datapoints: DataPoint):

        """

        :param stream_id:
        :param datapoints:
        """
        datapoints = self.serialize_datapoints_batch(datapoints)
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
        except Exception as e:
            self.logging.log(
                error_message="STREAM ID: " + str(stream_id) + " - Cannot save raw data. " + str(traceback.format_exc()),
                error_type=self.logtypes.CRITICAL)

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
