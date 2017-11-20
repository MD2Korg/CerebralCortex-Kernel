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

import uuid
from datetime import datetime
from enum import Enum
from typing import List

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
from pytz import timezone

from core.data_manager.sql.data import Data
from core.datatypes.datapoint import DataPoint
from core.datatypes.datastream import DataStream
from core.util.data_types import convert_sample


class DataSet(Enum):
    COMPLETE = 1,
    ONLY_DATA = 2,
    ONLY_METADATA = 3


class StreamHandler():
    def __init__(self):
        pass

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

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
        if not stream_id or not day:
            return None

        where_clause = "identifier=" + str(stream_id) + " and day='" + str(day) + "'"

        if start_time:
            where_clause += " and start_time>=cast('" + start_time + "' as timestamp)"

        if end_time:
            where_clause += " and start_time<=cast('" + end_time + "' as timestamp)"

        # query datastream(mysql) for metadata
        datastream_metadata = Data(self.CC).get_stream_info(stream_id)

        if data_type == DataSet.COMPLETE:
            dps = self.load_cassandra_data(stream_id, where_clause=where_clause)
            data = self.row_to_datapoints(dps)
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, data)
        elif data_type == DataSet.ONLY_DATA:
            dps = self.load_cassandra_data(stream_id, where_clause=where_clause)
            data = self.row_to_datapoints(dps)
            return data
        elif data_type == DataSet.ONLY_METADATA:
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datastream_metadata, None)
        else:
            raise ValueError("Failed to get data stream. Invalid type parameter.")
        return stream

    def load_cassandra_data(self, where_clause=None) -> List:
        """

        :param stream_id:
        :param datapoints:
        :param batch_size:
        """
        cluster = Cluster([self.hostIP], port=self.hostPort)

        session = cluster.connect(self.keyspace_name)

        query = "SELECT start_time,end_time, sample FROM " + self.datapoint_table + " " + where_clause
        statement = SimpleStatement(query)
        data = []
        for row in session.execute(statement):
            data.append(row)

        session.shutdown()
        cluster.shutdown()

        return data

    def get_stream_samples(self, stream_id, day, start_time=None, end_time=None) -> List[DataPoint]:
        """
        returns list of DataPoint objects
        :param stream_id:
        :param day:
        :param start_time:
        :param end_time:
        :return:
        """
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

    def row_to_datapoints(self, rows: object) -> List[DataPoint]:
        """
        Convert Cassandra rows into DataPoint list
        :param rows:
        :return:
        """
        dps = []
        if rows:
            for row in rows:
                sample = convert_sample(row[2])
                # Caasandra timezone is already in UTC. Adding timezone again would double the timezone value
                if self.CC.timezone != 'UTC':
                    localtz = timezone(self.CC.timezone)
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
        data_descriptor = datastream.data_descriptor
        execution_context = datastream.execution_context
        annotations = datastream.annotations
        stream_type = datastream.datastream_type
        data = datastream.data

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
            Data(self.CC).save_stream_metadata(stream_id, stream_name, owner_id,
                                               data_descriptor, execution_context,
                                               annotations,
                                               stream_type, new_start_time, new_end_time)

            # save raw sensor data in Cassandra
            self.save_raw_data(stream_id, data)

    def save_raw_data(self, stream_id: uuid, datapoints: DataPoint):

        """

        :param stream_id:
        :param datapoints:
        """
        cluster = Cluster([self.host_ip], port=self.host_port)

        session = cluster.connect(self.keyspace_name)

        qry_without_endtime = session.prepare(
            "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, sample) VALUES (?, ?, ?, ?)")
        qry_with_endtime = session.prepare(
            "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, end_time, sample) VALUES (?, ?, ?, ?, ?)")

        if isinstance(stream_id, str):
            stream_id = uuid.UUID(stream_id)

        for data_block in self.datapoints_to_cassandra_sql_batch(stream_id, datapoints, qry_without_endtime,
                                                                 qry_with_endtime):
            session.execute(data_block)
            data_block.clear()
        session.shutdown();
        cluster.shutdown();

    def datapoints_to_cassandra_sql_batch(self, stream_id: uuid, datapoints: DataPoint, qry_without_endtime: str,
                                          qry_with_endtime: str):

        """

        :param stream_id:
        :param datapoints:
        :param qry_without_endtime:
        :param qry_with_endtime:
        """
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch.clear()
        dp_number = 1
        for dp in datapoints:
            day = dp.start_time.strftime("%Y%m%d")
            sample = dp.sample
            if dp.end_time:
                insert_qry = qry_with_endtime
            else:
                insert_qry = qry_without_endtime

            if dp_number > self.batch_size:
                yield batch
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                # just to make sure batch does not have any existing entries.
                batch.clear()
                dp_number = 1
            else:
                if dp.end_time:
                    batch.add(insert_qry, (stream_id, day, dp.start_time, dp.end_time, sample))
                else:
                    batch.add(insert_qry, (stream_id, day, dp.start_time, sample))
                dp_number += 1
        yield batch
