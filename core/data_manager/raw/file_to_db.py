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

import datetime
import json
import uuid

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from influxdb import InfluxDBClient

from core.data_manager.sql.data import SqlData as metadata
from core.datatypes.datapoint import DataPoint
from core.datatypes.datastream import DataStream
from core.datatypes.stream_types import StreamTypes
from core.file_manager.read_handler import ReadHandler
from core.util.debuging_decorators import log_execution_time

'''It is responsible to read .gz files and insert data in Cassandra and Influx. 
This class is only for CC internal use.'''


class FileToDB():
    def __init__(self, CC):
        self.CC = CC
        self.config = CC.config

        self.host_ip = self.config['cassandra']['host']
        self.host_port = self.config['cassandra']['port']
        self.keyspace_name = self.config['cassandra']['keyspace']
        self.datapoint_table = self.config['cassandra']['datapoint_table']

        self.influxdbIP = self.config['influxdb']['host']
        self.influxdbPort = self.config['influxdb']['port']
        self.influxdbDatabase = self.config['influxdb']['database']
        self.influxdbUser = self.config['influxdb']['db_user']
        self.influxdbPassword = self.config['influxdb']['db_pass']

        self.batch_size = 999
        self.sample_group_size = 99

    @log_execution_time
    def file_processor(self, msg: dict, zip_filepath: str, influxdb=True) -> DataStream:
        """
        :param msg:
        :param zip_filepath:
        :return:
        """

        if not isinstance(msg["metadata"], dict):
            metadata_header = json.loads(msg["metadata"])
        else:
            metadata_header = msg["metadata"]

        cluster = Cluster([self.host_ip], port=self.host_port)

        session = cluster.connect(self.keyspace_name)

        influxdb_client = InfluxDBClient(host=self.influxdbIP, port=self.influxdbPort, username=self.influxdbUser,
                                         password=self.influxdbPassword, database=self.influxdbDatabase)

        qry_with_endtime = session.prepare(
            "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, end_time, sample) VALUES (?, ?, ?, ?, ?)")

        stream_id = metadata_header["identifier"]
        owner = metadata_header["owner"]
        name = metadata_header["name"]
        data_descriptor = metadata_header["data_descriptor"]
        execution_context = metadata_header["execution_context"]
        if "annotations" in metadata_header:
            annotations = metadata_header["annotations"]
        else:
            annotations = {}
        if "stream_type" in metadata_header:
            stream_type = metadata_header["stream_type"]
        else:
            stream_type = StreamTypes.DATASTREAM

        try:
            if isinstance(stream_id, str):
                stream_id = uuid.UUID(stream_id)
            gzip_file_content = ReadHandler().get_gzip_file_contents(zip_filepath + msg["filename"])
            lines = gzip_file_content.splitlines()

            if influxdb == False:
                all_data = self.line_to_sample(lines)
            else:
                # FOR INFLUXDB+CASSANDRA
                all_data = self.line_to_sample_influxdb(lines, stream_id, owner, "test", name, data_descriptor)
                try:
                    st = datetime.datetime.now()
                    for influx_batch in all_data["influxdb"]:
                        influxdb_client.write_points(influx_batch)

                    print("Time took to insert in Influxdb: ", datetime.datetime.now() - st)
                except Exception as e:
                    print(e)

            for data_block in self.line_to_batch_block(stream_id, all_data["samples"], qry_with_endtime):
                session.execute(data_block)
            session.shutdown();
            cluster.shutdown();

            metadata(self.CC).save_stream_metadata(stream_id, name, owner, data_descriptor, execution_context,
                                                   annotations, StreamTypes.DATASTREAM, all_data["samples"][0][0],
                                                   all_data["samples"][len(all_data["samples"]) - 1][1])
        except Exception as e:
            print(e)
            return []

    def line_to_batch_block(self, stream_id: uuid, lines: DataPoint, insert_qry: str):

        """

        :param stream_id:
        :param lines:
        :param qry_without_endtime:
        :param qry_with_endtime:
        """
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.clear()
        line_number = 0
        for line in lines:

            start_time = line[0]
            end_time = line[1]
            day = line[2]
            sample = line[3]

            if line_number > self.batch_size:
                yield batch
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                # just to make sure batch does not have any existing entries.
                batch.clear()
                batch.add(insert_qry.bind([stream_id, day, start_time, end_time, sample]))
                line_number = 1
            else:
                batch.add(insert_qry.bind([stream_id, day, start_time, end_time, sample]))
                line_number += 1
        yield batch

    @log_execution_time
    def line_to_sample(self, lines):

        """

        :param stream_id:
        :param lines:
        :param qry_without_endtime:
        :param qry_with_endtime:
        """

        sample_batch = []
        grouped_samples = []
        line_number = 0
        for line in lines:
            ts, offset, sample = line.split(',', 2)
            start_time = int(ts) / 1000.0
            offset = int(offset)
            if line_number == 1:
                sample_batch = []
                first_start_time = datetime.datetime.fromtimestamp(start_time)
                # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
                start_day = first_start_time.strftime("%Y%m%d")
            if line_number > self.sample_group_size:
                last_start_time = datetime.datetime.fromtimestamp(start_time)
                grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
                line_number = 1
            else:
                sample_batch.append([start_time, offset, sample])
                line_number += 1
        grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
        return {"samples": grouped_samples}

    def line_to_sample_influxdb(self, lines, stream_id, stream_owner_id, stream_owner_name, stream_name,
                                data_descriptor):

        """
        Converts a gz file lines into sample values format and influxdb object
        :param stream_id:
        :param lines:
        :param qry_without_endtime:
        :param qry_with_endtime:
        """

        sample_batch = []
        grouped_samples = []
        line_number = 0
        influx_batch = []
        influx_counter = 0
        influx_data = []

        if data_descriptor:
            total_dd_columns = len(data_descriptor)
            data_descriptor = data_descriptor
        else:
            data_descriptor = []
            total_dd_columns = 0

        for line in lines:
            ts, offset, sample = line.split(',', 2)
            start_time = int(ts) / 1000.0
            offset = int(offset)

            ############### START INFLUXDB BLOCK
            object = {}
            object['measurement'] = stream_name
            object['tags'] = {'stream_id': stream_id, 'owner_id': stream_owner_id,
                              'owner_name': stream_owner_name}
            object['time'] = int(ts)

            values = sample

            try:
                object['fields'] = {}
                # TODO: This method is SUPER slow
                # values = convert_sample(values)
                if isinstance(values, list):
                    for i, sample_val in enumerate(values):
                        if len(values) == total_dd_columns:
                            dd = data_descriptor[i]
                            if "NAME" in dd:
                                object['fields'][dd["NAME"]] = sample_val
                            else:
                                object['fields']['value_' + str(i)] = sample_val
                        else:
                            object['fields']['value_' + str(i)] = sample_val
                else:
                    dd = data_descriptor[0]

                    if "NAME" in dd:
                        object['fields'][dd["NAME"]] = values
                    else:
                        object['fields']['value_0'] = values
            except:
                try:
                    values = json.dumps(values)
                    object['fields']['value_0'] = values
                except:
                    object['fields']['value_0'] = str(values)
            if influx_counter > 10000:
                influx_batch.append(influx_data)
            else:
                influx_data.append(object)
            ############### END INFLUXDB BLOCK

            if line_number == 1:
                sample_batch = []
                first_start_time = datetime.datetime.fromtimestamp(start_time)
                # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
                start_day = first_start_time.strftime("%Y%m%d")
            if line_number > self.sample_group_size:
                last_start_time = datetime.datetime.fromtimestamp(start_time)
                grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
                line_number = 1
            else:
                sample_batch.append([start_time, offset, sample])
                line_number += 1
        grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
        influx_batch.append(influx_data)
        return {"samples": grouped_samples, "influxdb": influx_batch}
