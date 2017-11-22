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

from core.data_manager.raw.stream_handler import StreamHandler
from core.datatypes.datapoint import DataPoint
from core.datatypes.datastream import DataStream
from core.datatypes.stream_types import StreamTypes
from core.file_manager.read_handler import ReadHandler
from core.util.debuging_decorators import log_execution_time

'''It is responsible to read .gz files and insert data in Cassandra and Influx. 
This class is only for CC internal use.'''


class FileToDB(StreamHandler):
    def __init__(self, CC):
        self.CC = CC
        self.config = CC.config

        self.host_ip = self.config['cassandra']['host']
        self.host_port = self.config['cassandra']['port']
        self.keyspace_name = self.config['cassandra']['keyspace']
        self.datapoint_table = self.config['cassandra']['datapoint_table']
        self.batch_size = 999
        self.sample_group_size = 99

    @log_execution_time
    def file_processor(self, msg: dict, zip_filepath: str) -> DataStream:
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
            all_data = self.line_to_sample(lines)

            for data_block in self.line_to_batch_block(stream_id, all_data, qry_with_endtime):
                # st = datetime.datetime.now()
                session.execute(data_block)
                # data_block.clear()
                # print("Total time to insert batch ",len(data_block), datetime.datetime.now()-st)
            session.shutdown();
            cluster.shutdown();

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
        return grouped_samples
