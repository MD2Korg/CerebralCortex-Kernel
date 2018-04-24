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

import datetime
import gzip
import json
import traceback
from typing import List

from pympler import asizeof

from cerebralcortex.core.datatypes.datastream import DataStream, DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes


class ReadHandler():

    def read_file(self, filepath: str) -> str:
        """
        Read a file and return contents
        :param filepath:
        :return: file contents
        :rtype: str
        """
        if not filepath:
            raise ValueError("File path is required field.")

        with open(filepath, "r") as file:
            data = file.read()
            file.close()
        return data

    def file_processor(self, msg: dict, zip_filepath: str) -> DataStream:
        """
        Process a Kafka or MySQL msg. Parse compressed files. Convert json metadata and data in DataStream object.
        :param msg:
        :param zip_filepath:
        :return: DataStream object with metadata and data
        :rtype: DataStream
        """

        if not isinstance(msg["metadata"], dict):
            metadata_header = json.loads(msg["metadata"])
        else:
            metadata_header = msg["metadata"]

        identifier = metadata_header["identifier"]
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
            gzip_file_content = self.get_gzip_file_contents(zip_filepath + msg["filename"])
            datapoints = list(map(lambda x: self.row_to_datapoint(x), gzip_file_content.splitlines()))
            # self.rename_file(zip_filepath + msg["filename"])

            start_time = datapoints[0].start_time
            end_time = datapoints[len(datapoints) - 1].end_time

            ds = DataStream(identifier,
                            owner,
                            name,
                            data_descriptor,
                            execution_context,
                            annotations,
                            stream_type,
                            start_time,
                            end_time,
                            datapoints)
            return ds
        except Exception as e:
            self.logging.log(error_message="In Kafka preprocessor - Error in processing file: " + str(
                msg["filename"]) + " Owner-ID: " + owner + "Stream Name: " + name + " - " + str(traceback.format_exc()),
                             error_type=self.logtypes.CRITICAL)
            return DataStream

    def row_to_datapoint(self, row: str) -> DataPoint:
        """
        Format data based on mCerebrum's current GZ-CSV format into what Cerebral
        Cortex expects
        :param row:
        :return: single DataPoint
        :rtype: DataPoint
        """
        ts, offset, values = row.split(',', 2)
        ts = int(ts) / 1000.0
        offset = int(offset)

        timezone = datetime.timezone(datetime.timedelta(milliseconds=offset))
        ts = datetime.datetime.fromtimestamp(ts, timezone)
        return DataPoint(start_time=ts, sample=values)

    def get_gzip_file_contents(self, filepath: str) -> str:
        """
        Read and return gzip compressed file contents
        :param filepath:
        :return: gzip_file_content
        :rtype: str
        """
        fp = gzip.open(filepath)
        gzip_file_content = fp.read()
        fp.close()
        gzip_file_content = gzip_file_content.decode('utf-8')
        return gzip_file_content

    def get_chunk_size(self, data: List[DataPoint]) -> int:
        """
        get chunk size of DataPoint objects in 0.75 MB blocks. This method is computationally heavy and not scalable.
        :param data:
        :return: size of a list
        :rtype: int
        """
        if len(data) > 0:
            chunk_size = 750000 / (asizeof.asizeof(data) / len(data))  # 0.75MB chunk size without metadata
            return round(chunk_size)
        else:
            return 0

    def chunks(data: str, max_len: int) -> str:
        """
        Yields max_len sized chunks with the remainder in the last
        :param data:
        :param max_len:
        """
        # TODO: default yield value needs to be set
        for i in range(0, len(data), max_len):
            yield data[i:i + max_len]
