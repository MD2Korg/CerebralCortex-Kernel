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
import gzip
import json
import os
import traceback
import uuid

import os.path
from influxdb import InfluxDBClient

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.util.data_types import convert_sample, serialize_obj



class FileToDB():
    '''This class is responsible to read .gz files and insert data in Cassandra/ScyllaDB OR HDFS and Influx.
    This class is only for CC internal use.'''

    def __init__(self, CC, ksp_config):
        """

        :param CC: CerebralCortex Configurations
        """
        self.config = CC.config

        #self.rawData = CC.RawData
        self.nosql = CC.RawData.nosql
        #self.nosql_store = CC.RawData.nosql_store

        self.sql_data = CC.SqlData

        self.data_replay_type = ksp_config["data_replay"]["replay_type"]

        # pseudo factory
        # if self.nosql_store == "hdfs":
        #     self.hdfs = pyarrow.hdfs.connect(self.CC.RawData.hdfs_ip, self.CC.RawData.hdfs_port)
        # elif self.nosql_store=="filesystem":
        #     self.filesystem_path = self.CC.RawData.filesystem_path
        # elif self.nosql_store=="aws_s3":
        #     self.minio_input_bucket = self.CC.RawData.minio_input_bucket
        #     self.minio_output_bucket = self.CC.RawData.minio_output_bucket
        #     self.minio_dir_prefix = self.CC.RawData.minio_dir_prefix
        # else:
        #     raise ValueError(self.nosql_store + " is not supported.")

        self.logging = CC.logging
        self.logtypes = LogTypes()

        if self.config['visualization_storage']!="none":
            self.influxdbIP = self.config['influxdb']['host']
            self.influxdbPort = self.config['influxdb']['port']
            self.influxdbDatabase = self.config['influxdb']['database']
            self.influxdbUser = self.config['influxdb']['db_user']
            self.influxdbPassword = self.config['influxdb']['db_pass']
            self.influx_blacklist = ksp_config["influxdb_blacklist"]

            self.influx_batch_size = 10000
            self.influx_day_datapoints_limit = 10000




    def file_processor(self, msg: dict, zip_filepath: str, influxdb_insert: bool = False, nosql_insert: bool = True):

        """
        Process raw .gz files' data and json metadata files, convert into DataStream object format and store data in CC data-stores
        :param msg: Kafka message in json format
        :param zip_filepath: data folder path where all the gz/json files are located
        :param influxdb_insert: Turn on/off influxdb data ingestion
        :param nosql_insert: Turn on/off nosql data ingestion

        """

        if self.config['visualization_storage']=="none" and influxdb_insert:
            raise ValueError("visualization_storage param is set to none in cerebralcortex.yml. Please provide proper configuration for visualization storage.")

        if not msg:
            return []
        if not isinstance(msg["metadata"], dict):
            metadata_header = json.loads(msg["metadata"])
        else:
            metadata_header = msg["metadata"]

        stream_day = None

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

        owner_name = self.sql_data.get_user_name(owner)
        if "day" in msg:
            stream_day = msg["day"]
        if isinstance(msg["filename"], str):
            filenames = msg["filename"].split(",")
        else:
            filenames = msg["filename"]
        # influxdb_data = ""
        nosql_data = []
        all_data = []
        influxdb_client = None
        print("PROCESSING (owner, stream): ", owner, stream_id)
        if isinstance(stream_id, str):
            stream_id = uuid.UUID(stream_id)

        if influxdb_insert:
            influxdb_client = InfluxDBClient(host=self.influxdbIP, port=self.influxdbPort,
                                             username=self.influxdbUser,
                                             password=self.influxdbPassword, database=self.influxdbDatabase)
        if influxdb_insert or nosql_insert:
            if self.data_replay_type == "mydb":
                for filename in filenames:
                    if os.path.exists(str(zip_filepath + filename)):
                        all_data = self.line_to_sample(zip_filepath + filename, stream_id, owner, owner_name, name,
                                                       data_descriptor,
                                                       influxdb_insert, influxdb_client, nosql_insert)
                        if nosql_insert:
                            if not self.sql_data.is_day_processed(owner, stream_id, stream_day):
                                nosql_data.extend(all_data)
                                all_data.clear()
                    else:
                        print("Path does not exist:", str(zip_filepath + filename))
            else:
                if os.path.exists(zip_filepath + str(filenames[0])):
                    all_data = self.line_to_sample(zip_filepath + str(filenames[0]), stream_id, owner, owner_name, name,
                                                   data_descriptor,
                                                   influxdb_insert, influxdb_client, nosql_insert)
                    if nosql_insert:
                        nosql_data = all_data

            self.nosql.write_file(owner, stream_id, nosql_data)
            self.sql_data.save_stream_metadata(stream_id, name, owner, data_descriptor, execution_context,
                                               annotations, stream_type, nosql_data[0].start_time,
                                               nosql_data[len(nosql_data) - 1].start_time)

            nosql_data.clear()
            all_data.clear()

            # mark day as processed in data_replay table
            if self.data_replay_type == "mydb":
                self.sql_data.mark_processed_day(owner, stream_id, stream_day)

    def line_to_sample(self, filename, stream_id, stream_owner_id, stream_owner_name, stream_name,
                       data_descriptor, influxdb_insert, influxdb_client, nosql_insert):

        """
        Converts a gz file lines into sample values format and influxdb object
        :param filename:
        :param stream_id:
        :param stream_owner_id:
        :param stream_owner_name:
        :param stream_name:
        :param data_descriptor:
        :param influxdb_insert:
        :param nosql_insert:

        """

        grouped_samples = []
        line_number = 1
        current_day = None  # used to check boundry condition. For example, if half of the sample belong to next day
        last_start_time = None
        datapoints = []
        line_count = 0
        line_protocol = ""
        fields = ""

        if self.influx_blacklist:
            blacklist_streams = self.influx_blacklist.values()

        if data_descriptor:
            total_dd_columns = len(data_descriptor)
            data_descriptor = data_descriptor
        else:
            data_descriptor = []
            total_dd_columns = 0

        try:
            with gzip.open(filename) as lines:
                for line in lines:
                    line_count += 1
                    line = line.decode('utf-8')

                    try:
                        ts, offset, sample = line.split(',', 2)
                        bad_row = 0  # if line is not properly formatted then rest of the code shall not be executed
                    except:
                        bad_row = 1

                    if bad_row == 0:
                        start_time = int(ts) / 1000.0
                        offset = int(offset)
                        # TODO: improve the performance of sample parsing
                        if nosql_insert == True:
                            values = convert_sample(sample, stream_name)
                        elif influxdb_insert == True and stream_name not in blacklist_streams and line_count < self.influx_day_datapoints_limit:
                            values = convert_sample(sample, stream_name)

                        ############### START INFLUXDB BLOCK
                        if influxdb_insert and line_count < self.influx_day_datapoints_limit:
                            if stream_name not in blacklist_streams:
                                measurement_and_tags = '%s,owner_id=%s,owner_name=%s,stream_id=%s' % (
                                    str(stream_name.replace(" ", "_")), str(stream_owner_id), str(stream_owner_name),
                                    str(stream_id))

                                try:
                                    if isinstance(values, list):
                                        for i, sample_val in enumerate(values):
                                            if isinstance(sample_val, str):
                                                ptrn = '%s="%s",'
                                            else:
                                                ptrn = '%s=%s,'
                                            if len(values) == total_dd_columns:
                                                dd = data_descriptor[i]
                                                if "NAME" in dd:
                                                    fields += ptrn % (
                                                        str(dd["NAME"]).replace(" ", "-"), sample_val)
                                                else:
                                                    fields += ptrn % ('value_' + str(i), sample_val)
                                            else:
                                                fields += ptrn % ('value_' + str(i), sample_val)
                                    elif len(data_descriptor) > 0:
                                        dd = data_descriptor[0]
                                        if isinstance(values, str):
                                            ptrn = '%s="%s",'
                                        else:
                                            ptrn = '%s=%s,'
                                        if "NAME" in dd:
                                            fields = ptrn % (
                                                str(dd["NAME"]).replace(" ", "-"), values)
                                        else:
                                            fields = ptrn % ('value_0', values)
                                    else:
                                        if isinstance(values, str):
                                            ptrn = '%s="%s",'
                                        else:
                                            ptrn = '%s=%s,'
                                        fields = ptrn % ('value_0', values)
                                except Exception as e:
                                    try:
                                        values = json.loads(values)
                                        fields = '%s="%s",' % ('value_0', values)
                                    except Exception as e:
                                        if isinstance(values, str):
                                            ptrn = '%s="%s",'
                                        else:
                                            ptrn = '%s=%s,'
                                        fields = ptrn % ('value_0', values)
                                line_protocol += "%s %s %s\n" % (measurement_and_tags, fields.rstrip(","), str(
                                    int(ts) * 1000000))  # line protocol requires nanoseconds accuracy for timestamp
                                measurement_and_tags = ""
                                fields = ""
                        elif influxdb_client is not None and influxdb_insert and line_count > self.influx_day_datapoints_limit:
                            try:
                                influxdb_client.write_points(line_protocol, protocol="line")
                                line_protocol = ""
                                line_count=0
                            except:
                                self.logging.log(
                                    error_message="STREAM ID: " + str(stream_id) + "Owner ID: " + str(stream_owner_id) + "Files: " + str(
                                        filename) + " - Error in writing data to influxdb. " + str(
                                        traceback.format_exc()), error_type=self.logtypes.CRITICAL)


                        ############### END INFLUXDB BLOCK

                        ############### START OF NO-SQL (HDFS) DATA BLOCK
                        start_time_dt = datetime.datetime.utcfromtimestamp(start_time)

                        grouped_samples.append(DataPoint(start_time_dt, None, offset, values))

                        ############### END OF NO-SQL (HDFS) DATA BLOCK

                if influxdb_client is not None and influxdb_insert and line_protocol is not None and line_protocol!="":
                    try:
                        influxdb_client.write_points(line_protocol, protocol="line")
                        line_protocol = ""
                        line_count=0
                    except:
                        self.logging.log(
                            error_message="STREAM ID: " + str(stream_id) + "Owner ID: " + str(stream_owner_id) + "Files: " + str(
                                filename) + " - Error in writing data to influxdb. " + str(
                                traceback.format_exc()), error_type=self.logtypes.CRITICAL)

                return grouped_samples
        except:
            self.logging.log(error_message="STREAM ID: " + str(stream_id) + " - Cannot process file data. " + str(
                traceback.format_exc()), error_type=self.logtypes.MISSING_DATA)
            if line_count > self.influx_day_datapoints_limit:
                line_protocol = ""
            return grouped_samples
