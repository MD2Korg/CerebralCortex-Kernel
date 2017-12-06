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
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from cerebralcortex.core.file_manager.read_handler import ReadHandler
from influxdb import InfluxDBClient
from cerebralcortex.core.util.data_types import convert_sample,convert_sample_type
from cerebralcortex.core.util.debuging_decorators import log_execution_time
from cerebralcortex.core.log_manager.logging import CCLogging
from cerebralcortex.core.log_manager.log_handler import LogTypes

'''It is responsible to read .gz files and insert data in Cassandra and Influx. 
This class is only for CC internal use.'''


class FileToDB():
    def __init__(self, CC):
        self.config = CC.config

        self.sql_data = SqlData(CC)
        self.host_ip = self.config['cassandra']['host']
        self.host_port = self.config['cassandra']['port']
        self.keyspace_name = self.config['cassandra']['keyspace']
        self.datapoint_table = self.config['cassandra']['datapoint_table']
        self.cassandra_ingestion = self.config['data_ingestion']['cassandra_in']
        self.influxdb_ingestion = self.config['data_ingestion']['influxdb_in']

        self.logging = CCLogging(self.config['logging']['log_path'])
        self.logtypes = LogTypes()

        self.influxdbIP = self.config['influxdb']['host']
        self.influxdbPort = self.config['influxdb']['port']
        self.influxdbDatabase = self.config['influxdb']['database']
        self.influxdbUser = self.config['influxdb']['db_user']
        self.influxdbPassword = self.config['influxdb']['db_pass']

        self.batch_size = 10
        self.sample_group_size = 99
        self.influx_batch_size = 10000


    @log_execution_time
    def file_processor(self, msg: dict, zip_filepath: str, influxdb_insert=True):
        """
        :param msg:
        :param zip_filepath:
        :return:
        """
        if not msg:
            return []
        if not isinstance(msg["metadata"], dict):
            metadata_header = json.loads(msg["metadata"])
        else:
            metadata_header = msg["metadata"]

        influxdb_client = InfluxDBClient(host=self.influxdbIP, port=self.influxdbPort, username=self.influxdbUser,
                                         password=self.influxdbPassword, database=self.influxdbDatabase)

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
        owner_name = self.sql_data.get_user_id(owner)
        try:
            if isinstance(stream_id, str):
                stream_id = uuid.UUID(stream_id)
            gzip_file_content = ReadHandler().get_gzip_file_contents(zip_filepath + msg["filename"])
            lines = gzip_file_content.splitlines()

            all_data = self.line_to_sample(lines, stream_id, owner, owner_name, name, data_descriptor, influxdb_insert)

            if all_data["influxdb_data"] != "" and all_data["influxdb_data"]!=None:
                try:
                    influxdb_client.write_points(all_data["influxdb_data"], protocol="line")
                except:
                    self.logging.log(error_message="STREAM ID: "+str(stream_id)+" - Error in processing data for influxdb. "+str(traceback.format_exc()), error_type=self.logtypes.CRITICAL)

            # connect to cassandra
            cluster = Cluster([self.host_ip], port=self.host_port)
            session = cluster.connect(self.keyspace_name)
            qry_with_endtime = session.prepare(
                "INSERT INTO " + self.datapoint_table + " (identifier, day, start_time, end_time, sample) VALUES (?, ?, ?, ?, ?)")

            for data_block in self.line_to_batch_block(stream_id, all_data["cassandra_data"], qry_with_endtime):
                session.execute(data_block)

            session.shutdown()
            cluster.shutdown()

            self.sql_data.save_stream_metadata(stream_id, name, owner, data_descriptor, execution_context,
                                               annotations, StreamTypes.DATASTREAM, all_data["cassandra_data"][0][0],
                                               all_data["cassandra_data"][len(all_data["cassandra_data"]) - 1][1])
        except:
            self.logging.log(error_message="STREAM ID: "+str(stream_id)+" - SIZE "+str(sys.getsizeof(data_block))+" - Cannot process file data. "+str(traceback.format_exc()), error_type=self.logtypes.CRITICAL)

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

    # def line_to_sample22(self, lines):
    #
    #     """
    #
    #     :param stream_id:
    #     :param lines:
    #     :param qry_without_endtime:
    #     :param qry_with_endtime:
    #     """
    #
    #     sample_batch = []
    #     grouped_samples = []
    #     last_start_time =  None
    #     line_number = 1
    #     current_day = None # used to check boundry condition. For example, if half of the sample belong to next day
    #     for line in lines:
    #         ts, offset, sample = line.split(',', 2)
    #         sample = convert_sample_type(sample)
    #         start_time = int(ts) / 1000.0
    #         offset = int(offset)
    #         if line_number == 1:
    #             sample_batch = []
    #             first_start_time = datetime.datetime.fromtimestamp(start_time)
    #             # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
    #             start_day = first_start_time.strftime("%Y%m%d")
    #             current_day = int(start_time/86400)
    #         if line_number > self.sample_group_size:
    #             last_start_time = datetime.datetime.fromtimestamp(start_time)
    #             sample_batch.append([start_time, offset, sample])
    #             grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
    #             line_number = 1
    #         else:
    #             if (int(start_time/86400))>current_day:
    #                 start_day = datetime.datetime.fromtimestamp(start_time).strftime("%Y%m%d")
    #             sample_batch.append([start_time, offset, sample])
    #             line_number += 1
    #     if not last_start_time:
    #         last_start_time = start_time
    #     grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
    #     return {"samples": grouped_samples}

    # def line_to_sample_influxdb22(self, lines, stream_id, stream_owner_id, stream_owner_name, stream_name,
    #                             data_descriptor):
    #
    #     """
    #     Converts a gz file lines into sample values format and influxdb object
    #     :param stream_id:
    #     :param lines:
    #     :param qry_without_endtime:
    #     :param qry_with_endtime:
    #     """
    #
    #     sample_batch = []
    #     grouped_samples = []
    #     line_number = 1
    #     influx_data = []
    #     current_day = None # used to check boundry condition. For example, if half of the sample belong to next day
    #     last_start_time = None
    #
    #     if data_descriptor:
    #         total_dd_columns = len(data_descriptor)
    #         data_descriptor = data_descriptor
    #     else:
    #         data_descriptor = []
    #         total_dd_columns = 0
    #
    #     for line in lines:
    #         ts, offset, sample = line.split(',', 2)
    #         start_time = int(ts) / 1000.0
    #         offset = int(offset)
    #
    #         ############### START INFLUXDB BLOCK
    #         object = {}
    #         object['measurement'] = stream_name
    #         object['tags'] = {'stream_id': stream_id, 'owner_id': stream_owner_id,
    #                           'owner_name': stream_owner_name}
    #         object['time'] = int(ts)
    #
    #         values = sample
    #
    #         try:
    #             object['fields'] = {}
    #             # TODO: This method is SUPER slow
    #             values = convert_sample(values)
    #             if isinstance(values, list):
    #                 for i, sample_val in enumerate(values):
    #                     if len(values) == total_dd_columns:
    #                         dd = data_descriptor[i]
    #                         if "NAME" in dd:
    #                             object['fields'][dd["NAME"]] = sample_val
    #                         else:
    #                             object['fields']['value_' + str(i)] = sample_val
    #                     else:
    #                         object['fields']['value_' + str(i)] = sample_val
    #             else:
    #                 dd = data_descriptor[0]
    #
    #                 if "NAME" in dd:
    #                     object['fields'][dd["NAME"]] = values
    #                 else:
    #                     object['fields']['value_0'] = values
    #         except:
    #             try:
    #                 values = json.dumps(values)
    #                 object['fields']['value_0'] = values
    #             except:
    #                 object['fields']['value_0'] = str(values)
    #
    #         influx_data.append(object)
    #
    #         ############### END INFLUXDB BLOCK
    #
    #         ############### START OF CASSANDRA DATA BLOCK
    #         if line_number == 1:
    #             sample_batch = []
    #             first_start_time = datetime.datetime.fromtimestamp(start_time)
    #             # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
    #             start_day = first_start_time.strftime("%Y%m%d")
    #             current_day = int(start_time/86400)
    #         if line_number > self.sample_group_size:
    #             last_start_time = datetime.datetime.fromtimestamp(start_time)
    #             sample_batch.append([start_time, offset, sample])
    #             grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
    #             line_number = 1
    #         else:
    #             if (int(start_time/86400))>current_day:
    #                 start_day = datetime.datetime.fromtimestamp(start_time).strftime("%Y%m%d")
    #             sample_batch.append([start_time, offset, sample])
    #             line_number += 1
    #     if not last_start_time:
    #         last_start_time = start_time
    #     grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
    #     ############### END OF CASSANDRA DATA BLOCK
    #
    #     return {"samples": grouped_samples, "influxdb": influx_data}

    @log_execution_time
    def line_to_sample(self, lines, stream_id, stream_owner_id, stream_owner_name, stream_name,
                       data_descriptor, influxdb_insert):

        """
        Converts a gz file lines into sample values format and influxdb object
        :param stream_id:
        :param lines:
        :param qry_without_endtime:
        :param qry_with_endtime:
        """

        sample_batch = []
        grouped_samples = []
        line_number = 1
        influx_data = []
        current_day = None # used to check boundry condition. For example, if half of the sample belong to next day
        last_start_time = None
        measurement_and_tags = ""
        line_protocol = ""
        fields = ""

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
            if influxdb_insert:
                values = sample
                measurement_and_tags = "%s,owner_id=%s,owner_name=%s,stream_id=%s" % (str(stream_name),str(stream_owner_id),str(stream_owner_name),str(stream_id))

                try:
                    # TODO: This method is SUPER slow

                    values = convert_sample(values)

                    if isinstance(values, list):
                        for i, sample_val in enumerate(values):
                            if len(values) == total_dd_columns:
                                dd = data_descriptor[i]
                                if "NAME" in dd:
                                    fields += "%s=%s," % (str(dd["NAME"]).replace(" ","-"),str(sample_val).replace(" ","-"))
                                else:
                                    fields += "%s=%s," % ('value_'+str(i),str(sample_val).replace(" ","-"))
                            else:
                                fields += "%s=%s," % ('value_'+str(i),str(sample_val).replace(" ","-"))
                    else:
                        dd = data_descriptor[0]

                        if "NAME" in dd:
                            fields = "%s=%s," % (str(dd["NAME"]).replace(" ","-"),str(values).replace(" ","-"))
                        else:
                            fields = "%s=%s," % ('value_0',str(values).replace(" ","-"))
                except Exception as e:
                    self.logging.log(error_message="Sample: "+str(values)+" - Cannot parse sample. "+str(traceback.format_exc()), error_type=self.logtypes.DEBUG)
                    try:
                        values = json.dumps(values)
                        fields = "%s=%s," % ('value_0',str(values))
                    except:
                        fields = "%s=%s," % ('value_0',str(values).replace(" ","-"))
                line_protocol +="%s %s %s\n" % (measurement_and_tags,fields.rstrip(","),ts)
                measurement_and_tags = ""
                fields = ""

            ############### END INFLUXDB BLOCK

            ############### START OF CASSANDRA DATA BLOCK
            if line_number == 1:
                sample_batch = []
                first_start_time = datetime.datetime.fromtimestamp(start_time)
                # TODO: if sample is divided into two days then it will move the block into fist day. Needs to fix
                start_day = first_start_time.strftime("%Y%m%d")
                current_day = int(start_time/86400)
            if line_number > self.sample_group_size:
                last_start_time = datetime.datetime.fromtimestamp(start_time)
                sample_batch.append([start_time, offset, sample])
                grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
                line_number = 1
            else:
                if (int(start_time/86400))>current_day:
                    start_day = datetime.datetime.fromtimestamp(start_time).strftime("%Y%m%d")
                sample_batch.append([start_time, offset, sample])
                line_number += 1
        if not last_start_time:
            last_start_time = start_time
        grouped_samples.append([first_start_time, last_start_time, start_day, json.dumps(sample_batch)])
        ############### END OF CASSANDRA DATA BLOCK

        return {"cassandra_data": grouped_samples, "influxdb_data": line_protocol}