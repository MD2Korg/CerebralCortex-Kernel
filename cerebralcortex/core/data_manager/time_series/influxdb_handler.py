# Copyright (c) 2019, MD2K Center of Excellence
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
import traceback
from datetime import datetime

from influxdb import InfluxDBClient

from cerebralcortex.core.datatypes import DataStream


class InfluxdbHandler():

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def save_data_to_influxdb(self, datastream: DataStream):
        """
        Save data stream to influxdb only for visualization purposes.

        Args:
            datastream (DataStream): a DataStream object
        Returns:
            bool: True if data is ingested successfully or False otherwise
        Todo:
            This needs to be updated with the new structure. Should metadata be stored or not?
        Example:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> ds = DataStream(dataframe, MetaData)
            >>> CC.save_data_to_influxdb(ds)
        """

        st = datetime.now()
        client = InfluxDBClient(host=self.influxdbIP, port=self.influxdbPort, username=self.influxdbUser,
                                password=self.influxdbPassword, database=self.influxdbDatabase)
        datapoints = datastream.data
        stream_identifier = datastream.identifier
        stream_owner_id = datastream.owner
        stream_owner_name = self.sql_data.get_user_name(stream_owner_id)
        stream_name = datastream.name

        if datastream.data_descriptor:
            total_dd_columns = len(datastream.data_descriptor)
            data_descriptor = datastream.data_descriptor
        else:
            data_descriptor = []
            total_dd_columns = 0

        influx_data = []
        for datapoint in datapoints:
            object = {}
            object['measurement'] = stream_name
            object['tags'] = {'stream_id': stream_identifier, 'owner_id': stream_owner_id,
                              'owner_name': stream_owner_name}

            object['time'] = datapoint.start_time
            values = datapoint.sample

            try:
                object['fields'] = {}
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
                    if len(data_descriptor)>0 and "NAME" in data_descriptor[0]:
                        object['fields'][dd["NAME"]] = values
                    else:
                        object['fields']['value_0'] = values
            except:
                try:
                    values = json.dumps(values)
                    object['fields']['value_0'] = values
                except:
                    object['fields']['value_0'] = str(values)

            influx_data.append(object)

        try:
            client.write_points(influx_data)
        except:
            self.logging.log(error_message="STREAM ID: " + stream_identifier + " - Cannot save raw data. " + str(
                traceback.format_exc()), error_type=self.logtypes.CRITICAL)
