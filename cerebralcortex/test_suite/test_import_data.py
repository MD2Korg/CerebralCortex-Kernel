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

import pandas as pd
from datetime import datetime

from pyspark.sql import functions as F
from cerebralcortex import Kernel
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata

class ImportData:
    def test_01_read_file(self):
        """
        Test functionality related to save a stream

        """
        config_filepath = "./../../conf/"

        self.CC = Kernel(config_filepath, new_study=True)

        df=self.CC.sparkSession.read.format(format).load("/Users/ali/IdeaProjects/MD2K_DATA/cc1/data/NU/00465b72-18db-3541-9698-56584da2ff2a/00465b72-18db-3541-9698-56584da2ff2a+17284+org.md2k.datakit+PRIVACY+PHONE.csv.bz2")

        df.show()

config_filepath = "./../../conf/"

CC = Kernel(config_filepath, new_study=True)

column_names = ["timestamp", "user", "version", "latitude", "longitude", "altitude", "speed", "bearing","accuracy"]
#column_names = ["timestamp","localtime","ecg","version", "user"]
df = CC.read_csv("/Users/ali/IdeaProjects/MD2K_DATA/demo/csv_data/gps_with_timestamp_column.csv", stream_name="sample-stream-data", column_names=column_names)
#df=CC.sparkSession.read.format("csv").schema("ts Date, offset Integer, data String").load("/Users/ali/IdeaProjects/MD2K_DATA/cc1/data/NU/00465b72-18db-3541-9698-56584da2ff2a/00465b72-18db-3541-9698-56584da2ff2a+17284+org.md2k.datakit+PRIVACY+PHONE.csv.bz2")
#CC.save_data_to_influxdb(df)
ss=df.window(groupByColumnName=["user"])
print(type(ss))
df.show(truncate=False)
