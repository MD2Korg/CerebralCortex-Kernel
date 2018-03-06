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



import unittest
import json
import pickle
from datetime import datetime
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB
from cerebralcortex.core.test_suite.util.gen_test_data import get_datapoints
from cerebralcortex.core.datatypes.datastream import DataStream

class TestFileToDataStream(unittest.TestCase):
    def setUp(self):
        self.CC = CerebralCortex()
        self.filetodb = FileToDB(self.CC)
        self.data_dir = "test_data/raw/11111111-107f-3624-aff2-dc0e0b5be53d/20171122/00000000-107f-3624-aff2-dc0e0b5be53d/"
        self.days = ["20180221","20180223","20180224"]

        with open(self.data_dir+"7b3538af-1299-4504-b8fd-62683c66578e.json") as f:
            self.metadata = json.loads(f.read())

        with open("test_data/kafka_msg.txt") as f:
            self.kafka_msg = json.loads(f.read())

        self.owner = "11111111-107f-3624-aff2-dc0e0b5be53d"
        self.stream_id = "00000000-107f-3624-aff2-dc0e0b5be53d"
        self.stream_name = "org.md2k.test_suite.hdfs_test"
        self.data = get_datapoints(10000)



    # def test_01_filetodb(self):
    #     self.filetodb.file_processor(self.kafka_msg, self.data_dir, False, True)

    # def test_02_save_stream(self):
    #     outputdata = {}
    #
    #     #Data Processing loop
    #     for row in self.data:
    #         day = row.start_time.strftime("%Y%m%d")
    #         if day not in outputdata:
    #             outputdata[day] = []
    #
    #         outputdata[day].append(row)
    #     for day, dps in outputdata.items():
    #         ds = DataStream(self.stream_id, self.owner, self.stream_name, self.metadata["data_descriptor"], self.metadata["execution_context"], self.metadata["annotations"], "ds", None, None, dps)
    #         self.CC.save_stream(ds)

    def test_03_get_stream(self):
        data_len = []
        start_time = datetime.utcfromtimestamp(1519355692)
        end_time = datetime.utcfromtimestamp(1519355699)
        for day in self.days:
            ds = self.CC.get_stream(self.stream_id, self.owner, day)
            data_len.append(len(ds.data))
        self.assertEqual(data_len, [3999,999,5001])

        ds = self.CC.get_stream(self.stream_id, self.owner, self.days[1], start_time,end_time)
        self.assertEqual(len(ds.data), 700)


    # def test_04_test_pickle(self):
    #     with open("/home/ali/Desktop/tmp/nazir.pickle", "rb") as f:
    #         data = f.read()
    #         data = pickle.loads(data)
    #         print(data)
    #         self.CC.save_stream(data)
    #         ds = self.CC.get_stream("033e3442-6671-3e4f-84fc-11bc6576bbe7", "08149a8b-8b77-45b6-b092-c9049f9e0214", "20171126")
    #         print("done")


    
if __name__ == '__main__':
    unittest.main()
