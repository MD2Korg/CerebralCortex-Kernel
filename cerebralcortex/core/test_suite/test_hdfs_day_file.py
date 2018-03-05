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
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB
from cerebralcortex.core.test_suite.util.gen_test_data import get_datapoints
from cerebralcortex.core.datatypes.datastream import DataStream

class TestFileToDataStream(unittest.TestCase):
    def setUp(self):
        self.CC = CerebralCortex()
        self.filetodb = FileToDB(self.CC)
        self.data_dir = "test_data/raw/11111111-107f-3624-aff2-dc0e0b5be53d/20171122/00000000-107f-3624-aff2-dc0e0b5be53d/"

        with open(self.data_dir+"7b3538af-1299-4504-b8fd-62683c66578e.json") as f:
            self.metadata = json.loads(f.read())

        self.owner = "11111111-107f-3624-aff2-dc0e0b5be53d"
        self.stream_id = "00000000-107f-3624-aff2-dc0e0b5be53d"
        self.stream_name = "org.md2k.test_suite.hdfs_test"
        self.data = get_datapoints(10000)
        ds = DataStream(self.stream_id, self.owner, self.stream_name, self.metadata["data_descriptor"], self.metadata["execution_context"], self.metadata["annotations"], "ds", None, None, self.data)


        with open("test_data/kafka_msg.txt") as f:
            self.kafka_msg = json.loads(f.read())

    # def test_01_save_data(self):
    #     self.filetodb.file_processor(self.kafka_msg, self.data_dir, False, True)

    def test_02_get_data(self):
        ds = self.CC.get_stream("044504f7-107f-3624-aff2-dc0e0b5be53d", "00162d05-3248-4b7d-b4f6-8593b4faaa63", "20171113")
        print(ds)



    
if __name__ == '__main__':
    unittest.main()
