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
from cerebralcortex.core.metadata_manager.metadata import Metadata
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

class TestFileToDataStream(unittest.TestCase):
    def setUp(self):
        self.CC = CerebralCortex()
        self.filetodb = FileToDB(self.CC)
        self.data_dir = "/home/ali/IdeaProjects/MD2K_DATA/data/"
        metadata = Metadata()
        self.owner = "11111111-107f-3624-aff2-dc0e0b5be53d"
        self.dd = [metadata.DataDescriptor("float", "milliseconds", None)]
        self.ec = metadata.ExecutionContext({}, {}, {}, None)
        self.annotations = metadata.Annotations("test-stream", "00000000-107f-3624-aff2-dc0e0b5be53d")
        self.data = self.get_datapoints()

        with open("test_data/kafka_msg.txt") as f:
            self.kafka_msg = json.loads(f.read())

    # def test_01_save_data(self):
    #     self.filetodb.file_processor(self.kafka_msg, self.data_dir, False, True)

    def test_02_get_data(self):
        ds = self.CC.get_stream("044504f7-107f-3624-aff2-dc0e0b5be53d", "00162d05-3248-4b7d-b4f6-8593b4faaa63", "20171113")
        print(ds)

    def get_datapoints(self):
        for dp in range(400):

    
if __name__ == '__main__':
    unittest.main()
