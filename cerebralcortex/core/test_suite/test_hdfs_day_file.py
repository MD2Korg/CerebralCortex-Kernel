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


class TestFileToDataStream(unittest.TestCase):
    def setUp(self):
        self.CC = CerebralCortex()
        self.filetodb = FileToDB(self.CC)
        self.data_dir = "/home/ali/IdeaProjects/MD2K_DATA/data/"
        with open("test_data/kafka_msg.txt") as f:
            self.kafka_msg = json.loads(f.read())

    def test_01_save_data(self):
        self.filetodb.file_processor(self.kafka_msg, self.data_dir, False, True)

    # def test_02_get_data(self):
    #     ds = self.CC.get_stream("7c75c079-af50-3ae1-a952-f61a6be32a8a", "00ab666c-afb8-476e-9872-6472b4e66b68", "20171122")
    #     print(ds)

    

if __name__ == '__main__':
    unittest.main()
