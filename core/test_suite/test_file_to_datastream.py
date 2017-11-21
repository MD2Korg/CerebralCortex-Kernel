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
import os
import unittest
import json

from pytz import timezone

from cerebralcortex import CerebralCortex
from core.file_manager.file_io import FileIO
from core.data_manager.raw.file_to_db import FileToDB

class TestFileToDataStream(unittest.TestCase):
    testConfigFile = os.path.join(os.path.dirname(__file__), 'res/test_configuration.yml')
    CC = CerebralCortex(testConfigFile)
    configuration = CC.config

    def test_01_setup_data(self):
        msg= {}
        test_dir_path = "/home/ali/IdeaProjects/CerebralCortex-2.0/core/test_suite/sample_data/"
        test_json_file = test_dir_path+"6ff7c2ff-deaf-4c2f-aff5-63228ee13540.json"
        test_gz_file = "6ff7c2ff-deaf-4c2f-aff5-63228ee13540.csv.gz"
        metadata = FileIO().read_file(test_json_file)
        msg["metadata"] = json.loads(metadata)
        msg["filename"] = test_gz_file

        #result = FileIO().file_processor(msg, test_dir_path)

        FileToDB(self.CC).file_processor(msg, test_dir_path)

        print("completed")


        


if __name__ == '__main__':
    unittest.main()
