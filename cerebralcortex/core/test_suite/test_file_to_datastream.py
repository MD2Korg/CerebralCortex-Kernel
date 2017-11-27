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


import json
import os
import unittest
from dateutil import parser
from cerebralcortex.core.file_manager.file_io import FileIO

from cerebralcortex.cerebralcortex import CerebralCortex

class TestFileToDataStream(unittest.TestCase):
    CC = CerebralCortex()
    configuration = CC.config

    def test_01_save_data(self):
        msg = {}
        test_dir_path = "/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/core/test_suite/sample_data/"
        test_json_file = test_dir_path + "6ff7c2ff-deaf-4c2f-aff5-63228ee13540.json"
        test_gz_file = "6ff7c2ff-deaf-4c2f-aff5-63228ee13540.csv.gz"
        metadata = FileIO().read_file(test_json_file)
        msg["metadata"] = json.loads(metadata)
        msg["filename"] = test_gz_file

        #FileToDB(self.CC).file_processor(msg, test_dir_path, False)

    def test_02_get_data(self):
        ds = self.CC.get_stream("f28a97c6-b76a-3f96-ac78-5f142dd2d401", "24481117")
        self.assertEqual(len(ds.data), 989999)

        # metadata
        self.assertEqual(ds.owner,'fbf8d50c-7f1d-47aa-b958-9caeadc676bd')
        self.assertEqual(ds.name, 'RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST')
        self.assertEqual(ds.datastream_type, '1')
        self.assertEqual(ds.data_descriptor, [{'DATA_TYPE': 'double', 'UNIT': 'percentage', 'NAME': 'Raw Data', 'MIN_VALUE': '0', 'MAX_VALUE': '100', 'DESCRIPTION': 'Raw byte array'}])
        self.assertEqual(ds.annotations, [])
        self.assertEqual(ds.execution_context['application_metadata'], {'VERSION_NUMBER': '12099', 'NAME': 'MotionSense', 'VERSION_NAME': '0.1.20', 'DESCRIPTION': 'Collects data from the motion sense. Sensors supported: [Accelerometer, Gyroscope, Battery, LED, DataQuality]'})
        self.assertEqual(ds.execution_context['datasource_metadata'], {'DATA_TYPE': 'org.md2k.datakitapi.datatype.DataTypeDoubleArray', 'NAME': 'Raw', 'DESCRIPTION': 'Raw byte array'})
        self.assertEqual(ds.execution_context['platform_metadata'], {'NAME': 'MotionSenseHRV', 'DEVICE_ID': 'F2:27:C9:B5:23:C3'})
        self.assertEqual(ds.execution_context['processing_module'], {'input_parameters': {}, 'name': '', 'algorithm': [{'method': '', 'authors': [''], 'description': '', 'version': '', 'reference': {'url': 'http://md2k.org/'}}], 'input_streams': [], 'output_streams': [], 'description': ''})

        # first data point
        self.assertEqual(ds.data[0].start_time, parser.parse('2448-11-17 03:48:54.532000'))
        self.assertEqual(len(ds.data[0].sample),5)

        # last data point
        self.assertEqual(ds.data[len(ds.data)-1].start_time, parser.parse('2448-11-17 06:35:34.502001'))
        self.assertEqual(len(ds.data[len(ds.data)-1].sample),5)
        print("Completed")


if __name__ == '__main__':
    unittest.main()
