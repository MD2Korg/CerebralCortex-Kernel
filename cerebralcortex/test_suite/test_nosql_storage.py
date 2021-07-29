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

from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata

class NoSqlStorageTest:
    def test_01_save_stream(self):
        """
        Test functionality related to save a stream

        """
        data = gen_phone_battery_data()
        metadata = gen_phone_battery_metadata()
        ds = DataStream(data, metadata)
        result = self.CC.save_stream(ds, overwrite=True)

        self.assertEqual(result, True)

    def test_02_stream(self):
        all_streams = self.CC.list_streams()
        searched_streams = self.CC.search_stream(stream_name="battery")

        self.assertTrue(len(all_streams)>0)

        self.assertEqual(len(searched_streams),1)
        self.assertEqual(searched_streams[0],self.stream_name)

    def test_03_get_stream(self):
        """
        Test functionality related to get a stream

        """
        ds = self.CC.get_stream(self.stream_name)
        data = ds

        metadata = ds.metadata

        datapoint = data.take(1)

        self.assertEqual(datapoint[0][0], datetime(2019, 1, 9, 11, 50, 30))
        self.assertEqual(datapoint[0][2], 91)
        self.assertEqual(datapoint[0][4], 1)
        self.assertEqual(datapoint[0][3], self.user_id)
        self.assertTrue(data.count()> 500)

        self.assertEqual(len(metadata.data_descriptor), 5)
        self.assertEqual(len(metadata.modules), 1)

        self.assertEqual(metadata.get_hash(), self.metadata_hash)
        self.assertEqual(metadata.name, self.stream_name)
        self.assertEqual(metadata.version, int(self.stream_version))
        self.assertEqual(metadata.modules[0].name, 'battery')
        self.assertEqual(metadata.modules[0].version, '1.2.4')
        self.assertEqual(metadata.modules[0].authors[0].get("test_user"), 'test_user@test_email.com')

    def test_04_get_storage_path(self):
        result = self.CC.RawData._get_storage_path()
        self.assertTrue(len(result)>0)

    def test_05_path_exist(self):
        result = self.CC.RawData._path_exist()
        self.assertEqual(result, True)

    def test_06_ls_dir(self):
        result = self.CC.RawData._ls_dir()
        self.assertTrue(len(result)>0)

    def test_07_create_dir(self):
        result = self.CC.RawData._create_dir()
        self.assertTrue(len(result)>0)

    def test_08_write_pandas_to_parquet_file(self):
        test_list = [['a','b','c'], ['AA','BB','CC']]
        pdf  = pd.DataFrame(test_list, columns=['col_A', 'col_B', 'col_C'])
        result = self.CC.RawData.write_pandas_to_parquet_file(df=pdf, stream_name="pandas-to-parquet-test-stream", stream_version=1, user_id="test_user")
        self.assertTrue(len(result)>0)

    def test_09_is_study(self):
        result = self.CC.RawData.is_study()
        self.assertEqual(result, True)

    def test_10_is_stream(self):
        result = self.CC.RawData.is_stream(stream_name=self.stream_name)
        self.assertEqual(result, True)

    def test_11_get_stream_versions(self):
        result = self.CC.RawData.get_stream_versions(stream_name=self.stream_name)
        self.assertTrue(len(result)>0)

    def test_12_list_streams(self):
        result = self.CC.RawData.list_streams()
        print("Stream names", result)
        self.assertTrue(len(result)>0)

    # def test_list_users(self):
    #     result = self.CC.RawData.list_users(stream_name=self.stream_name)
    #     self.assertTrue(len(result)>0)

    def test_14_search_stream(self):
        result = self.CC.RawData.search_stream(stream_name="battery")
        self.assertTrue(len(result)>0)
