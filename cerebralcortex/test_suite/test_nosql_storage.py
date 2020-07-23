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

class NoSqlStorageTest:

    def test__get_storage_path(self):
        result = self.CC.RawData._get_storage_path()
        self.assertTrue(len(result)>0)

    def test__path_exist(self):
        result = self.CC.RawData._path_exist()
        self.assertEqual(result, True)

    def test__ls_dir(self):
        result = self.CC.RawData._ls_dir()
        self.assertTrue(len(result)>0)

    def test__create_dir(self):
        result = self.CC.RawData._create_dir()
        self.assertTrue(len(result)>0)

    def test_write_pandas_to_parquet_file(self):
        test_list = [['a','b','c'], ['AA','BB','CC']]
        pdf  = pd.DataFrame(test_list, columns=['col_A', 'col_B', 'col_C'])
        result = self.CC.RawData.write_pandas_to_parquet_file(df=pdf, stream_name="pandas-to-parquet-test-stream", stream_version=1, user_id="test_user")
        self.assertTrue(len(result)>0)

    def test_is_study(self):
        result = self.CC.RawData.is_study()
        self.assertEqual(result, True)

    def test_is_stream(self):
        result = self.CC.RawData.is_stream(stream_name=self.stream_name)
        self.assertEqual(result, True)

    def test_get_stream_versions(self):
        result = self.CC.RawData.get_stream_versions(stream_name=self.stream_name)
        self.assertTrue(len(result)>0)

    def test_list_streams(self):
        result = self.CC.RawData.list_streams()
        print("Stream names", result)
        self.assertTrue(len(result)>0)

    # def test_list_users(self):
    #     result = self.CC.RawData.list_users(stream_name=self.stream_name)
    #     self.assertTrue(len(result)>0)

    def test_search_stream(self):
        result = self.CC.RawData.search_stream(stream_name="battery")
        self.assertTrue(len(result)>0)
