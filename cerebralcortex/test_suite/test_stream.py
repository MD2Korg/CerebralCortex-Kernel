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

from datetime import datetime

from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata


class DataStreamTest:

    def test_01_save_stream(self):
        """
        Test functionality related to save a stream

        """
        data = gen_phone_battery_data()
        metadata = gen_phone_battery_metadata()
        ds = DataStream(data, metadata)
        #dd = ds.filter_user("dfce1e65-2882-395b-a641-93f31748591b")
        result = self.CC.save_stream(ds)

        self.assertEqual(result, True)

    def test_02_get_stream(self):
        """
        Test functionality related to get a stream

        """
        ds = self.CC.get_stream(self.stream_name)
        data = ds.data
        metadata = ds.metadata[0]

        datapoint = data.take(1)

        self.assertEqual(datapoint[0][0], datetime(2019, 1, 9, 11, 49, 28))
        self.assertEqual(datapoint[0][1], '21600000')
        self.assertEqual(datapoint[0][2], 92)
        self.assertEqual(datapoint[0][3], 1)
        self.assertEqual(datapoint[0][4], self.user_id)
        self.assertEqual(data.count(), 999)

        self.assertEqual(len(metadata.data_descriptor), 1)
        self.assertEqual(len(metadata.modules), 1)

        self.assertEqual(metadata.metadata_hash, self.metadata_hash)
        self.assertEqual(metadata.name, self.stream_name)
        self.assertEqual(metadata.version, int(self.stream_version))
        self.assertEqual(metadata.data_descriptor[0]._name, 'level')
        self.assertEqual(metadata.data_descriptor[0]._type, 'float')
        self.assertEqual(metadata.data_descriptor[0]._attributes.get("description"), 'current battery charge')
        self.assertEqual(metadata.modules[0]._name, 'battery')
        self.assertEqual(metadata.modules[0]._version, '1.2.4')
        self.assertEqual(metadata.modules[0]._authors[0].get("test_user"), 'test_user@test_email.com')
