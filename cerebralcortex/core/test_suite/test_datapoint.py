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


from dateutil import parser
import unittest
from cerebralcortex.core.data_manager.raw.stream_handler import StreamHandler
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB

from cerebralcortex.core.datatypes.datapoint import DataPoint

class TestDataPoints():

    def test_01_sort_dedup(self):
        """
        Duplication removal and sort of a DataPoint list
        :return:
        """
        dps = []
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:21"), None, -21600000, [2,2]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:23"), None, -21600000, [3,3]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:21"), None, -21600000, [1,1]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:22"), None, -21600000, [1,2]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:23"), None, -21600000, [1,3]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:24"), None, -21600000, [1,4]))

        clean_data = StreamHandler.dedup("",sorted(dps))
        self.assertEqual(clean_data[0],dps[0])
        self.assertEqual(clean_data[1],dps[3])
        self.assertEqual(clean_data[2],dps[1])
        self.assertEqual(clean_data[3],dps[5])

    # def test_02_line_to_sample(self):
    #     file_to_db = FileToDB(self.CC)
    #     msg = {"metadata":self.metadata, "day":"20180412", "filename": self.corupt_data+","}
    #     data = file_to_db.file_processor(msg, "resources/test_data/raw/file_to_sample/", False, True)
    #     print(data)