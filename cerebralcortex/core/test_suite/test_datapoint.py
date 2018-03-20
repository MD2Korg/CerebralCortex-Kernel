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

from cerebralcortex.core.datatypes.datapoint import DataPoint

class TestDataPoints(unittest.TestCase):


    def test_01_simple_parsing(self):
        dps = []
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:21"), None, -21600000, [2,2]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:23"), None, -21600000, [3,3]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:21"), None, -21600000, [1,1]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:22"), None, -21600000, [1,2]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:23"), None, -21600000, [1,3]))
        dps.append(DataPoint(parser.parse("2018-02-21 23:28:24"), None, -21600000, [1,4]))


        #unique_dps = list(set(dps))
        clean_data = self.dedup(sorted(dps))
        #unique_dps = set(dps)

        print(len(clean_data),len(dps))
        return clean_data

    def dedup(self, data):
        result = [data[0]]
        for l in data[1:]:
            if l.start_time == result[-1].start_time:
                continue
            result.append(l)

        return result
if __name__ == '__main__':
    unittest.main()
