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
import random
import unittest

from cerebralcortex.core.util.data_types import convert_sample_simple, convert_sample


class TestFileToDataStream(unittest.TestCase):
    sample_data = {}
    sample_data["string"] = 'some-string'
    sample_data["string_array"] = 'string1, string2, string3'
    sample_data["string_tuple"] = '(1,2,3,4)' #maybe
    sample_data["string_list"] = '[1,2,3,4]' #maybe
    sample_data["string_float"] = '123.0'
    sample_data["string_json"] = '{"k1": "v1", "k2": "v2"}'
    sample_data["list_of_dict"] = '[{"k1": "v1", "k2": "v2"}, {"k11": "v12"}]'

    # def test_01_simple_parsing(self):
    #     self.assertListEqual(convert_sample(self.sample_data["string"]), ['some-string'])
    #     self.assertListEqual(convert_sample(self.sample_data["float"]), [123.0])
    #     self.assertListEqual(convert_sample(self.sample_data["tuple"]), [1, 2, 3, 4])
    #     self.assertListEqual(convert_sample(self.sample_data["list"]), [1, 2, 3, 4])
    #     self.assertListEqual(convert_sample(self.sample_data["string_array"]), ['string1, string2, string3'])
    #     self.assertListEqual(convert_sample(self.sample_data["string_tuple"]), [1, 2, 3, 4])
    #     self.assertListEqual(convert_sample(self.sample_data["string_list"]), [1, 2, 3, 4])
    #     self.assertListEqual(convert_sample(self.sample_data["string_float"]), [123.0])
    #     self.assertEqual(convert_sample(self.sample_data["string_json"]), {"k1": "v1", "k2": "v2"})

    def test_02_simple_parsing(self):
        self.assertEqual(convert_sample_simple(self.sample_data["string"]), 'some-string')
        self.assertListEqual(convert_sample_simple(self.sample_data["string_array"]), ['string1', 'string2', 'string3'])
        self.assertListEqual(convert_sample_simple(self.sample_data["string_tuple"]), [1, 2, 3, 4])
        self.assertListEqual(convert_sample_simple(self.sample_data["string_list"]), [1, 2, 3, 4])
        self.assertEqual(convert_sample_simple(self.sample_data["string_float"]), 123.0)
        self.assertEqual(convert_sample_simple(self.sample_data["string_json"]), {"k1": "v1", "k2": "v2"})

    def test_03_stress_test(self):
        st = datetime.datetime.now()
        for i in range(1, 1000000):
            #sample = random.random(), random.random(), random.random(), random.random(), random.random()
            convert_sample_simple(str((1.2, 32.23, 43.23, 1.2, 23.34234, 12, 5454)))
        print("\n\nTime took to process all samples: ", datetime.datetime.now() - st)


if __name__ == '__main__':
    unittest.main()
