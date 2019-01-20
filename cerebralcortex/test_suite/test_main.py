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
import unittest
import yaml
import warnings

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.test_suite.test_stream import DataStreamTest

class TestCerebralCortex(unittest.TestCase, DataStreamTest):

    def setUp(self):
        warnings.simplefilter("ignore")
        config_filepath = "./../../conf/"

        self.CC = CerebralCortex(config_filepath, auto_offset_reset="smallest")
        self.cc_conf = self.CC.config

        # DO NOT CHANGE THESE VALUES! OTHERWISE TESTS WILL FAIL. These values are hardcoded in util/data_helper file
        self.stream_name = "BATTERY--org.md2k.phonesensor--PHONE"
        self.stream_version = "1"
        self.metadata_hash = "45afcf70-ac08-3c08-807a-043315e33858"
        self.username = "test_user"
        self.user_id = "00000000-afb8-476e-9872-6472b4e66b68"
        self.user_password = "test_password"
        self.study_name = "test_study"

    def tearDown(self):
        pass
