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
from cerebralcortex.core.test_suite.util.gen_test_data import gen_raw_data
from cerebralcortex.core.test_suite.test_hdfs_and_filesystem import TestFileToDB, TestStreamHandler
from cerebralcortex.core.test_suite.test_kafka import TestKafkaMessaging
from cerebralcortex.core.test_suite.test_sample_parsing import TestSampleParsing
from cerebralcortex.core.test_suite.test_minio import TestMinio
from cerebralcortex.core.test_suite.test_users import TestUserMySQLMethods
from cerebralcortex.core.test_suite.test_datapoint import TestDataPoints


class TestCerebralCortex(unittest.TestCase, TestDataPoints, TestUserMySQLMethods, TestMinio,TestSampleParsing, TestKafkaMessaging, TestStreamHandler):
    def setUp(self):
        warnings.simplefilter("ignore")
        test_config_filepath = "./resources/cc_test_configuration.yml"#args["test_config_filepath"]
        config_filepath ="./../../../../CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml" #args["config_filepath"]

        with open(test_config_filepath) as test_conf:
            test_conf = yaml.load(test_conf)

        with open(test_conf["sample_data"]["data_folder"] + test_conf["sample_data"]["json_file"], "r") as md:
            self.metadata = json.loads(md.read())

        self.CC = CerebralCortex(config_filepath, auto_offset_reset="smallest")
        self.cc_conf = self.CC.config

        self.owner_id = self.metadata["owner"]
        self.stream_id = self.metadata["identifier"]
        self.stream_name = self.metadata["name"]
        self.test_data_folder = test_conf["sample_data"]["data_folder"]
        self.gz_file = self.test_data_folder + test_conf["sample_data"]["gz_file"]
        self.corupt_data = test_conf["sample_data"]["corupt_data"]
        self.days = ["20180221", "20180222", "20180224"]

        # generate sample raw data file
        self.data = gen_raw_data(self.gz_file, 10000, True, "float")