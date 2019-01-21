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

import os
import shutil
import unittest
import warnings

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.test_suite.test_stream import DataStreamTest
from cerebralcortex.test_suite.test_sql_storage import SqlStorageTest
from cerebralcortex.test_suite.test_object_storage import TestObjectStorage


class TestCerebralCortex(unittest.TestCase, DataStreamTest, SqlStorageTest, TestObjectStorage):

    def setUp(self):
        """
        Setup test params to being testing with.
        Notes:
            DO NOT CHANGE PARAMS DEFINED UNDER TEST-PARAMS! OTHERWISE TESTS WILL FAIL. These values are hardcoded in util/data_helper file as well.
        """
        warnings.simplefilter("ignore")
        config_filepath = "./../../conf/"

        self.CC = CerebralCortex(config_filepath, auto_offset_reset="smallest")
        self.cc_conf = self.CC.config

        # TEST-PARAMS
        # sql/nosql params
        self.stream_name = "BATTERY--org.md2k.phonesensor--PHONE"
        self.stream_version = "1"
        self.metadata_hash = "45afcf70-ac08-3c08-807a-043315e33858"
        self.username = "test_user"
        self.user_id = "dfce1e65-2882-395b-a641-93f31748591b"
        self.user_password = "test_password"
        self.user_role = "test_role"
        self.auth_token = "xxx"
        self.study_name = "test_study"
        self.user_metadata = {"study_name": self.study_name}

        # object test params
        self.bucket_name = "test_bucket"
        self.obj_file_path = os.getcwd() + "/test_data/objects/some_obj.zip"
        self.obj_metadata_file_path = os.getcwd() + "/test_data/objects/some_obj.json"

        # kafka test params
        self.test_topic_name = "test_topic"
        self.test_message = "{'msg1':'some test message'}"

    def test_00(self):
        """
        This test will create required entries in sql database.
        """
        self.CC.create_user(self.username, self.user_password, self.user_role, self.user_metadata)

    def test_9999_last(self):
        """
        Delete all the sample test data folder/files and sql entries
        """
        self.CC.delete_user(self.username)
        if self.cc_config['nosql_storage']=="filesystem":
            shutil.rmtree(os.path.join(self.cc_conf["filesystem"]["filesystem_path"], self.bucket_name))
        shutil.rmtree(os.path.join(self.cc_conf["object_storage"]["filesystem_path"], self.bucket_name))
