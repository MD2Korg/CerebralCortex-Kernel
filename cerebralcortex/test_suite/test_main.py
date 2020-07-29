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
import pathlib
import unittest
import warnings

from cerebralcortex import Kernel
from cerebralcortex.test_suite.test_sql_storage import SqlStorageTest
from cerebralcortex.test_suite.test_nosql_storage import NoSqlStorageTest


class TestCerebralCortex(unittest.TestCase, NoSqlStorageTest, SqlStorageTest):

    def setUp(self):
        """
        Setup test params to being testing with.

        Notes:
            DO NOT CHANGE PARAMS DEFINED UNDER TEST-PARAMS! OTHERWISE TESTS WILL FAIL. These values are hardcoded in util/data_helper file as well.
        """
        warnings.simplefilter("ignore")
        config_filepath = "./../../conf/"

        # create sample_data directory. Note: make sure this path is same as the filesystem path in cerebralcortex.yml
        pathlib.Path("./sample_data/").mkdir(parents=True, exist_ok=True)

        self.study_name = "default"
        self.CC = Kernel(config_filepath, study_name=self.study_name, new_study=True)
        self.cc_conf = self.CC.config

        # TEST-PARAMS
        # sql/nosql params
        self.stream_name = "battery--org.md2k.phonesensor--phone"
        self.stream_version = 1
        self.metadata_hash = "48e367f8-12f1-32d5-8d18-d17a8dae82ef"
        self.username = "test_user"
        self.user_id = "bfb2ca0c-e19c-3956-9db2-5459ccadd40c"
        self.user_password = "test_password"
        self.user_password_encrypted = "10a6e6cc8311a3e2bcc09bf6c199adecd5dd59408c343e926b129c4914f3cb01"
        self.user_role = "test_role"
        self.auth_token = "xxx"
        self.user_metadata = {"study_name": self.study_name}
        self.user_settings = {"mcerebrum":"confs"}

        # object test params
        self.bucket_name = "test_bucket"
        self.obj_file_path = os.getcwd() + "/sample_data/objects/test_obj.zip"
        self.obj_metadata_file_path = os.getcwd() + "/sample_data/objects/test_obj.json"

        # kafka test params
        self.test_topic_name = "test_topic"
        self.test_message = "{'msg1':'some test message'}"

    def test_00(self):
        """
        This test will create required entries in sql database.

        """
        try:
            if not os.path.isdir(self.cc_conf["filesystem"]["filesystem_path"]):
                os.mkdir(self.cc_conf["filesystem"]["filesystem_path"])
            self.CC.create_user(self.username, self.user_password, self.user_role, self.user_metadata, self.user_settings)
        except:
            pass