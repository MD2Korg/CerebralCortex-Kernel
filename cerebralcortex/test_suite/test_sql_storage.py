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

import jwt
from datetime import datetime, timedelta

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from datetime import datetime, timedelta
import random
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.test_suite.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet


class SqlStorageTest:
    ###### STREAM RELATED TEST CASES #####
    def test_save_stream_metadata_00(self):
        stream_metadata = gen_phone_battery_metadata()

        result = self.CC.SqlData.save_stream_metadata(stream_metadata)
        self.assertEqual(result.get("status", False), True)


    def test_get_stream_metadata_by_name_01(self):
        result = self.CC.SqlData.get_stream_metadata_by_name(self.stream_name)
        self.assertTrue(len(result)>0)

    def test_list_streams_02(self):
        result = self.CC.SqlData.list_streams()
        self.assertTrue(len(result)>0)

    def test_search_stream_03(self):
        result = self.CC.SqlData.search_stream("battery")
        self.assertTrue(len(result) > 0)

    def test_get_stream_versions_04(self):
        result = self.CC.SqlData.get_stream_versions(self.stream_name)
        self.assertTrue(len(result) > 0)

    def test_get_stream_metadata_hash_05(self):
        result = self.CC.SqlData.get_stream_metadata_hash(self.stream_name)
        self.assertEqual(result[0].metadata_hash,self.metadata_hash)

    def test_get_stream_name_06(self):
        result = self.CC.SqlData.get_stream_name(self.metadata_hash)
        self.assertEqual(result,self.stream_name)

    def test_get_stream_metadata_by_hash_07(self):
        result = self.CC.SqlData.get_stream_metadata_by_hash(self.metadata_hash)
        self.assertEqual(result.name, self.stream_name)

    def test_is_stream_08(self):
        result = self.CC.SqlData.is_stream(self.stream_name)
        self.assertEqual(result, True)

    def test_is_metadata_changed_09(self):
        result = self.CC.SqlData._is_metadata_changed(self.stream_name, self.metadata_hash)
        self.assertEqual(result.get("status"), "exist")

    ########## USER RELATED TEST CASES #######
    def test_create_user(self):
        result = self.CC.SqlData.create_user(username=self.username, user_password=self.user_password, user_role=self.user_role,
                                             user_metadata=self.user_metadata, user_settings=self.user_settings)
        self.assertEqual(result, True)

    def test_get_user_metadata(self):
        result = self.CC.SqlData.get_user_metadata(username=self.username)
        self.assertEqual(result, self.user_metadata)

    def test_get_user_settings(self):
        result = self.CC.SqlData.get_user_settings(username=self.username)
        self.assertEqual(result, self.user_settings)

    def test_login_user(self):
        result = self.CC.SqlData.login_user(self.username, self.user_password, encrypt_password=False)
        self.assertEqual(result.get("status"), True)

    # def test_is_auth_token_valid(self):
    #     result = self.CC.SqlData.is_auth_token_valid(self.username, "")
    #     self.assertEqual(result, self.user_metadata)

    def test_list_users(self):
        result = self.CC.SqlData.list_users()
        self.assertTrue(len(result)>0)

    def test_get_username(self):
        result = self.CC.SqlData.get_username(self.user_id)
        self.assertEqual(result, self.username)

    def test_is_user(self):
        result = self.CC.SqlData.is_user(user_name=self.username)
        self.assertEqual(result, True)

    def test_get_user_id(self):
        result = self.CC.SqlData.get_user_id(self.username)
        self.assertEqual(result, self.user_id)

    # def test_update_auth_token(self):
    #     result = self.CC.SqlData.update_auth_token(self.username)
    #     self.assertEqual(result, self.user_metadata)



    # def test_01_is_stream(self):
    #     result = self.CC.is_stream(self.stream_name)
    #     self.assertEqual(result, True)
    #
    # def test_02_get_stream_versions(self):
    #     versions = self.CC.get_stream_versions(self.stream_name)
    #     for version in versions:
    #         self.assertEqual(int(self.stream_version), int(version))
    #
    # def test_03_get_stream_name(self):
    #     ds = self.CC.get_stream(self.stream_name, data_type=DataSet.ONLY_METADATA)
    #     metadata_hash = ds.metadata[0].metadata_hash
    #     stream_name = self.CC.get_stream_name(metadata_hash)
    #     self.assertEqual(self.stream_name, stream_name)
    #
    # def test_04_get_stream_metadata_hash(self):
    #     metadata_hash = self.CC.get_stream_metadata_hash(self.stream_name)[0]
    #     self.assertEqual(self.metadata_hash, metadata_hash)
    #
    # def test_05_get_user_id(self):
    #     user_id = self.CC.get_user_id(self.username)
    #     self.assertEqual(self.user_id, user_id)
    #
    # def test_06_get_user_name(self):
    #     username = self.CC.get_username(self.user_id)
    #     self.assertEqual(self.username, username)
    #
    # def test_07_get_all_users(self):
    #     all_users = self.CC.list_users()
    #     self.assertEqual(len(all_users), 1)
    #     username = all_users[0].get("username")
    #     user_id = all_users[0].get("user_id")
    #
    #     self.assertEqual(self.username, username)
    #     self.assertEqual(self.user_id, user_id)
    #
    # def test_08_get_user_metadata(self):
    #     self.CC.get_user_metadata(self.user_id)
    #     self.CC.get_user_metadata(self.username)
    #     self.CC.get_user_metadata(self.user_id, self.username)
    #
    # def test_09_encrypt_user_password(self):
    #     result = self.CC.encrypt_user_password(self.user_password)
    #     self.assertEqual(result, self.user_password_encrypted)
    #
    # def test_10_connect(self):
    #     result = self.CC.connect(self.username, self.user_password_encrypted, True)
    #     self.assertEqual(result.get("status"), True)
    #     self.assertNotEqual(result.get("msg"), "")
    #     token = result.get("auth_token")
    #
    #     decoded_token = jwt.decode(token, self.CC.config["cc"]["auth_encryption_key"], algorithm='HS256')
    #     is_valid = self.CC.is_auth_token_valid(decoded_token.get("username",""), token)
    #     self.assertEqual(is_valid, True)
    #
    #     user_settings =self.CC.SqlData.get_user_settings(auth_token=token)
    #     self.assertEqual(user_settings.get("user_id", ""), self.user_id)
    #     self.assertEqual(user_settings.get("username", ""), self.username)
    #     self.assertEqual(user_settings.get("user_settings", ""), '{"mcerebrum": "confs"}')

