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
import uuid
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet


class SqlStorageTest:

    def is_stream(self):
        result = self.CC.is_stream(self.stream_name)
        self.assertEqual(result, True)

    def get_stream_versions(self):
        versions = self.CC.get_stream_versions(self.stream_name)
        for version in versions:
            self.assertEqual(int(self.stream_version), version)

    def get_stream_name(self, metadata_hash: uuid):
        ds = self.CC.get_stream(self.stream_name, data_type=DataSet.ONLY_METADATA)
        metadata_hash = ds.metadata.get_hash()
        self.metadata_hash = metadata_hash
        stream_name = self.CC.get_stream_name(metadata_hash)
        self.assertEqual(self.stream_name, stream_name)

    def get_stream_metadata_hash(self, stream_name: str):
        metadata_hash = self.CC.get_stream_metadata_hash(self.stream_name)
        self.assertEqual(self.metadata_hash, metadata_hash)

    def get_user_id(self, user_name: str):
        user_id = self.CC.get_user_id(self.username)
        self.assertEqual(self.user_id, user_id)

    def get_user_name(self, user_id: str):
        username = self.CC.get_user_name(self.user_id)
        self.assertEqual(self.username, username)

    def get_all_users(self, study_name: str):
        all_users = self.CC.get_all_users(self.study_name)
        self.assertEqual(len(all_users), 1)
        username = all_users[0].get("username")
        user_id = all_users[0].get("user_id")

        self.assertEqual(self.username, username)
        self.assertEqual(self.user_id, user_id)

    def get_user_metadata(self, user_id: str = None, username: str = None):
        self.CC.get_user_metadata(self.user_id)
        self.CC.get_user_metadata(self.username)
        self.CC.get_user_metadata(self.user_id, self.username)

    def connect(self, username: str, password: str):
        result = self.CC.connect(self.username, self.user_password)
        self.assertEqual(result, True)

    def is_auth_token_valid(self, username: str, auth_token: str, auth_token_expiry_time: datetime):
        current_datetime = datetime.now()
        self.CC.is_auth_token_valid(self.username, self.aut_token, current_datetime)

    def update_auth_token(self, username: str, auth_token: str, auth_token_issued_time: datetime,
                          auth_token_expiry_time: datetime):
        self.CC.update_auth_token()

    def encrypt_user_password(self, user_password: str):
        result = self.CC.encrypt_user_password(self.user_password)
        self.assertEqual(result, True)
