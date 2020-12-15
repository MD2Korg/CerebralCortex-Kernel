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
import os
import unittest

from cerebralcortex_rest import client

class TestCerebralCortex(unittest.TestCase):

    auth_token = ""

    def test_00_user_routes(self):
        """
        Test user routes of API server

        """
        result = client.register_user(url="http://localhost:8089/api/v3/user/default/register", user_metadata={
            "username": "demo",
            "password": "demo",
            "user_role": "string",
            "user_metadata": {
                "key": "string",
                "value": "string"
            },
            "user_settings": {
                "key": "string",
                "value": "string"
            }
        })
        if(json.loads(result.text).get("message")!='username is already registered. Please select another user name'):
            self.assertEqual(result.status_code, 200)

        auth_token = client.login_user(url="http://localhost:8089/api/v3/user/default/login", username="demo", password="demo")
        self.assertEqual(auth_token.status_code, 200)
        self.__class__.auth_token = json.loads(auth_token.content).get("auth_token")
        user_settings = client.get_user_settings(url="http://localhost:8089/api/v3/user/default/config", auth_token=self.__class__.auth_token)
        self.assertEqual(user_settings.status_code, 200)


    def test_01_stream_routes(self):
        """
        Test stream routes of API server
        """
        result = client.register_stream(url="http://localhost:8089/api/v3/stream/default/register", auth_token=self.__class__.auth_token,
                                        stream_metadata={
                                            "name": "test-stream-name",
                                            "description": "some description",
                                            "data_descriptor": [
                                                {
                                                    "name": "timestamp",
                                                    "type": "datetime",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                },{
                                                    "name": "localtime",
                                                    "type": "datetime",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                },{
                                                    "name": "battery",
                                                    "type": "string",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                }
                                            ],
                                            "modules": [
                                                {
                                                    "name": "string",
                                                    "version": "1.0",
                                                    "authors": [
                                                        {
                                                            "name": "Nasir Ali",
                                                            "email": "nasir.ali08@gmail.com",
                                                            "attributes": {
                                                                "key": "string",
                                                                "value": "string"
                                                            }
                                                        }
                                                    ],
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                }
                                            ]
                                        })
        self.assertEqual(result.status_code, 200)

        result = client.upload_stream_data(url="http://localhost:8089/api/v3/stream/default/86dee1cd-519d-396b-937c-253c432b37e2", auth_token=self.__class__.auth_token, data_file_path=os.getcwd() + "/sample_data/msgpack/phone_battery_stream.gz")
        self.assertEqual(result.status_code, 200)

        result = client.get_stream_metadata(url="http://localhost:8089/api/v3/stream/metadata/default/test-stream-name", auth_token=self.__class__.auth_token)
        self.assertEqual(result.status_code, 200)

        # TODO: API server object is created without init spark so cannot get data without spark
        # result = client.get_stream_data(url="http://localhost:8089/api/v3/stream/data/default/test-stream-name", auth_token=self.__class__.auth_token)
        # self.assertEqual(result.status_code, 200)

    def test_02_object_routes(self):
        """
        Test stream routes of API server
        """

        result = client.get_bucket_list(url="http://localhost:8089/api/v3/bucket/default", auth_token=self.__class__.auth_token)
        self.assertEqual(result.status_code, 200)

        result = client.get_objects_list_in_bucket(url="http://localhost:8089/api/v3/bucket/default", auth_token=self.__class__.auth_token)
        self.assertEqual(result.status_code, 200)

        result = client.get_objects_stats(url="http://localhost:8089/api/v3/bucket/stats/default/test_bucket/some_obj.zip", auth_token=self.__class__.auth_token)
        self.assertEqual(result.status_code, 200)

        result = client.get_object(url="http://localhost:8089/api/v3/bucket/default/test_bucket/some_obj.zip", auth_token=self.__class__.auth_token)
        self.assertEqual(result.status_code, 200)
