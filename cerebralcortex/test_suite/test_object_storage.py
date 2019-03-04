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


class TestObjectStorage():

    def test_01_bucket(self):
        """
        Perform all bucket related tests

        """
        msg = self.CC.create_bucket(self.bucket_name)
        if msg == True:
            self.assertEqual(msg, True)
        else:
            self.assertEqual(str(msg["error"]).startswith("[Errno 17] File exists"), True)

        result = self.CC.is_bucket(self.bucket_name)
        self.assertEqual(result, True)

        result = self.CC.get_buckets()['buckets-list']
        found = False
        for res in result:
            if res['name'] == self.bucket_name:
                found = True
        if not found:
            self.fail("Faied get_buckets, cannot find bucket.")
        else:
            self.assertEqual(found, True)

    def test_03_bucket_objects(self):
        """
        Perform all object related tests

        """
        self.CC.upload_object(self.bucket_name, "some_obj.zip", self.obj_file_path)
        self.CC.upload_object(self.bucket_name, "some_obj.json", self.obj_metadata_file_path)
        obj_stats = self.CC.get_object_stats(self.bucket_name, "some_obj.zip")
        self.assertEqual(obj_stats["type"], "file")
        self.assertEqual(obj_stats["name"], "some_obj.zip")

        try:
            obj = self.CC.get_object(self.bucket_name, "some_obj.zip")
            self.assertEqual(type(obj), bytes)
        except Exception as e:
            self.fail(e.message)

        obj_list = self.CC.get_bucket_objects(self.bucket_name)['bucket-objects']
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(os.path.split(obj_list[0]["object_name"])[1], "some_obj.zip")
        self.assertNotEqual(len(obj_list[0]["metadata"]), 0)
