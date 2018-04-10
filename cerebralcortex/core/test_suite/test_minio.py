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

import unittest

class TestMinio():

    bucket_name = "testbucket"
    obj_name = "test_obj.json"

    def test_01_bucket(self):
        try:
            self.CC.create_bucket(self.bucket_name)
        except Exception as err:
            self.assertEqual(err.message, "Your previous request to create the named bucket succeeded and you already own it.")

        result = self.CC.is_bucket(self.bucket_name)
        self.assertEqual(result, True)

        result = self.CC.get_buckets()['buckets-list']
        found = False
        for res in result:
            if res['bucket-name']==self.bucket_name:
                found = True
        if not found:
            self.fail("Faied get_buckets, cannot find bucket.")


    def test_03_bucket_objects(self):
        self.CC.upload_object(self.bucket_name, self.obj_name, self.gz_file.replace(".gz", ".json"))
        obj_stats = self.CC.get_object_stats(self.bucket_name, self.obj_name)
        self.assertEqual(obj_stats["bucket_name"], self.bucket_name)
        self.assertEqual(obj_stats["object_name"], self.obj_name)

        try:
            self.CC.get_object(self.bucket_name, self.obj_name)
        except Exception as e:
            self.fail(e.message)

        obj_list = self.CC.get_bucket_objects(self.bucket_name)['bucket-objects']
        found = False
        for obj in obj_list:
            if obj['object_name']==self.obj_name:
                found = True
        if not found:
            self.fail("Faied get_bucket_objects, cannot find object.")

