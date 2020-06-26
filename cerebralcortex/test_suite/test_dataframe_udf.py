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

import os
import pathlib
import unittest
import warnings

from cerebralcortex import Kernel
from cerebralcortex.test_suite.util.data_helper import gen_location_datastream
from cerebralcortex.algorithms.gps.clustering import cluster_gps
from cerebralcortex.test_suite.test_object_storage import TestObjectStorage
from cerebralcortex.test_suite.test_sql_storage import SqlStorageTest
from cerebralcortex.test_suite.test_stream import DataStreamTest

class TestDataframeUDF(unittest.TestCase):

    # def setUp(self):
    #     """
    #     Setup test params to being testing with.
    #
    #     Notes:
    #         DO NOT CHANGE PARAMS DEFINED UNDER TEST-PARAMS! OTHERWISE TESTS WILL FAIL. These values are hardcoded in util/data_helper file as well.
    #     """
    #     warnings.simplefilter("ignore")
    #     config_filepath = "./../../conf/"
    #
    #     # create sample_data directory. Note: make sure this path is same as the filesystem path in cerebralcortex.yml
    #     pathlib.Path("./sample_data/").mkdir(parents=True, exist_ok=True)
    #
    #     self.study_name = "default"
    #     self.CC = Kernel(config_filepath, auto_offset_reset="smallest", study_name=self.study_name, new_study=True)
    #     self.cc_conf = self.CC.config
    #
    #     # TEST-PARAMS
    #     # sql/nosql params
    #     self.stream_name = "battery--org.md2k.phonesensor--phone"
    #     self.stream_version = 1
    #     self.metadata_hash = "96816db3-ce79-37a8-bfe2-034db8c56a6d"
    #     self.username = "test_user"
    #     self.user_id = "bfb2ca0c-e19c-3956-9db2-5459ccadd40c"
    #     self.user_password = "test_password"
    #     self.user_password_encrypted = "10a6e6cc8311a3e2bcc09bf6c199adecd5dd59408c343e926b129c4914f3cb01"
    #     self.user_role = "test_role"
    #     self.auth_token = "xxx"
    #     self.user_metadata = {"study_name": self.study_name}
    #     self.user_settings = {"mcerebrum":"confs"}
    #
    #     # object test params
    #     self.bucket_name = "test_bucket"
    #     self.obj_file_path = os.getcwd() + "/sample_data/objects/test_obj.zip"
    #     self.obj_metadata_file_path = os.getcwd() + "/sample_data/objects/test_obj.json"
    #
    #     # kafka test params
    #     self.test_topic_name = "test_topic"
    #     self.test_message = "{'msg1':'some test message'}"

    def test_01_udf_on_gps(self):
        """
        Window datastream and perform a udf on top of it

        """
from pennprov.connection.mprov_connection import MProvConnection
conn = MProvConnection('sample', 'sample', host='http://localhost:8088')
# conn.set_graph("nasir-graph")
# conn.create_or_reset_graph()
#
# #from cerebralcortex.algorithms.gps.ttt import gps_clusters
# os.environ["MPROV_USER"] = "sample"
# os.environ["MPROV_PASSWORD"] = "sample"
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
def get_stream_metadata(stream_name):

	metadata = Metadata().set_name(stream_name).set_description("mobile phone accelerometer sensor data.") \
	.add_dataDescriptor(
	DataDescriptor().set_name("accelerometer_x").set_type("float").set_attribute("description", "acceleration minus gx on the x-axis")) \
	.add_dataDescriptor(
	DataDescriptor().set_name("accelerometer_y").set_type("float").set_attribute("description", "acceleration minus gy on the y-axis")) \
	.add_dataDescriptor(
	DataDescriptor().set_name("accelerometer_z").set_type("float").set_attribute("description", "acceleration minus gz on the z-axis")) \
	.add_module(
	ModuleMetadata().set_name("cerebralcortex.streaming_operation.main").set_version("2.0.7").set_attribute("description", "data is collected using mcerebrum.").set_author(
	    "test_user", "test_user@test_email.com"))

	stream_metadata = metadata.to_json()

	return stream_metadata
mt = get_stream_metadata("namona")
ds_gps = gen_location_datastream(user_id="bfb2ca0c-e19c-3956-9db2-5459ccadd40c", stream_name="gps--org.md2k.phonesensor--phone")
#ds_gps.show(3)

d2=ds_gps.window(windowDuration=60)
dd=cluster_gps(d2)
dd.show()


# ds_gps.compute(gps_clusters).show(5)
# print(ds_gps)