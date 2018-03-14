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
from datetime import datetime

import yaml
from dateutil import parser
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.test_suite.util.gen_test_data import gen_raw_data
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB
import pickle
class TestFileToDB():
    def test01_file_to_db(self):
        file_to_db = FileToDB(self.CC)
        for replay_batch in self.CC.SqlData.get_replay_batch(record_limit=5000):
            for msg in replay_batch:
                file_to_db.file_processor(msg, self.cc_conf["data_replay"]["data_dir"], self.cc_conf['data_ingestion']['influxdb_in'], self.cc_conf['data_ingestion']['nosql_in'])



class TestStreamHandler():
    def test01_save_stream(self):
        outputdata = {}

        # Data Processing loop
        for row in self.data:
            day = row.start_time.strftime("%Y%m%d")
            if day not in outputdata:
                outputdata[day] = []

            outputdata[day].append(row)
        tt = self.CC.get_stream_metadata_by_user("636fcc1f-8966-4e63-a9df-0cbaa6e9296c","DATA_QUALITY--ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST", parser.parse("2017-12-01 00:00:00"),parser.parse("2017-12-01 23:59:00"))
        for day, dps in outputdata.items():
            ds = DataStream(self.stream_id, self.owner_id, self.stream_name, self.metadata["data_descriptor"],
                            self.metadata["execution_context"], self.metadata["annotations"], self.metadata["type"], None, None, dps)
            self.CC.save_stream(ds)

    def test02_get_stream(self):
        data_len = []
        start_times = []
        end_times = []
        start_time = datetime.utcfromtimestamp(1519355692)
        end_time = datetime.utcfromtimestamp(1519355699)
        for day in self.days:
            ds = self.CC.get_stream(self.stream_id, self.owner_id, day)
            data_len.append(len(ds.data))
            start_times.append(ds.data[0].start_time)
            end_times.append(ds.data[len(ds.data)-1].start_time)

        # test start/end time of datapoints
        self.assertEqual(data_len, [3999, 999, 5001])
        expected_start_times = [parser.parse("2018-02-21 23:28:21.133000"),parser.parse("2018-02-23 03:14:51.133000"),parser.parse("2018-02-24 07:01:41.123000")]
        expected_end_times = [parser.parse("2018-02-21 23:29:01.113000"),parser.parse("2018-02-23 03:15:01.113000"),parser.parse("2018-02-24 07:03:11.113000")]
        self.assertEqual(start_times, expected_start_times)
        self.assertEqual(end_times, expected_end_times)

        # test sub-set of stream
        ds = self.CC.get_stream(self.stream_id, self.owner_id, self.days[1], start_time, end_time)
        if self.CC.config["data_ingestion"]["nosql_store"]=="hdfs" or self.CC.config["data_ingestion"]["nosql_store"]=="filesystem":
            self.assertEqual(len(ds.data), 700)
            self.assertEqual(ds.data[0].start_time, parser.parse("2018-02-23 03:14:52.003000"))
            self.assertEqual(ds.data[len(ds.data)-1].start_time, parser.parse("2018-02-23 03:14:58.993000"))
        else:
            self.assertEqual(len(ds.data), 600)
            self.assertEqual(ds.data[0].start_time, parser.parse("2018-02-23 03:14:52.133000"))
            self.assertEqual(ds.data[len(ds.data)-1].start_time, parser.parse("2018-02-23 03:14:58.123000"))


        # test metadata
        self.assertEqual(ds.owner,self.owner_id)
        self.assertEqual(ds.name, self.stream_name)
        self.assertEqual(ds.datastream_type, self.metadata["type"])
        self.assertEqual(ds.data_descriptor, self.metadata["data_descriptor"])
        self.assertEqual(ds.annotations, self.metadata["annotations"])
        self.assertEqual(ds.execution_context['application_metadata'], self.metadata["execution_context"]["application_metadata"])
        self.assertEqual(ds.execution_context['datasource_metadata'], self.metadata["execution_context"]["datasource_metadata"])
        self.assertEqual(ds.execution_context['platform_metadata'], self.metadata["execution_context"]["platform_metadata"])
        self.assertEqual(ds.execution_context['processing_module'], self.metadata["execution_context"]["processing_module"])



class TestHDFS(unittest.TestCase, TestStreamHandler):
    def setUp(self):
        with open("resources/cc_test_configuration.yml") as test_conf:
            test_conf = yaml.load(test_conf)

        with open(test_conf["sample_data"]["data_folder"] + test_conf["sample_data"]["json_file"], "r") as md:
            self.metadata = json.loads(md.read())

        self.CC = CerebralCortex()
        self.cc_conf = self.CC.config

        self.owner_id = self.metadata["owner"]
        self.stream_id = self.metadata["identifier"]
        self.stream_name = self.metadata["name"]
        self.test_data_folder = test_conf["sample_data"]["data_folder"]
        self.gz_file = self.test_data_folder + test_conf["sample_data"]["gz_file"]
        self.days = ["20180221", "20180223", "20180224"]

        # generate sample raw data file
        self.data = gen_raw_data(self.gz_file, 10000, True, "float")
        # ss = pickle.dumps(self.data)
        # with open("/home/ali/Desktop/tmp/hadoop/sample.pickle", "wb") as f:
        #     f.write(ss)
        print("done")
