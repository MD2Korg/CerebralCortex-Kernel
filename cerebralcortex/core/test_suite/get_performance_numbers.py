import gzip
import random
import argparse
from datetime import datetime
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.util.data_types import convert_sample
from cerebralcortex.core.test_suite.util.gen_test_data import gen_raw_data
import json
import unittest
from datetime import datetime
import argparse
import yaml
from dateutil import parser

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.file_to_db import FileToDB
from cerebralcortex.core.datatypes.datastream import DataStream, DataPoint
from cerebralcortex.core.test_suite.util.gen_test_data import gen_raw_data

file_name = "/home/ali/IdeaProjects/MD2K_DATA/data/test/perf.gz"
#data = gen_raw_data(file_name, 1000000, True, "str")

config_filepath ="./../../../../CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

CC = CerebralCortex(config_filepath, auto_offset_reset="smallest")
cc_conf = CC.config

msg={"owner_id": "1111", "stream_id": "2222", "stream_name": "performance",
     "metadata": {"name": "CU_NOTIF_RM_PACKAGE--edu.dartmouth.eureka", "type": "datastream", "owner": "11111111-107f-3624-aff2-dc0e0b5be53d", "identifier": "00000000-107f-3624-aff2-dc0e0b5be53d", "annotations": [], "data_descriptor": [], "execution_context": {"processing_module": {"name": "", "algorithm": [{"method": "", "authors": [""], "version": "", "reference": {"url": "http://md2k.org/"}, "description": ""}], "description": "", "input_streams": [], "output_streams": [], "input_parameters": {}}, "datasource_metadata": {}, "application_metadata": {"VERSION_NAME": "1.0.2", "VERSION_NUMBER": "301"}}}, "day": "20180121",
     "filename": "perf.gz"}

file_to_db = FileToDB(CC)

file_to_db.file_processor(msg, cc_conf["data_replay"]["data_dir"],
                          cc_conf['data_ingestion']['influxdb_in'],
                          cc_conf['data_ingestion']['nosql_in'])