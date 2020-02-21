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

import json
import os

import gzip
import types
import warnings
from typing import Callable
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from texttable import Texttable

from cerebralcortex import Kernel
from cerebralcortex.core.data_manager.raw.data import RawData
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.data_importer.data_parsers.util import assign_column_names_types
from cerebralcortex.data_importer.util.directory_scanners import dir_scanner

# Disable pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def export_data(cc_config: dict, export_data_dir: str, study_name:str, stream_names:list=False, user_ids: list = None, export_type: str = "csv"):
    """
    Scan data directory, parse files and ingest data in cerebralcortex backend.

    Args:
        cc_config (str): cerebralcortex config directory
        export_data_dir (str): data directory path where exported data should be stored
        user_ids (list[str]): user id. Currently import_dir only supports parsing directory associated with a user
        study_name (str): a valid study name must exist OR use default as a study name
        stream_names list[str]: name of the stream
        export_type (str): currently only supports csv data export

    """

    enable_spark = False

    CC = Kernel(cc_config, study_name=study_name, enable_spark=enable_spark)
    cc_config = CC.config
    cc_config ["study_name"] = study_name
    cc_config["new_study"] = stream_names

    if export_data_dir[-1:] != "/":
        export_data_dir = export_data_dir + "/"

    storage_layer = "hdfs"

    if storage_layer == "hdfs":
        hdfs = pa.hdfs.connect('dantooine10dot', port=8020)
        hdfs_base_dir = "/cc3/study="+study_name+"/"
        for stream in hdfs.ls(hdfs_base_dir):
            if 'stream=' in stream:
                print('-' * 10, 'PROCESSING STREAM', stream, '-' * 10)
                for version in hdfs.ls(stream):
                    for user in hdfs.ls(version):
                        if all(s not in user for s in ['SUCCESS', '_tmp']):
                            path_array = user.split("/")
                            user_id = path_array[5]
                            version_id = path_array[4]
                            stream_name = path_array[3]
                            export_path = export_data_dir+"study=" + study_name + "/" + stream_name + "/" + str(version_id) + "/user="+user_id + "/"

                            if not os.path.exists(export_path):
                                os.makedirs(export_path, exist_ok=True)
                            if os.path.exists(export_path):
                                for user_files in hdfs.ls(user):
                                    print(user_files)
                                    df = pd.read_parquet(user_files)
                                    df.to_csv(export_path)
                                exit()
    #processed_files_list = CC.SqlData.get_processed_files_list()


export_data(cc_config="/cerebralcortex/code/config/cc3_conf/", export_data_dir="/md2k/data6/", study_name="rice")