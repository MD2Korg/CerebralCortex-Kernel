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


def import_file(cc_config: dict, user_id: str, file_path: str, allowed_streamname_pattern: str = None, compression: str = None, header: int = None,
                metadata: Metadata = None, metadata_parser: Callable = None, data_parser: Callable = None):
    """
    Import a single file and its metadata into cc-storage.

    Args:
        cc_config (str): cerebralcortex config directory
        user_id (str): user id. Currently import_dir only supports parsing directory associated with a user
        file_path (str): file path.
        compression (str): pass compression name if csv files are compressed
        header (str): (optional) row number that must be used to name columns. None means file does not contain any header
        metadata (Metadata): (optional) Same metadata will be used for all the data files if this parameter is passed. If metadata is passed then metadata_parser cannot be passed.
        metadata_parser (python function): a parser that can parse json files and return a valid MetaData object. If metadata_parser is passed then metadata parameter cannot be passed.
        data_parser (python function): a parser than can parse each line of data file. import_dir read data files as a list of lines of a file. data_parser will be applied on all the rows.

        Notes:
        Each csv file should contain a metadata file. Data file and metadata file should have same name. For example, data.csv and data.json.
        Metadata files should be json files.

    Returns:
        bool: False in case of an error

    """
    warnings.simplefilter(action='ignore', category=FutureWarning)


    if user_id is None:
        raise ValueError("user_id cannot be None.")

    if cc_config is None:
        raise ValueError("cc_config parameter is missing.")

    if not isinstance(data_parser, types.FunctionType):
        raise Exception("data_parser is not a function.")

    if metadata_parser is not None and not isinstance(metadata_parser, types.FunctionType):
        raise Exception("metadata_parser is not a function.")

    if metadata is not None and metadata_parser is not None:
        raise ValueError("Either pass metadata or metadata_parser.")

    sql_data = SqlData(cc_config)

    if metadata is not None:
        if isinstance(metadata, str):
            metadata_dict = json.loads(metadata)
            try:
                metadata = Metadata().from_json_file(metadata_dict)
            except Exception as e:
                fault_description = "Cannot convert string metadata into MetaData object: " + str(e)
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="CANNOT_PARSE_METADATA_FILE",
                                           fault_description=fault_description, success=0)
            # df = assign_column_names_types(df, metadata_dict)
        elif isinstance(metadata, dict):
            metadata_dict = metadata
            try:
                metadata = Metadata().from_json_file(metadata_dict)
            except Exception as e:
                fault_description = "Cannot convert dict metadata into MetaData object: " + str(e)
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="CANNOT_PARSE_METADATA_FILE",
                                           fault_description=fault_description, success=0)
                return False
        elif isinstance(metadata, Metadata):
            try:
                metadata.is_valid()
                metadata_dict = metadata.to_json()
            except Exception as e:
                fault_description = "metadata object is not valid: " + str(e)
            sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                       file_path=file_path, fault_type="CANNOT_PARSE_METADATA_FILE",
                                       fault_description=fault_description, success=0)
        else:
            raise Exception("Invalid metadata")

    else:
        try:

            file_ext = os.path.splitext(file_path)[1]
            metadata_file = file_path.replace(file_ext, ".json")
            if metadata_file.endswith(".json"):
                with open(metadata_file, "r") as md:
                    metadata = md.read()
                    metadata = metadata.lower()
                    metadata_dict = json.loads(metadata)
                    cmm = sql_data.get_corrected_metadata(stream_name=metadata_dict.get("name"))
                    if cmm.get("status")!="include":
                        fault_description = "Ignored stream ingestion: "+str(metadata_dict.get("name"))+". Criteria: "+cmm.get("status")
                        sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                                   file_path=file_path, fault_type="IGNORED_STREAM",
                                                   fault_description=fault_description, success=0)
                        return False
                    if cmm.get("metadata"):
                        metadata_dict = cmm.get("metadata")
                    #metadata = Metadata().from_json_file(metadata_dict)
        except Exception as e:
            fault_description = "read/parse metadata: " + str(e)
            sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                       file_path=file_path, fault_type="CANNOT_PARSE_METADATA_FILE",
                                       fault_description=fault_description, success=0)
            return False

    #df = assign_column_names_types(df, metadata_dict)

    try:
        if metadata_parser is not None:
            metadata = metadata_parser(metadata_dict)
        else:
            metadata = Metadata().from_json_file(metadata_dict)

    except Exception as e:
        raise Exception("Error in converting metadata json object to Metadata class structure. "+str(e))

    try:
        if isinstance(metadata, dict):
            metadata["stream_metadata"].is_valid()
        else:
            metadata.is_valid()
    except Exception as e:
        fault_description = "metadata is not valid: " + str(e)
        sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                   file_path=file_path, fault_type="MISSING_METADATA_FIELDS",
                                   fault_description=fault_description, success=0)
        return False
    if metadata_parser is not None and metadata_parser.__name__ == 'mcerebrum_metadata_parser':
        stream_metadata = metadata["stream_metadata"]
    else:
        stream_metadata = metadata

    if allowed_streamname_pattern is not None:
        try:
            allowed_streamname_pattern = re.compile(allowed_streamname_pattern)
            is_blacklisted = allowed_streamname_pattern.search(stream_metadata.name)
        except:
            raise Exception("allowed_streamname_pattern regular expression is not valid.")
    else:
        is_blacklisted = False

    if not is_blacklisted:
        try:
            if compression is "gzip":
                try:
                    df = pd.read_csv(file_path, compression=compression, delimiter="\n", header=header, quotechar='"')
                except:
                    df = []
                    with gzip.open(file_path,'rt') as f:
                        try:
                            for line in f:
                                df.append(line)
                        except Exception as e:
                            fault_description = "cannot read file: " \
                                                "" + str(e)
                            sql_data.add_ingestion_log(user_id=user_id, stream_name=stream_metadata.name, file_path=file_path,
                                                       fault_type="PARTIAL_CORRUPT_DATA_FILE", fault_description=fault_description, success=0)
            if compression is not None:
                df = pd.read_csv(file_path, compression=compression, delimiter="\n", header=header, quotechar='"')
            else:
                df = pd.read_csv(file_path, delimiter="\n", header=header, quotechar='"')
        except Exception as e:
            fault_description = "cannot read file:" + str(e)
            sql_data.add_ingestion_log(user_id=user_id, stream_name=stream_metadata.name, file_path=file_path,
                                       fault_type="CORRUPT_DATA_FILE", fault_description=fault_description, success=0)
            return False

        try:
            if isinstance(df, list):
                df_list = df
            else:
                df_list = df.values.tolist()

            tmp_list = []
            for tmp in df_list:
                result = data_parser(tmp)
                tmp_list.append(result)
            df = pd.DataFrame(tmp_list)
        except Exception as e:
            fault_description = "cannot apply parser:" + str(e)
            sql_data.add_ingestion_log(user_id=user_id, stream_name=stream_metadata.name, file_path=file_path,
                                       fault_type="CANNOT_PARSE_DATA_FILE", fault_description=fault_description, success=0)
            return False


        df = assign_column_names_types(df, metadata_dict)

        # save metadata/data
        if metadata_parser is not None and metadata_parser.__name__ == 'mcerebrum_metadata_parser':

            platform_data = metadata_dict.get("execution_context", {}).get("platform_metadata", "")
            if platform_data:
                platform_df = pd.DataFrame([[df["timestamp"][0], df["localtime"][0], json.dumps(platform_data)]])
                platform_df.columns = ["timestamp", "localtime", "device_info"]
                sql_data.save_stream_metadata(metadata["platform_metadata"])
                save_data(df=platform_df, cc_config=cc_config, user_id=user_id,
                          stream_name=metadata["platform_metadata"].name)
            try:
                df = df.dropna()
                save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata["stream_metadata"].name)
                sql_data.save_stream_metadata(metadata["stream_metadata"])
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="SUCCESS", fault_description="", success=1)
            except Exception as e:
                fault_description = "cannot store data: " + str(e)
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="CANNOT_STORE_DATA",
                                           fault_description=fault_description, success=0)
        else:
            try:
                sql_data.save_stream_metadata(metadata)
                save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata.name)
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="SUCCESS", fault_description="", success=1)
            except Exception as e:
                fault_description = "cannot store data: " + str(e)
                sql_data.add_ingestion_log(user_id=user_id, stream_name=metadata_dict.get("name", "no-name"),
                                           file_path=file_path, fault_type="CANNOT_STORE_DATA",
                                           fault_description=fault_description, success=0)


def save_data(df: object, cc_config: dict, user_id: str, stream_name: str):
    """
    save dataframe to cc storage system

    Args:
        df (pandas): dataframe
        cc_config (str): cerebralcortex config directory
        user_id (str): user id
        stream_name (str): name of the stream
    """
    df["version"] = 1
    df["user"] = str(user_id)
    table = pa.Table.from_pandas(df)
    partition_by = ["version", "user"]
    if cc_config["nosql_storage"] == "filesystem":
        data_file_url = os.path.join(cc_config["filesystem"]["filesystem_path"], "stream="+str(stream_name))
        # data_file_url = os.path.join("/home/ali/IdeaProjects/MD2K_DATA/tmp/", "stream=" + str(stream_name), "version=1",
        #                              "user=" + str(user_id))
        pq.write_to_dataset(table, root_path=data_file_url, partition_cols=partition_by, preserve_index=False)
    elif cc_config["nosql_storage"] == "hdfs":
        raw_files_dir = cc_config['hdfs']['raw_files_dir']
        fs = pa.hdfs.connect(cc_config['hdfs']['host'], cc_config['hdfs']['port'])
        with fs.open(raw_files_dir, "wb") as fw:
            pq.write_table(table, fw, partition_cols=partition_by, preserve_index=False)


def print_stats_table(ingestion_stats: dict):
    """
    Print import data stats in table.

    Args:
        ingestion_stats (dict): basic import statistics. {"fault_type": [], "total_faults": []}
    """

    rows = []
    rows.append(["Type", "Total Files"])
    print("\n\n" + "=" * 47)
    print("*" * 13, "IMPORTED DATA STATS", "*" * 13)
    print("=" * 47, "\n")
    for ing_stat in ingestion_stats:
        rows.append([ing_stat.get("fault_type", "No-Fault-Type"), ing_stat.get("total_faults", 0)])
    table = Texttable()
    table.set_cols_align(["l", "c"])
    table.add_rows(rows)
    print(table.draw())


def import_dir(cc_config: dict, input_data_dir: str, user_id: str = None, data_file_extension: list = [],
               allowed_filename_pattern: str = None,
               allowed_streamname_pattern: str = None,
               batch_size: int = None, compression: str = None, header: int = None, metadata: Metadata = None,
               metadata_parser: Callable = None, data_parser: Callable = None,
               gen_report: bool = False):
    """
    Scan data directory, parse files and ingest data in cerebralcortex backend.

    Args:
        cc_config (str): cerebralcortex config directory
        input_data_dir (str): data directory path
        user_id (str): user id. Currently import_dir only supports parsing directory associated with a user
        data_file_extension (list[str]): (optional) provide file extensions (e.g., .doc) that must be ignored
        allowed_filename_pattern (list[str]): (optional) regex of files that must be processed.
        batch_size (int): (optional) using this parameter will turn on spark parallelism. batch size is number of files each worker will process
        compression (str): pass compression name if csv files are compressed
        header (str): (optional) row number that must be used to name columns. None means file does not contain any header
        metadata (Metadata): (optional) Same metadata will be used for all the data files if this parameter is passed. If metadata is passed then metadata_parser cannot be passed.
        metadata_parser (python function): a parser that can parse json files and return a valid MetaData object. If metadata_parser is passed then metadata parameter cannot be passed.
        data_parser (python function): a parser than can parse each line of data file. import_dir read data files as a list of lines of a file. data_parser will be applied on all the rows.
        gen_report (bool): setting this to True will produce a console output with total failures occurred during ingestion process.

    Notes:
        Each csv file should contain a metadata file. Data file and metadata file should have same name. For example, data.csv and data.json.
        Metadata files should be json files.

    Todo:
        Provide sample metadata file URL
    """
    all_files = dir_scanner(input_data_dir, data_file_extension=data_file_extension,
                            allowed_filename_pattern=allowed_filename_pattern)
    batch_files = []
    tmp_user_id = None
    enable_spark = True
    if batch_size is None:
        enable_spark = False

    CC = Kernel(cc_config, enable_spark=enable_spark)
    cc_config = CC.config

    if input_data_dir[:1] != "/":
        input_data_dir = input_data_dir + "/"
    processed_files_list = CC.SqlData.get_processed_files_list()

    for file_path in all_files:

        if not file_path in processed_files_list:
            if data_parser.__name__ == "mcerebrum_data_parser":
                user_id = file_path.replace(input_data_dir, "")[:36]
            if batch_size is None:
                import_file(cc_config=cc_config, user_id=user_id, file_path=file_path, compression=compression,
                            header=header, metadata=metadata, metadata_parser=metadata_parser, data_parser=data_parser)
            else:
                if len(batch_files) > batch_size or tmp_user_id != user_id:

                    rdd = CC.sparkContext.parallelize(batch_files)
                    rdd.foreach(lambda file_path: import_file(cc_config=cc_config, user_id=user_id, file_path=file_path,
                                                              compression=compression, header=header, metadata=metadata,
                                                              metadata_parser=metadata_parser, data_parser=data_parser))
                    print("Total Files Processed:", len(batch_files))
                    batch_files.clear()
                    batch_files.append(file_path)
                    tmp_user_id = user_id
                else:
                    batch_files.append(file_path)
                    tmp_user_id = user_id
    if len(batch_files) > 0:
        rdd = CC.sparkContext.parallelize(batch_files)
        rdd.foreach(lambda file_path: import_file(cc_config=cc_config, user_id=user_id, file_path=file_path,
                                                  compression=compression, header=header, metadata=metadata,
                                                  metadata_parser=metadata_parser, data_parser=data_parser))
        print("Total Files Processed:", len(batch_files))

    if gen_report:
        print_stats_table(CC.SqlData.get_ingestion_stats())
