from cerebralcortex import Kernel
import json
import os
import pandas as pd
import pyarrow as pa
import types
import pyarrow.parquet as pq

from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.core.data_manager.sql.data import SqlData
from cerebralcortex.data_importer.directory_scanners import dir_scanner
from cerebralcortex.data_importer.metadata_parsers.mcerebrum import parse_mcerebrum_metadata, metadata_parser
from cerebralcortex.data_importer.raw_data_parsers.mcerebrum import mcerebrum_data_parser, assign_column_names_types
from cerebralcortex.core.data_manager.sql.data import SqlData

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", enable_spark=True)
metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"


def import_file(user_id, file_path, compression=None, header=None, metadata=None, metadata_parser=None, data_parser=None, cc_config=None):
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

    if compression is not None:
        df = pd.read_fwf(file_path, compression=compression, header=header, quotechar='"')
    else:
        df = pd.read_fwf(file_path, header=header, quotechar='"')

    df = df.apply(data_parser, axis=1)

    if metadata is not None:
        if isinstance(metadata,str):
            metadata_dict = json.loads(metadata)

        if isinstance(metadata, dict):
            metadata_dict = metadata
            try:
                metadata = Metadata().from_json_file(metadata)
            except Exception as e:
                raise Exception("Cannot convert metadata into MetaData object: "+str(e))

        if not metadata.is_valid():
            raise Exception("Metadata is not valid.")
    else:
        metadata_file = file_path.replace(".gz", ".json")
        if metadata_file.endswith(".json"):
            with open(metadata_file, "r") as md:
                metadata = md.read()
                metadata = metadata.lower()
                metadata_dict = json.loads(metadata)

        metadata = metadata_parser(metadata_dict, df)


    df = assign_column_names_types(df, metadata_dict)



    # write metadata
    sql_data = SqlData(cc_config)
    if metadata_parser.__name__=='parse_mcerebrum_metadata':
        sql_data.save_stream_metadata(metadata["stream_metadata"])
        sql_data.save_stream_metadata(metadata["platform_metadata"])
        save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata["stream_metadata"].name)
        #save_data(df=platform_df, cc_config=cc_config, user_id=user_id, stream_name=metadata["platform_metadata"].name)
    else:
        sql_data.save_stream_metadata(metadata)
        save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata.name)




def save_data(df, cc_config, user_id, stream_name):
    # write data if metadata is successfully registered
    table = pa.Table.from_pandas(df)

    if cc_config["nosql_storage"]=="filesystem":
        #data_file_url = os.path.join(cc_config["filesystem"]["filesystem_path"], "stream="+str(stream_name), "version=1", "user="+str(user_id))
        data_file_url = os.path.join("/home/ali/IdeaProjects/MD2K_DATA/tmp/", "stream="+str(stream_name), "version=1", "user="+str(user_id))
        pq.write_to_dataset(table,root_path=data_file_url)
    elif cc_config["nosql_storage"]=="hdfs":
        raw_files_dir = cc_config['hdfs']['raw_files_dir']
        fs = pa.hdfs.connect(cc_config['hdfs']['host'], cc_config['hdfs']['port'])
        with fs.open(raw_files_dir, "wb") as fw:
            pq.write_table(table, fw)


def import_dir(data_dir, user_id=None, skip_file_extensions=[], allowed_filename_pattern=None, batch_size=None, compression=None, header=None, json_parser=None, data_parser=None):
    all_files = dir_scanner(data_dir, skip_file_extensions=skip_file_extensions, allowed_filename_pattern=allowed_filename_pattern)
    batch_files = []
    cc_config = CC.config

    if data_dir[:1]!="/":
        data_dir = data_dir+"/"

    for file_path in all_files:
        if batch_size is None:
            if user_id is None and data_parser.__name__=="mcerebrum_data_parser":
                user_id = file_path.replace(data_dir,"")[:36]
            import_file(user_id=user_id, file_path=file_path, compression=compression, header=header, metadata_parser=json_parser, data_parser=data_parser, cc_config=cc_config)
        else:
            if user_id is None and data_parser.__name__=="mcerebrum_data_parser":
                user_id = file_path.replace(data_dir,"")[:36]
            if len(batch_files)>batch_size:
                tmp = CC.sparkContext.parallelize(batch_files)
                tmp.foreach(lambda file_path: import_file(user_id=user_id, file_path=file_path, compression=compression, header=header, metadata_parser=json_parser, data_parser=data_parser, cc_config=cc_config))
            else:
                batch_files.append(file_path)

import_dir(data_dir="/home/ali/IdeaProjects/MD2K_DATA/data/test/",
           #batch_size=2,
           compression='gzip',
           header=None,
           skip_file_extensions=[".json"],
           #allowed_filename_pattern="REGEX PATTERN",
           data_parser=mcerebrum_data_parser,
           json_parser=parse_mcerebrum_metadata)