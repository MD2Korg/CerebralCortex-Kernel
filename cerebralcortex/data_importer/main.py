from cerebralcortex import Kernel
import json
import os
import pandas as pd
import pyarrow as pa
import types
import pyarrow.parquet as pq

from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.data_importer.util.directory_scanners import dir_scanner
from cerebralcortex.data_importer.metadata_parsers.mcerebrum import parse_mcerebrum_metadata
from cerebralcortex.data_importer.raw_data_parsers.mcerebrum import mcerebrum_data_parser, assign_column_names_types
from cerebralcortex.core.data_manager.sql.data import SqlData


metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"


def import_file(user_id, file_path, compression=None, header=None, metadata=None, metadata_parser=None, data_parser=None, cc_config=None):
    move_forward = True
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

    try:
        if compression is not None:
            df = pd.read_csv(file_path, compression=compression, delimiter = "\n", header=header, quotechar='"')
        else:
            df = pd.read_csv(file_path, delimiter = "\n", header=header, quotechar='"')
    except Exception as e:
        # cannot read file: str(e)
        print(str(e))
        return False

    try:
        df = df.apply(data_parser, axis=1)
    except Exception as e:
        # cannot apply parser: str(e)
        print(str(e))
        return False

    if metadata is not None:
        if isinstance(metadata,str):
            metadata_dict = json.loads(metadata)
            df = assign_column_names_types(df, metadata_dict)
        if isinstance(metadata, dict):
            try:
                metadata = Metadata().from_json_file(metadata)
            except Exception as e:
                raise Exception("Cannot convert metadata into MetaData object: "+str(e))
                return False

        try:
            metadata["stream_metadata"].is_valid()
        except Exception as e:
            # metadata is not valid: str(e)
            print(str(e))
            return False
    else:
        try:
            metadata_file = file_path.replace(".gz", ".json")
            if metadata_file.endswith(".json"):
                with open(metadata_file, "r") as md:
                    metadata = md.read()
                    metadata = metadata.lower()
                    metadata_dict = json.loads(metadata)
        except Exception as e:
            # cannot read/parse metadata: str(e)
            print(str(e))
        dd_total_columns = len(metadata_dict.get("data_descriptor",[]))
        df_total_columns = len(df.columns)-2
        if dd_total_columns!=df_total_columns:
            # number data_descriptor columns (str(dd_total_columns)) and dataframe columns (str(df_total_columns)) do not match
            pass

            #return False
        df = assign_column_names_types(df, metadata_dict)
        metadata = metadata_parser(metadata_dict)

        try:
            metadata["stream_metadata"].is_valid()
        except Exception as e:
            # metadata is not valid: str(e)
            print(str(e))
            return False

    # write metadata
    sql_data = SqlData(cc_config)
    if metadata_parser.__name__=='parse_mcerebrum_metadata':
        platform_data = metadata_dict.get("execution_context",{}).get("platform_metadata","")
        if platform_data:
            platform_df = pd.DataFrame([[df["timestamp"][0],df["localtime"][0], json.dumps(platform_data)]])
            platform_df.columns = ["timestamp", "localtime","device_info"]
            sql_data.save_stream_metadata(metadata["platform_metadata"])
            save_data(df=platform_df, cc_config=cc_config, user_id=user_id, stream_name=metadata["platform_metadata"].name)
        try:
            sql_data.save_stream_metadata(metadata["stream_metadata"])
            save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata["stream_metadata"].name)
        except Exception as e:
            # cannot store data: str(e)
            print("dd")


        print("done")
    else:
        sql_data.save_stream_metadata(metadata)
        save_data(df=df, cc_config=cc_config, user_id=user_id, stream_name=metadata.name)


def save_data(df, cc_config, user_id, stream_name):
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


def import_dir(cc_config, input_data_dir, user_id=None, skip_file_extensions=[], allowed_filename_pattern=None, batch_size=None, compression=None, header=None, json_parser=None, data_parser=None):
    all_files = dir_scanner(input_data_dir, skip_file_extensions=skip_file_extensions, allowed_filename_pattern=allowed_filename_pattern)
    batch_files = []
    enable_spark = True
    if batch_size is None:
        enable_spark = False

    CC = Kernel(cc_config, enable_spark=enable_spark)
    cc_config = CC.config

    if input_data_dir[:1]!= "/":
        input_data_dir = input_data_dir + "/"

    for file_path in all_files:
        if user_id is None and data_parser.__name__=="mcerebrum_data_parser":
            user_id = file_path.replace(input_data_dir, "")[:36]
        if batch_size is None:
            import_file(user_id=user_id, file_path=file_path, compression=compression, header=header, metadata_parser=json_parser, data_parser=data_parser, cc_config=cc_config)
        else:
            if len(batch_files)>batch_size:
                tmp = CC.sparkContext.parallelize(batch_files)
                tmp.foreach(lambda file_path: import_file(user_id=user_id, file_path=file_path, compression=compression, header=header, metadata_parser=json_parser, data_parser=data_parser, cc_config=cc_config))
            else:
                batch_files.append(file_path)

import_dir( cc_config="/home/ali/IdeaProjects/CerebralCortex-2.0/conf/",
            input_data_dir="/home/ali/IdeaProjects/MD2K_DATA/data/test/",
           #batch_size=2,
           compression='gzip',
           header=None,
           skip_file_extensions=[".json"],
           #allowed_filename_pattern="REGEX PATTERN",
           data_parser=mcerebrum_data_parser,
           json_parser=parse_mcerebrum_metadata)