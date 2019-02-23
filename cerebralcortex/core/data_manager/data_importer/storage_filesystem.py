import json
import os
import re
import pandas as pd
from cerebralcortex import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.core.data_manager.sql.data import SqlData

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", enable_spark=False)
metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"

sql_data = SqlData(CC)

def df_cleaner(df, metadata):
    data_desciptor = metadata.get("data_descriptor", [])
    metadata_columns = []
    new_column_names ={0:"timestamp", 1:"offset"}
    #df.rename(columns={0:"timestamp", 1:"offset"}, inplace=True)

    if isinstance(data_desciptor, dict):
        data_desciptor = [data_desciptor]

    for dd in data_desciptor:
        name = re.sub('[^a-zA-Z0-9]+', '_', dd.get("name", "", )).strip("_")
        metadata_columns.append({"name": name,"type": dd.get("data_type", "")})

    if len(metadata_columns)>0:
        col_no = 2 # first two column numbers are timestamp and offset
        for mc in metadata_columns:
            new_column_names[col_no] = mc["name"]
            col_no +=1
    else:
        for column in df:
            if column!=0 and column!=1:
                new_column_names[column] = "value_"+str(column)
#                df[column] = pd.to_numeric(df[column], errors='ignore')

    df.rename(columns=new_column_names, inplace=True)
    for column in df:
        if column not in ['localtime','timestamp']:
            df[column] = pd.to_numeric(df[column], errors='ignore')
    return df

def CustomParser(line):
    data = []
    ts, offset, sample = line[0].split(',',2)
    try:
        sample = json.loads(sample)
    except:
        sample = sample.split(",")
    data.append(ts)
    data.append(offset)
    if isinstance(sample, list):
        data.extend(sample)
    else:
        data.append(sample)
    result = pd.Series(data)
    return result

def scan_day_dir(data_dir):
    for user_dir in os.scandir(data_dir):
        #owner = stream_dir.path[-36:]
        if user_dir.is_dir():
            for stream_dir in os.scandir(user_dir):
                if stream_dir.is_dir():
                    stream_dir = stream_dir.path
                    tmp = stream_dir.split("/")[-3:]
                    owner_id = tmp[0]
                    day = tmp[1]
                    stream_id = tmp[2]
                    files_list = []
                    dir_size = 0
                    for day_folder in os.scandir(stream_dir):
                        for data_file in os.scandir(day_folder):
                            if data_file.path.endswith(".gz"):
                                metadata_file = data_file.path.replace(".gz", ".json")
                                with open(metadata_file, "r") as md:
                                    metadata = md.read()
                                    metadata = metadata.lower()
                                    metadata = json.loads(metadata)
                                df = pd.read_csv("/home/ali/IdeaProjects/MD2K_DATA/data/test/tt.csv", header=None, sep='\t', quotechar='"')
                                df = df.apply(CustomParser, axis=1)
                                print(df)
                                df = pd.read_csv(data_file.path, compression='gzip', header=None, sep=',', quotechar='"')
                                df = df_cleaner(df, metadata)
                                metadata = convert_json_to_metadata_obj(metadata, df)
                                print("done")


def new_data_descript_frmt(data_descriptor, data):
    new_data_descriptor = {}
    basic_dd = {}
    attr = {}
    for field in data:
        if field.name is not None and field.name not in ["timestamp", "localtime", "user", "version"]:
            basic_dd["name"] = field.name
            basic_dd["type"]= str(field.dataType)
    for key, value in data_descriptor.items():
        if key=="name" or key=="type":
            pass
        else:
            attr[key] = value
    # remove any name inside attribute to avoid confusion
    if "name" in attr:
        attr.pop("name")
    new_data_descriptor["data_descriptor"] = basic_dd
    new_data_descriptor["data_descriptor"]["attributes"] =  attr
    sd = basic_dd
    sd["attributes"] = attr
    return sd

def new_module_metadata(ec_algo_pm):
    new_module = {}
    nm_attr = {}
    ec = ec_algo_pm
    application_metadata = ec["application_metadata"] #dict
    datasource_metadata = ec["datasource_metadata"] #dict
    platform_metadata = ec["platform_metadata"] #dict
    algorithm = ec["processing_module"]["algorithm"] # list of dict
    processing_module = ec["processing_module"]

    for key, value in application_metadata.items():
        if key=="version_name":
            new_module["version"] = value
        elif key=="name":
            new_module[key] = value
        else:
            nm_attr[key] = value

    new_module["input_streams"] = processing_module.get("input_streams", [])
    for key, value in datasource_metadata.items():
        nm_attr[key] = value

    for key, value in platform_metadata.items():
        nm_attr[key] = value

    for key, value in processing_module.items():
        if key!="algorithm" and key!="input_streams":
            nm_attr[key] = value

    for tmp in algorithm:
        for key, value in tmp.items():
            if key=="authors":
                new_module["authors"] = value
            elif  key=="reference":
                if tmp.get("reference", None) is not None:
                    for key, value in tmp.get("reference", {}).items():
                        nm_attr[key] = value
            else:
                nm_attr[key] = value

    if "name" in nm_attr:
        nm_attr.pop("name")
    new_module["attributes"] = nm_attr

    return new_module

def convert_json_to_metadata_obj(metadata, df):
    new_metadata = {}
    #metadata = json.loads(metadata.lower())
    # new data descriptor
    new_dd_list = []
    new_module = []
    new_dd = {}
    data = []
    if isinstance(metadata["data_descriptor"],dict):
        new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"], data))
    else:
        for dd in metadata["data_descriptor"]:
            new_dd_list.append(new_data_descript_frmt(dd, data))

    new_module.append(new_module_metadata(metadata["execution_context"]))

    input_streams = []
    if "input_streams" in metadata["execution_context"]["processing_module"]:
        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
            input_streams.append(input_stream["name"])
            new_metadata["name"] = metadata["name"]
            new_metadata["description"] = metadata.get("description", "xxxx")
            new_metadata["input_streams"] = input_streams
            new_metadata["data_descriptor"] = new_dd_list
            new_metadata["module"] = new_module

            return Metadata().from_json_file(new_metadata)

scan_day_dir(data_files_path)
# def read_dir(data_dir):
#     with os.scandir(data_dir) as user_dir:
#         for udir in user_dir:
#             user_id = udir.name
#
#             with os.scandir(udir.path) as metadata_files:
#                 for metadata_file in metadata_files:
#                     with open(metadata_file.path,"r") as mf:
#                         new_metadata = {}
#                         metadata = json.loads(mf.read())
#                         # new data descriptor
#                         new_dd_list = []
#                         new_module = []
#                         new_dd = {}
#
#                         #data_file_path = "/home/ali/IdeaProjects/MD2K_DATA/hdfs/cc3_export/cc3_export/stream=org.md2k.data_analysis.day_based_data_presence/version=1/user=00ab666c-afb8-476e-9872-6472b4e66b68/org.md2k.data_analysis.day_based_data_presence.parquet"
#                         data_file_path = data_files_path+"stream="+metadata["name"]+"/version=1/user="+str(user_id)+"/"+metadata["name"]+".parquet"
#                         data = CC.sparkSession.read.load(data_file_path)
#
#                         if isinstance(metadata["data_descriptor"],dict):
#                             new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"], data))
#                         else:
#                             for dd in metadata["data_descriptor"]:
#                                 new_dd_list.append(new_data_descript_frmt(dd, data))
#
#                         #TODO: this only support one module for now
#                         new_module.append(new_module_metadata(metadata["execution_context"]))
#
#                         input_streams = []
#                         if "input_streams" in metadata["execution_context"]["processing_module"]:
#                             for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
#                                 input_streams.append(input_stream["name"])
#                     new_metadata["name"] = metadata["name"]
#                     new_metadata["description"] = metadata.get("description", "xxxx")
#                     new_metadata["input_streams"] = input_streams
#                     new_metadata["data_descriptor"] = new_dd_list
#                     new_metadata["module"] = new_module
#
#
#                     # for field in data.schema.fields:
#                     #     if field.name not in ["timestamp", "localtime", "user", "version"]:
#                     #         dd[field.name] = field.dataType
#                     sql_data.save_stream_metadata(Metadata().from_json_file(new_metadata))
#


