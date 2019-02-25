import json
import os
import re
import pandas as pd
from datetime import datetime
from cerebralcortex import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.data_manager.sql.data import SqlData

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/", enable_spark=False)
metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/data/test/"

sql_data = SqlData(CC)

def assign_column_names_types(df, metadata):
    data_desciptor = metadata.get("data_descriptor", [])
    metadata_columns = []
    new_column_names ={0:"timestamp", 1:"localtime"}
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
    tmp = []
    ts, offset, sample = line[0].split(',',2)
    try:
        ts = int(ts)
        offset = int(offset)
    except:
        raise Exception("cannot convert timestamp/offsets into int")
    try:
        vals = json.loads(sample)
    except:
        vals = sample.split(",")

    timestamp = datetime.utcfromtimestamp(ts/1000)
    localtime = datetime.utcfromtimestamp((ts+offset)/1000)
    data.append(timestamp)
    data.append(localtime)
    if isinstance(vals, list):

        data.extend(vals)
    else:
        data.append(vals)

    result = pd.Series(data)
    return result

def scan_day_dir(data_dir):
    for user_dir in os.scandir(data_dir):
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
                                # used DONOTSEPARATECOLUMNS as sep so rows are not split using comma. Comma was causing issues for dict (EMA) columns
                                df = pd.read_fwf(data_file.path, compression='gzip', header=None, quotechar='"')
                                df = df.apply(CustomParser, axis=1)
                                df = assign_column_names_types(df, metadata)
                                metadata = convert_json_to_metadata_obj(metadata, df.columns.values, df.dtypes.values)
                                print("done")


def new_data_descript_frmt(data_descriptor, column_names, column_types):
    new_data_descriptor = {}
    basic_dd = {}
    attr = {}
    for col_name, col_type in zip(column_names, column_types):
        if col_name not in ["timestamp", "localtime", "user", "version"]:
            basic_dd["name"] = col_name
            basic_dd["type"]= str(col_type)
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

def get_platform_metadata(stream_name, execution_context):

    platform_metadata = execution_context["platform_metadata"] #dict
    application_metadata = execution_context["application_metadata"] #dict

    if platform_metadata.get("device_id", "")!="":
        stream_name = stream_name+"_"+platform_metadata.get("name", "name_not_available")+"_"+platform_metadata.get("device_id", "")
    else:
        stream_name = stream_name+"_"+platform_metadata.get("name", "name_not_available")

    return Metadata().set_name(stream_name).set_version(1).\
        set_description(application_metadata.get("description", "no description available.")).add_dataDescriptor(
        DataDescriptor().set_name("device_id").set_type("string").set_attribute("description", "hardware device id.")
    ).add_module(ModuleMetadata().set_name(application_metadata.get("name", "name_not_available")).set_version(application_metadata.get("version", 1)).set_attribute("description", application_metadata.get("description", "no description available.")).set_author(
        "Monowar Hossain", "smhssain@memphis.edu"))

def new_module_metadata(ec_algo_pm):
    new_module = {}
    nm_attr = {}
    ec = ec_algo_pm
    application_metadata = ec["application_metadata"] #dict
    #annotations = []
    #datasource_metadata = ec["datasource_metadata"] #dict
    #platform_metadata = ec["platform_metadata"] #dict
    algorithm = ec["processing_module"]["algorithm"] # list of dict
    processing_module = ec["processing_module"]


    for key, value in application_metadata.items():
        if key=="version_name":
            new_module["version"] = value
        elif key=="name":
            new_module[key] = value
        else:
            nm_attr[key] = value

    # new_module["input_streams"] = processing_module.get("input_streams", [])
    # for key, value in datasource_metadata.items():
    #     nm_attr[key] = value

    # for key, value in platform_metadata.items():
    #     nm_attr[key] = value

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

def convert_json_to_metadata_obj(metadata,column_names, column_types):
    new_metadata = {}
    new_dd_list = []
    annotations = []
    new_module = []
    if isinstance(metadata["data_descriptor"],dict):
        new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"], column_names, column_types))
    else:
        for dd in metadata["data_descriptor"]:
            new_dd_list.append(new_data_descript_frmt(dd, column_names, column_types))

    new_module.append(new_module_metadata(metadata["execution_context"]))

    platform_metadata = get_platform_metadata(metadata.get("name", "name_not_available"), metadata["execution_context"])

    annotations.append(platform_metadata.name)

    input_streams = []
    if "input_streams" in metadata["execution_context"]["processing_module"]:
        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
            input_streams.append(input_stream["name"])

    new_metadata["name"] = metadata["name"]
    new_metadata["description"] = metadata.get("description", "xxxx")
    new_metadata["annotations"] = annotations
    new_metadata["input_streams"] = input_streams
    new_metadata["data_descriptor"] = new_dd_list
    new_metadata["modules"] = new_module

    return Metadata().from_json_file(new_metadata)



scan_day_dir(data_files_path)